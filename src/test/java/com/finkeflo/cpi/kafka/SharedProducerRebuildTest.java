/*-
 * #%L
 * Kafka Adapter Plus
 * %%
 * Copyright (C) 2026 Florian Kube
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */
package com.finkeflo.cpi.kafka;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.finkeflo.cpi.kafka.KafkaErrorHelper.Classification;
import com.finkeflo.cpi.kafka.TopicMetadataChecker.TopicCheckResult;

/**
 * The shared producer of the receiver against its rebuild (#246): the rebuild only discards the
 * producer and the next send builds a new one, and a send that runs while another send's failure
 * has the producer rebuilt uses either the old or the new producer, never a missing one. Each race
 * is staged at its exact point by a test double that rebuilds the producer from inside the call it
 * races with, so nothing depends on timing.
 */
public class SharedProducerRebuildTest {

    private static final String TOPIC = "orders";

    private DefaultCamelContext ctx;
    private final List<CpiKafkaPlusProducer> producers = new ArrayList<>();
    private final List<KafkaProducer<byte[], byte[]>> clients = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        TlsListenerProbe.setProbeRunnerForTests(address -> TlsListenerProbe.Verdict.INCONCLUSIVE);
        ctx = new DefaultCamelContext();
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        for (CpiKafkaPlusProducer producer : producers) {
            producer.doStop();
        }
        for (KafkaProducer<byte[], byte[]> client : clients) {
            client.close(Duration.ZERO);
        }
        TlsListenerProbe.clearCacheForTests();
        ctx.close();
    }

    // ── the rebuild ──────────────────────────────────────────────────────────────

    /**
     * A rebuild discards the producer and says so; the next send builds the new one. The event used
     * to report {@code producerRecreated=false} for every rebuild, because it looked for a new
     * producer right after discarding the old one.
     */
    @Test
    public void aRebuildReportsTheDiscardedProducerAndTheNextSendBuildsTheNewOne() throws Exception {
        CpiKafkaPlusProducer producer = newProducer("NONE", 1);
        install(producer, new AcknowledgingClient());
        AcknowledgingClient rebuilt = new AcknowledgingClient();
        producer.sharedProducerFactory = props -> rebuilt;

        String logged = capture(() -> producer.handleSendFailure(
                new AuthenticationException("SASL secret rotated"), "producer.single.send",
                new LinkedHashMap<String, String>()));

        String outcome = ownLine(logged, "producer.rebuild.outcome");
        Assert.assertTrue(outcome, outcome.contains(" producerDiscarded=true consequence='the next send "
                + "builds a new producer' durationMs="));
        Assert.assertFalse(outcome, outcome.contains("producerRecreated"));
        Assert.assertNull("discarded, and not built again until a send needs it",
                getField(producer, "kafkaProducer"));

        logged = capture(() -> send(producer, "payload"));

        Assert.assertEquals(1, rebuilt.sends());
        Assert.assertTrue(logged, ownLine(logged, "producer.rebuild.effect").contains(" outcome=RECOVERED "));

        // Nothing left to discard: another send's rebuild got there first.
        invoke(producer, "triggerReconnect");
        setField(producer, "lastRebuildAttemptMs", 0L);
        logged = capture(() -> producer.handleSendFailure(
                new AuthenticationException("SASL secret rotated"), "producer.single.send",
                new LinkedHashMap<String, String>()));
        Assert.assertTrue(logged, ownLine(logged, "producer.rebuild.outcome").contains(" producerDiscarded=false "));
    }

    // ── a send racing the rebuild ────────────────────────────────────────────────

    /**
     * The producer is discarded after the send passed the check that one exists, here during the
     * topic probe: the send builds a new producer instead of failing with a
     * {@code NullPointerException}.
     */
    @Test
    public void aRebuildBeforeTheSendMakesTheSendBuildANewProducer() throws Exception {
        CpiKafkaPlusProducer producer = newProducer("NONE", 1);
        AcknowledgingClient discarded = install(producer, new AcknowledgingClient());
        AcknowledgingClient rebuilt = new AcknowledgingClient();
        producer.sharedProducerFactory = props -> rebuilt;
        producer.topicChecker.topicProbe = topic -> {
            rebuild(producer);
            return TopicCheckResult.EXISTS;
        };

        Exchange exchange = send(producer, "payload");

        Assert.assertEquals("OK", exchange.getIn().getHeader("CpiKafkaPlusStatus"));
        Assert.assertEquals(0, discarded.sends());
        Assert.assertEquals(1, rebuilt.sends());
    }

    /** The same for a batch: it is sent with the new producer. */
    @Test
    public void aRebuildBeforeTheBatchMakesTheBatchBuildANewProducer() throws Exception {
        CpiKafkaPlusProducer producer = newProducer("JSON_ARRAY", 1);
        AcknowledgingClient discarded = install(producer, new AcknowledgingClient());
        AcknowledgingClient rebuilt = new AcknowledgingClient();
        producer.sharedProducerFactory = props -> rebuilt;
        producer.topicChecker.topicProbe = topic -> {
            rebuild(producer);
            return TopicCheckResult.EXISTS;
        };

        Exchange exchange = send(producer, "[{\"value\":\"a\"},{\"value\":\"b\"}]");

        Assert.assertEquals("OK", exchange.getIn().getHeader("CpiKafkaPlusStatus"));
        Assert.assertEquals(0, discarded.sends());
        Assert.assertEquals(2, rebuilt.sends());
    }

    /**
     * The producer is closed by a rebuild while the send is in {@code send()}: Kafka refuses with
     * "producer has been closed". That is a lost race, not a fault of the producer or the record, so
     * it is transient: no second rebuild, no node-fault count, and with retries off the message fails
     * as transient instead of permanently.
     */
    @Test
    public void aSendWhoseProducerIsClosedUnderItFailsAsTransient() throws Exception {
        CpiKafkaPlusProducer producer = newProducer("NONE", 1);
        RacingClient racing = install(producer, new RacingClient());
        racing.onNextSend(() -> rebuild(producer));

        RuntimeException[] failure = new RuntimeException[1];
        String logged = capture(() -> failure[0] = sendExpectingFailure(producer, "payload"));

        Assert.assertEquals(Classification.RETRIABLE, KafkaErrorHelper.classify(failure[0]));
        Assert.assertTrue(failure[0].getMessage(), failure[0].getMessage().startsWith("Failed to send message "
                + "to Kafka topic 'orders': The shared Kafka producer was closed while this send was using it"));
        Assert.assertTrue(failure[0].getMessage(),
                failure[0].getMessage().contains("Cannot perform operation after producer has been closed"));
        String line = ownLine(logged, "producer.single.send");
        Assert.assertTrue(line, line.contains(" classification=RETRIABLE "));
        Assert.assertTrue(line, line.contains(" rebuildJustified=false "));
        Assert.assertTrue(line, line.contains(" stopReason=RETRY_DISABLED "));
        Assert.assertEquals("a lost race is no node fault", 0, nodeFaultCount(producer));
    }

    /**
     * With retries on, the send that lost the race is repeated on the new producer: the record never
     * reached the closed producer's buffer.
     */
    @Test
    public void aSendWhoseProducerIsClosedUnderItIsRepeatedOnTheNewProducer() throws Exception {
        CpiKafkaPlusProducer producer = newProducer("NONE", 2);
        RacingClient racing = install(producer, new RacingClient());
        racing.onNextSend(() -> rebuild(producer));
        AcknowledgingClient rebuilt = new AcknowledgingClient();
        producer.sharedProducerFactory = props -> rebuilt;

        Exchange exchange = send(producer, "payload");

        Assert.assertEquals("OK", exchange.getIn().getHeader("CpiKafkaPlusStatus"));
        Assert.assertEquals(2, exchange.getIn().getHeader("CamelKafkaPlusRetryAttempts"));
        Assert.assertEquals(1, racing.sends());
        Assert.assertEquals(1, rebuilt.sends());
        Assert.assertEquals(0, nodeFaultCount(producer));
    }

    /**
     * A retry reads the shared producer again: when another send's failure has it rebuilt while the
     * first attempt fails, the second attempt uses the new producer instead of the discarded one or
     * none at all.
     */
    @Test
    public void aRetryAttemptUsesTheProducerOfTheMoment() throws Exception {
        CpiKafkaPlusProducer producer = newProducer("NONE", 2);
        RacingClient racing = install(producer, new RacingClient());
        racing.onNextSend(() -> rebuild(producer));
        racing.answerNextSend(futureFailure(new org.apache.kafka.common.errors.TimeoutException(
                "Topic orders not present in metadata after 1000 ms.")));
        AcknowledgingClient rebuilt = new AcknowledgingClient();
        producer.sharedProducerFactory = props -> rebuilt;

        Exchange exchange = send(producer, "payload");

        Assert.assertEquals("OK", exchange.getIn().getHeader("CpiKafkaPlusStatus"));
        Assert.assertEquals(2, exchange.getIn().getHeader("CamelKafkaPlusRetryAttempts"));
        Assert.assertEquals(1, rebuilt.sends());
    }

    /**
     * A batch whose producer is closed under it fails as transient as well; batches are never
     * repeated.
     */
    @Test
    public void aBatchWhoseProducerIsClosedUnderItFailsAsTransient() throws Exception {
        CpiKafkaPlusProducer producer = newProducer("JSON_ARRAY", 1);
        RacingClient racing = install(producer, new RacingClient());
        racing.onNextSend(() -> rebuild(producer));

        RuntimeException[] failure = new RuntimeException[1];
        String logged = capture(() -> failure[0] = sendExpectingFailure(producer, "[{\"value\":\"a\"}]"));

        Assert.assertEquals(Classification.RETRIABLE, KafkaErrorHelper.classify(failure[0]));
        String line = ownLine(logged, "producer.batch.send");
        Assert.assertTrue(line, line.contains(" classification=RETRIABLE "));
        Assert.assertTrue(line, line.contains(" rebuildJustified=false "));
        Assert.assertEquals(0, nodeFaultCount(producer));
    }

    /**
     * A failure that has its own classification keeps it, also when the producer was replaced
     * meanwhile: only the unrecognised failure of a closed producer is taken for a lost race.
     */
    @Test
    public void aRecognisedFailureKeepsItsClassificationWhenTheProducerWasReplaced() throws Exception {
        CpiKafkaPlusProducer producer = newProducer("NONE", 1);
        RacingClient racing = install(producer, new RacingClient());
        racing.onNextSend(() -> rebuild(producer));
        racing.answerNextSend(futureFailure(new AuthenticationException("SASL secret rotated")));

        RuntimeException failure = sendExpectingFailure(producer, "payload");

        Assert.assertEquals(Classification.FATAL_PRODUCER_UNUSABLE, KafkaErrorHelper.classify(failure));
        Assert.assertEquals(1, nodeFaultCount(producer));
    }

    // ── a send running into the stop ─────────────────────────────────────────────

    /**
     * A send still running when the integration flow stops builds no new producer, since nothing
     * would ever close it: here the stop closes the producer under the first attempt, and the retry
     * fails instead of building one. A message that arrives after the stop builds none either.
     */
    @Test
    public void aSendRunningIntoTheStopBuildsNoNewProducer() throws Exception {
        CpiKafkaPlusProducer producer = newProducer("NONE", 2);
        RacingClient racing = install(producer, new RacingClient());
        racing.onNextSend(() -> stop(producer));
        AtomicInteger built = new AtomicInteger();
        producer.sharedProducerFactory = props -> {
            built.incrementAndGet();
            return new AcknowledgingClient();
        };

        RuntimeException failure = sendExpectingFailure(producer, "payload");

        Assert.assertTrue(failure.getMessage(), failure.getMessage().startsWith("The receiver for Kafka topic "
                + "'orders' was stopped (the integration flow is stopping or was undeployed), so no new Kafka "
                + "producer is built and the message is not sent."));
        Assert.assertEquals(1, racing.sends());
        Assert.assertEquals("no producer may outlive the stop", 0, built.get());
        Assert.assertNull(getField(producer, "kafkaProducer"));

        failure = sendExpectingFailure(producer, "late");
        Assert.assertTrue(failure.getMessage(), failure.getMessage().startsWith("The receiver for Kafka topic "
                + "'orders' was stopped"));
        Assert.assertEquals(0, built.get());
    }

    /** A receiver that is started again after the stop builds its producer again. */
    @Test
    public void aRestartedReceiverBuildsItsProducerAgain() throws Exception {
        CpiKafkaPlusProducer producer = newProducer("NONE", 1);
        install(producer, new AcknowledgingClient());
        stop(producer);
        AcknowledgingClient restarted = new AcknowledgingClient();
        producer.sharedProducerFactory = props -> restarted;

        producer.doStart();
        send(producer, "payload");

        Assert.assertEquals(1, restarted.sends());
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private CpiKafkaPlusProducer newProducer(String batchMode, int maxAttempts) throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(ctx);
        endpoint.setBootstrapServers("localhost:9092");
        endpoint.setSecurityProtocol("PLAINTEXT");
        endpoint.setTopic(TOPIC);
        endpoint.setProducerBatchMode(batchMode);
        endpoint.setProducerRetryMaxAttempts(maxAttempts);
        // The 1 s floor is a start check of retries; doStart is only run here with retries off.
        endpoint.setProducerRetryDelaySeconds(0);
        endpoint.setProducerRetryTotalBudgetSeconds(60);

        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        // Normally assigned in doStart(); injected so the send path can run without a start.
        setField(producer, "tracingHelper", new AdapterTracingHelper(endpoint));
        producer.topicChecker.topicProbe = topic -> TopicCheckResult.EXISTS;
        producers.add(producer);
        return producer;
    }

    /** Hands the producer a shared client, as if it had just been initialized. */
    private <T extends KafkaProducer<byte[], byte[]>> T install(CpiKafkaPlusProducer producer, T client)
            throws Exception {
        setField(producer, "kafkaProducer", client);
        setField(producer, "helpersInitialized", true);
        setField(producer, "initialized", true);
        return client;
    }

    /** What the failure handling of another send does when its failure justifies a rebuild. */
    private static void rebuild(CpiKafkaPlusProducer producer) {
        try {
            invoke(producer, "triggerReconnectWithMeasurement");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** What Camel does when the integration flow is stopped or undeployed. */
    private static void stop(CpiKafkaPlusProducer producer) {
        try {
            producer.doStop();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Exchange send(CpiKafkaPlusProducer producer, String body) throws Exception {
        Exchange exchange = new DefaultExchange(ctx);
        exchange.getIn().setBody(body);
        producer.doProcess(exchange);
        return exchange;
    }

    private RuntimeException sendExpectingFailure(CpiKafkaPlusProducer producer, String body) throws Exception {
        try {
            send(producer, body);
        } catch (RuntimeException e) {
            return e;
        }
        throw new AssertionError("expected the send to fail");
    }

    private static int nodeFaultCount(CpiKafkaPlusProducer producer) throws Exception {
        return ((NodeFaultEscalation) getField(producer, "nodeFaultEscalation")).countInWindow();
    }

    @SuppressWarnings("unchecked")
    private static Future<RecordMetadata> futureFailure(Exception e) throws Exception {
        Constructor<?> ctor = Class.forName(ProducerRetryPolicy.KAFKA_FUTURE_FAILURE_CLASS)
                .getDeclaredConstructor(Exception.class);
        ctor.setAccessible(true);
        return (Future<RecordMetadata>) ctor.newInstance(e);
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Runs {@code action} and returns what was logged meanwhile; slf4j-simple writes to System.err. */
    private static String capture(ThrowingRunnable action) throws Exception {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, "UTF-8"));
        try {
            action.run();
        } finally {
            System.setErr(original);
        }
        return new String(captured.toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * The test thread's diagnostic line for {@code operation}. Clients left behind by other tests
     * keep logging on their own threads.
     */
    private static String ownLine(String logged, String operation) {
        String own = "[" + Thread.currentThread().getName() + "] ";
        for (String line : logged.split("\\R")) {
            if (line.startsWith(own) && line.contains(AdapterDiagnostics.MARKER + " " + operation + " ")) {
                return line;
            }
        }
        throw new AssertionError("no " + operation + " line of this thread in:\n" + logged);
    }

    private static Object invoke(Object target, String name) throws Exception {
        Method method = target.getClass().getDeclaredMethod(name);
        method.setAccessible(true);
        try {
            return method.invoke(target);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw (Error) cause;
        }
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Properties stubProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "shared-producer-rebuild-test");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        return props;
    }

    /** A client that never connects: the metadata is always there, and every send is acknowledged. */
    private final class AcknowledgingClient extends KafkaProducer<byte[], byte[]> {
        private final AtomicInteger sends = new AtomicInteger();

        AcknowledgingClient() {
            super(stubProps(), new ByteArraySerializer(), new ByteArraySerializer());
            clients.add(this);
        }

        int sends() {
            return sends.get();
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return Collections.emptyList();
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            sends.incrementAndGet();
            return CompletableFuture.completedFuture(
                    new RecordMetadata(new TopicPartition(record.topic(), 0), 0L, 0, 0L, 0, 0));
        }

        @Override
        public void close(Duration timeout) {
            super.close(Duration.ZERO);
        }
    }

    /**
     * A client whose next send first runs a hook, the rebuild that races it, and then either answers
     * with a scripted future or does what a real {@code KafkaProducer} does, which on a closed
     * producer is to refuse.
     */
    private final class RacingClient extends KafkaProducer<byte[], byte[]> {
        private final AtomicInteger sends = new AtomicInteger();
        private volatile Runnable nextSendHook;
        private volatile Future<RecordMetadata> nextAnswer;

        RacingClient() {
            super(stubProps(), new ByteArraySerializer(), new ByteArraySerializer());
            clients.add(this);
        }

        void onNextSend(Runnable hook) {
            nextSendHook = hook;
        }

        void answerNextSend(Future<RecordMetadata> answer) {
            nextAnswer = answer;
        }

        int sends() {
            return sends.get();
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return Collections.emptyList();
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            sends.incrementAndGet();
            Runnable hook = nextSendHook;
            if (hook != null) {
                nextSendHook = null;
                hook.run();
            }
            Future<RecordMetadata> answer = nextAnswer;
            if (answer != null) {
                nextAnswer = null;
                return answer;
            }
            return super.send(record);
        }

        @Override
        public void close(Duration timeout) {
            super.close(Duration.ZERO);
        }
    }
}
