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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Pins how the receiver hands out transaction slots and builds the {@code transactional.id} of a
 * transactional batch (ADR 0001): the id layout, one id for every attempt of a message, which slot a
 * message gets, and that a slot comes back after a failure, an {@code Error}, an interrupt and a
 * failed close. The producer runs its real start-up; only the throw-away transactional producer is
 * replaced, through {@code txnProducerFactory}. No broker.
 *
 * <p>Batches enter at {@code processBatch}, the step after the AdminClient topic probe, which would
 * otherwise wait for its timeout on every message without a broker.
 */
public class ProducerTransactionSlotTest {

    private static final String TOPIC = "test-topic";
    /** First four bytes of SHA-256("test-topic"), pinned in {@link NodeIdentityTest}. */
    private static final String TOPIC_HASH = "c6fb053c";
    private static final String PREFIX = "slot-test";
    private static final String BATCH = "[{\"key\": \"k1\", \"value\": \"v1\"}]";
    private static final String PRODUCER_CATEGORY = CpiKafkaPlusProducer.class.getName();
    private static final long WAIT_SECONDS = 10;

    private DefaultCamelContext ctx;
    private final List<CpiKafkaPlusProducer> producers = new ArrayList<>();
    private final List<CountDownLatch> gates = Collections.synchronizedList(new ArrayList<CountDownLatch>());
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Before
    public void setUp() throws Exception {
        // The transactional path probes for a TLS-only listener before it takes a slot.
        TlsListenerProbe.setProbeRunnerForTests(address -> TlsListenerProbe.Verdict.INCONCLUSIVE);
        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        synchronized (gates) {
            for (CountDownLatch gate : gates) {
                gate.countDown();
            }
        }
        executor.shutdownNow();
        for (CpiKafkaPlusProducer producer : producers) {
            producer.doStop();
        }
        TlsListenerProbe.clearCacheForTests();
        ctx.close();
    }

    @Test
    public void startUpBuildsTheIdFromPrefixTopicHashMemberSuffixAndSlot() throws Exception {
        CpiKafkaPlusProducer[] started = new CpiKafkaPlusProducer[1];
        String logged = capture(() -> started[0] = startProducer(3, ""));

        Matcher layout = Pattern.compile("INFO " + Pattern.quote(PRODUCER_CATEGORY + " - "
                + "[CPI-KAFKA-PLUS-DIAG] Transactional batching enabled with max 3 concurrent transactions. "
                + "Prefix: " + PREFIX + ", topicHash: " + TOPIC_HASH + ", memberSuffix: ") + "(\\S+)"
                + Pattern.quote(", example transactional.id: ") + "(\\S+)").matcher(logged);
        Assert.assertTrue("start-up must report the id layout once, under the producer category:\n" + logged,
                layout.find());
        String suffix = layout.group(1);
        String exampleId = layout.group(2);
        Assert.assertEquals(PREFIX + "-" + TOPIC_HASH + "-" + suffix + "-0", exampleId);

        String nodeSuffix = NodeIdentity.memberSuffix();
        if (nodeSuffix == null) {
            Assert.assertTrue(suffix, suffix.matches("r[0-9a-f]{12}"));
            Assert.assertTrue(logged, logged.contains("WARN " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                    + "Could not determine a stable identifier for this worker node, so a random one ('" + suffix
                    + "') will be used instead. This is safe, but note that it will change on every restart."));
        } else {
            Assert.assertEquals(nodeSuffix, suffix);
            Assert.assertFalse(logged, logged.contains("Could not determine a stable identifier"));
        }

        ScriptedFactory factory = install(started[0]);
        send(started[0]);
        send(started[0]);
        Assert.assertEquals("the suffix is resolved once at start-up, and a slot that was given back is "
                + "handed out again", Arrays.asList(exampleId, exampleId), factory.ids);
    }

    @Test
    public void everyAttemptOfAMessageUsesTheSameTransactionalId() throws Exception {
        CpiKafkaPlusProducer producer = startProducer(2, "&deliveryTimeoutSeconds=2"
                + "&producerRetryMaxAttempts=3&producerRetryDelaySeconds=1&producerRetryTotalBudgetSeconds=60");
        // Validated at start-up only; no pause between the attempts keeps the test fast.
        ((CpiKafkaPlusEndpoint) producer.getEndpoint()).setProducerRetryDelaySeconds(0);
        ScriptedFactory factory = install(producer);
        // The first two attempts lose the connection in the SEND phase, which is retried.
        factory.stubs = call -> new StubProducer(
                call <= 2 ? new NetworkException("Disconnected from node 1") : null, null);

        Exchange exchange = send(producer);

        Assert.assertEquals("precondition: the third attempt succeeded",
                3, exchange.getIn().getHeader("CamelKafkaPlusRetryAttempts"));
        Assert.assertEquals("every attempt must use the same transactional.id, so that each producer "
                + "fences the epoch of the one before", Collections.nCopies(3, factory.ids.get(0)), factory.ids);
        Assert.assertEquals(Collections.nCopies(3, 0), slots(factory.ids));
    }

    @Test
    public void aFreeSlotIsHandedOutLowestIndexFirst() throws Exception {
        CpiKafkaPlusProducer producer = startProducer(2, "");
        ScriptedFactory factory = install(producer);
        Hold first = factory.hold(1);

        Future<Exchange> holding = sendAsync(producer);
        first.awaitEntered();
        send(producer);
        send(producer);
        first.release();
        holding.get(WAIT_SECONDS, TimeUnit.SECONDS);
        send(producer);

        // Slot 0 is held while the next two messages run, so both get slot 1; once everything is
        // free again the next message gets slot 0.
        Assert.assertEquals(Arrays.asList(0, 1, 1, 0), slots(factory.ids));
        Assert.assertEquals("all slots share one id space and differ only in the slot",
                1, idSpaces(factory.ids).size());
    }

    @Test
    public void aFailedMessageGivesItsSlotBack() throws Exception {
        CpiKafkaPlusProducer producer = startProducer(1, "");
        ScriptedFactory factory = install(producer);
        factory.fail(1, new NetworkException("injected: broker unreachable"));

        String logged = capture(() -> {
            try {
                send(producer);
                Assert.fail("expected the transactional send to fail");
            } catch (RuntimeException e) {
                Assert.assertEquals("Failed to send transactional batch to Kafka topic '" + TOPIC
                        + "': injected: broker unreachable", e.getMessage());
            }
        });

        Assert.assertTrue("the failure report names the id, the slot and the topic hash:\n" + logged,
                logged.contains(" topic=" + TOPIC + " batchMode=JSON_ARRAY transactionalId=" + factory.ids.get(0)
                        + " slotId=0 topicHash=" + TOPIC_HASH + " retryAttempts=1 txnPhase=CREATE "));
        // With a single slot the next message could never start if the failed one had kept it.
        sendAsync(producer).get(WAIT_SECONDS, TimeUnit.SECONDS);
        Assert.assertEquals(Arrays.asList(0, 0), slots(factory.ids));
    }

    @Test
    public void anErrorStillGivesTheSlotBack() throws Exception {
        CpiKafkaPlusProducer producer = startProducer(1, "");
        ScriptedFactory factory = install(producer);
        factory.fail(1, new NoClassDefFoundError("injected"));

        try {
            send(producer);
            Assert.fail("expected the Error to reach the caller");
        } catch (NoClassDefFoundError expected) {
            // The attempt loop catches Exception only; the slot is given back all the same.
        }

        sendAsync(producer).get(WAIT_SECONDS, TimeUnit.SECONDS);
        Assert.assertEquals(Arrays.asList(0, 0), slots(factory.ids));
    }

    @Test
    public void anInterruptWhileWaitingForASlotFailsOnlyThatMessage() throws Exception {
        CpiKafkaPlusProducer producer = startProducer(1, "");
        ScriptedFactory factory = install(producer);
        Hold first = factory.hold(1);
        Future<Exchange> holding = sendAsync(producer);
        first.awaitEntered();

        SendingThread waiting = new SendingThread(producer);
        waiting.start();
        awaitWaitingForSlot(waiting);
        waiting.interrupt();
        waiting.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));

        Throwable failure = waiting.failure.get();
        Assert.assertTrue(String.valueOf(failure), failure instanceof RuntimeException);
        Assert.assertEquals("Interrupted while waiting for a transaction slot", failure.getMessage());
        Assert.assertTrue(String.valueOf(failure.getCause()), failure.getCause() instanceof InterruptedException);
        Assert.assertTrue("the interrupt flag must be set again for the caller", waiting.interruptedWhenFailed);
        Assert.assertEquals("the interrupted message never got a slot", 1, factory.ids.size());

        first.release();
        holding.get(WAIT_SECONDS, TimeUnit.SECONDS);

        // The interrupted wait neither took nor returned a permit: a message that arrives while the
        // single slot is held still has to wait. A spare permit would let it through to "Acquired
        // semaphore but no slot was free" instead.
        Hold second = factory.hold(2);
        Future<Exchange> holdingAgain = sendAsync(producer);
        second.awaitEntered();
        SendingThread late = new SendingThread(producer);
        late.start();
        awaitWaitingForSlot(late);
        second.release();
        holdingAgain.get(WAIT_SECONDS, TimeUnit.SECONDS);
        late.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        Assert.assertNull(String.valueOf(late.failure.get()), late.failure.get());
        Assert.assertEquals(Arrays.asList(0, 0, 0), slots(factory.ids));
    }

    @Test
    public void aFailedCloseIsLoggedAndTheSlotStillComesBack() throws Exception {
        CpiKafkaPlusProducer producer = startProducer(1, "");
        ScriptedFactory factory = install(producer);
        factory.stubs = call -> new StubProducer(null,
                call == 1 ? new KafkaException("injected close failure") : null);

        String logged = capture(() -> send(producer));

        Assert.assertTrue(logged, logged.contains("WARN " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "Failed to close transactional producer for slot 0: injected close failure"));
        Assert.assertEquals("the close is bounded", Duration.ofSeconds(5), factory.created.get(0).closeTimeout);
        sendAsync(producer).get(WAIT_SECONDS, TimeUnit.SECONDS);
        Assert.assertEquals(Arrays.asList(0, 0), slots(factory.ids));
    }

    /**
     * Pins today's behaviour, which issue #204 proposes to change: a message that took its slot
     * before a stop/start gives it back to the slots of the new start. A message after the restart
     * can then get a slot that another one still holds, under the same {@code transactional.id}.
     * The fix for #204 is expected to turn this test around.
     */
    @Test
    public void aSlotTakenBeforeARestartIsReleasedIntoTheNewStart() throws Exception {
        CpiKafkaPlusProducer producer = startProducer(1, "");
        ScriptedFactory factory = install(producer);
        Hold beforeRestart = factory.hold(1);
        Hold afterRestart = factory.hold(2);

        Future<Exchange> first = sendAsync(producer);
        beforeRestart.awaitEntered();
        producer.doStop();
        producer.doStart();
        Future<Exchange> second = sendAsync(producer);
        afterRestart.awaitEntered();

        beforeRestart.release();
        first.get(WAIT_SECONDS, TimeUnit.SECONDS);
        sendAsync(producer).get(WAIT_SECONDS, TimeUnit.SECONDS);

        Assert.assertFalse("precondition: the second message still holds its slot", second.isDone());
        Assert.assertEquals("the third message ran on the slot the second one holds",
                factory.ids.get(1), factory.ids.get(2));
        afterRestart.release();
        second.get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void theDefaultFactoryBuildsAPlainClientUnderTheBundleClassLoader() throws Exception {
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri(1, ""));
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, PREFIX + "-" + TOPIC_HASH + "-default-0");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ContextClassLoaderProbe.class.getName());
        ClassLoader callers = Thread.currentThread().getContextClassLoader();
        ContextClassLoaderProbe.seen = null;

        KafkaProducer<byte[], byte[]> created = producer.txnProducerFactory.apply(props);
        try {
            Assert.assertEquals(KafkaProducer.class, created.getClass());
            Assert.assertTrue("Kafka clients are created under the bundle class loader (SASL/SSL in OSGi): "
                    + ContextClassLoaderProbe.seen, ContextClassLoaderProbe.seen instanceof BundleBackedClassLoader);
            Assert.assertSame(callers, Thread.currentThread().getContextClassLoader());
        } finally {
            created.close(Duration.ZERO);
        }

        Properties invalid = ProducerConfigFactory.buildProducerProperties(endpoint);
        invalid.put(ProducerConfig.ACKS_CONFIG, "sometimes");
        try {
            producer.txnProducerFactory.apply(invalid);
            Assert.fail("expected the invalid acks value to be rejected");
        } catch (ConfigException e) {
            Assert.assertEquals("passed on as thrown, not wrapped: the retry classification reads it",
                    ConfigException.class, e.getClass());
        }
        Assert.assertSame(callers, Thread.currentThread().getContextClassLoader());
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static String uri(int slots, String extraParams) {
        return "cpi-kafka-plus:" + TOPIC + "?bootstrapServers=localhost:9999&securityProtocol=PLAINTEXT"
                + "&producerBatchMode=JSON_ARRAY&enableTransactions=true&transactionalIdPrefix=" + PREFIX
                + "&maxConcurrentTransactions=" + slots + extraParams;
    }

    private CpiKafkaPlusProducer startProducer(int slots, String extraParams) throws Exception {
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri(slots, extraParams));
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        producers.add(producer);
        producer.doStart();
        return producer;
    }

    private ScriptedFactory install(CpiKafkaPlusProducer producer) {
        ScriptedFactory factory = new ScriptedFactory();
        producer.txnProducerFactory = factory;
        return factory;
    }

    private Future<Exchange> sendAsync(CpiKafkaPlusProducer producer) {
        return executor.submit(() -> send(producer));
    }

    /** Sends {@link #BATCH} through the transactional path of a started producer. */
    private static Exchange send(CpiKafkaPlusProducer producer) throws Exception {
        Exchange exchange = new DefaultExchange(producer.getEndpoint().getCamelContext());
        exchange.getIn().setBody(BATCH);
        Method processBatch = CpiKafkaPlusProducer.class.getDeclaredMethod(
                "processBatch", Exchange.class, Message.class, String.class, String.class);
        processBatch.setAccessible(true);
        try {
            processBatch.invoke(producer, exchange, exchange.getIn(), TOPIC, "JSON_ARRAY");
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw e;
        }
        return exchange;
    }

    /** Waits until {@code thread} is parked on the transaction slot semaphore. */
    private static void awaitWaitingForSlot(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            if (thread.getState() == Thread.State.WAITING && waitsOnSemaphore(thread)) {
                return;
            }
            Thread.sleep(10);
        }
        Assert.fail(thread.getName() + " never waited for a transaction slot: "
                + Arrays.toString(thread.getStackTrace()));
    }

    private static boolean waitsOnSemaphore(Thread thread) {
        for (StackTraceElement frame : thread.getStackTrace()) {
            if (frame.getClassName().equals(Semaphore.class.getName()) && frame.getMethodName().equals("acquire")) {
                return true;
            }
        }
        return false;
    }

    private static List<Integer> slots(List<String> ids) {
        List<Integer> slots = new ArrayList<>();
        synchronized (ids) {
            for (String id : ids) {
                slots.add(Integer.valueOf(id.substring(id.lastIndexOf('-') + 1)));
            }
        }
        return slots;
    }

    private static Set<String> idSpaces(List<String> ids) {
        Set<String> spaces = new HashSet<>();
        synchronized (ids) {
            for (String id : ids) {
                spaces.add(id.substring(0, id.lastIndexOf('-')));
            }
        }
        return spaces;
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

    /** A call of the factory that waits inside its slot until released. */
    private final class Hold {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        Hold() {
            gates.add(released);
        }

        void awaitEntered() throws InterruptedException {
            Assert.assertTrue("the held message never reached its slot", entered.await(WAIT_SECONDS, TimeUnit.SECONDS));
        }

        void release() {
            released.countDown();
        }
    }

    /**
     * Stands in for {@code txnProducerFactory}: records the {@code transactional.id} of every call
     * and can hold a call inside its slot or fail it. Calls are counted from 1, one per attempt.
     */
    private final class ScriptedFactory implements Function<Properties, KafkaProducer<byte[], byte[]>> {
        final List<String> ids = Collections.synchronizedList(new ArrayList<String>());
        final List<StubProducer> created = Collections.synchronizedList(new ArrayList<StubProducer>());
        volatile IntFunction<StubProducer> stubs = call -> new StubProducer(null, null);
        private final AtomicInteger calls = new AtomicInteger();
        private final Map<Integer, Hold> holds = new ConcurrentHashMap<>();
        private final Map<Integer, Throwable> failures = new ConcurrentHashMap<>();

        Hold hold(int call) {
            Hold hold = new Hold();
            holds.put(call, hold);
            return hold;
        }

        void fail(int call, Throwable failure) {
            failures.put(call, failure);
        }

        @Override
        public KafkaProducer<byte[], byte[]> apply(Properties props) {
            ids.add(props.getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
            int call = calls.incrementAndGet();
            Hold hold = holds.get(call);
            if (hold != null) {
                hold.entered.countDown();
                try {
                    hold.released.await(WAIT_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            Throwable failure = failures.get(call);
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            StubProducer producer = stubs.apply(call);
            created.add(producer);
            return producer;
        }
    }

    /** Sends one batch on its own thread, so the test can see where it waits and interrupt it. */
    private static final class SendingThread extends Thread {
        private final CpiKafkaPlusProducer producer;
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        volatile boolean interruptedWhenFailed;

        SendingThread(CpiKafkaPlusProducer producer) {
            super("transaction-slot-test-sender");
            this.producer = producer;
        }

        @Override
        public void run() {
            try {
                send(producer);
            } catch (Throwable t) {
                interruptedWhenFailed = Thread.currentThread().isInterrupted();
                failure.set(t);
            }
        }
    }

    /** Acknowledges every send; transaction calls are no-ops. Fails send() or close() when told to. */
    private static final class StubProducer extends KafkaProducer<byte[], byte[]> {
        private final RuntimeException sendFailure;
        private final RuntimeException closeFailure;
        volatile Duration closeTimeout;

        StubProducer(RuntimeException sendFailure, RuntimeException closeFailure) {
            super(stubProps(), new ByteArraySerializer(), new ByteArraySerializer());
            this.sendFailure = sendFailure;
            this.closeFailure = closeFailure;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            if (sendFailure != null) {
                throw sendFailure;
            }
            return CompletableFuture.completedFuture(new RecordMetadata(
                    new TopicPartition(record.topic(), 0), 0, 0, System.currentTimeMillis(), 0, 0));
        }

        @Override
        public void initTransactions() {
        }

        @Override
        public void beginTransaction() {
        }

        @Override
        public void commitTransaction() {
        }

        @Override
        public void close(Duration timeout) {
            closeTimeout = timeout;
            super.close(Duration.ZERO);
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static Properties stubProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-transaction-slot-test");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        return props;
    }

    /** Records the context class loader a client is constructed under. */
    public static final class ContextClassLoaderProbe implements ProducerInterceptor<byte[], byte[]> {
        static volatile ClassLoader seen;

        public ContextClassLoaderProbe() {
            seen = Thread.currentThread().getContextClassLoader();
        }

        @Override
        public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }
}
