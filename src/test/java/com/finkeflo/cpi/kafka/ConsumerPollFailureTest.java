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
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * What one scheduler tick of the sender does when the Kafka client's {@code poll()} fails, for each
 * kind of failure and for both polls that can fail: the emit poll that fetches records and the
 * keep-alive poll between two emit cycles. Both first decide which kind of failure it is (credentials,
 * permissions or certificates rejected; the static member fenced; the adapter's class space replaced;
 * anything else) and run that kind's handler. Only "anything else" is handled differently by the two.
 *
 * <p>No broker: the Kafka client is a real {@link KafkaConsumer} whose {@code poll()} throws.
 */
public class ConsumerPollFailureTest {

    private static final String CONSUMER = CpiKafkaPlusConsumer.class.getName();

    /** The text OSGi puts into a class load over the purged revision of an updated adapter. */
    private static final String WIRING_GONE = "Unable to load class "
            + "'org.apache.kafka.clients.consumer.internals.ConsumerCoordinator$3' because the bundle wiring "
            + "for com.finkeflo.cpi.kafka.cpi-kafka-adapter-plus is no longer valid.";

    private static final String STOPPED_BY_CREDENTIALS = "Non-retryable Kafka consumer poll failure detected "
            + "for topic='orders' group='g' (%s). Polling has been stopped by error policy. Fix credentials, "
            + "permissions, or certificates and redeploy the integration flow to resume consumption.";

    private static final String STOPPED_BY_CLASS_SPACE = "The adapter bundle was updated while this "
            + "integration flow was running; the class space of this route (topic='orders' group='g') is gone "
            + "and cannot be recovered from inside the adapter. Polling has been stopped. Redeploy the "
            + "integration flow to bind it to the new adapter version.";

    /** The two polls of a tick. */
    private enum Path {
        /** The first tick after the start: an emit cycle, whose poll fetches records. */
        EMIT,
        /** A tick between two emit cycles: the keep-alive poll, which only drives the group protocol. */
        KEEP_ALIVE
    }

    private DefaultCamelContext ctx;
    private final List<FailingKafkaConsumer> clients = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        for (FailingKafkaConsumer client : clients) {
            client.release();
        }
        ctx.close();
    }

    // --- credentials, permissions or certificates rejected: stop polling until a redeploy ---

    @Test
    public void rejectedCredentialsStopTheEmitPoll() throws Exception {
        SaslAuthenticationException failure = new SaslAuthenticationException("bad credentials");
        Tick tick = tick(Path.EMIT, failure);

        Assert.assertSame(failure, tick.thrown);
        assertStoppedByErrorPolicy(tick, String.format(STOPPED_BY_CREDENTIALS, "SaslAuthenticationException"));
        assertFailedLineFirst(tick, "poll: NON-RETRYABLE failure state active");
        assertNextTickDoesNotPoll(tick);
    }

    @Test
    public void rejectedCredentialsStopTheKeepAlivePoll() throws Exception {
        SaslAuthenticationException failure = new SaslAuthenticationException("bad credentials");
        Tick tick = tick(Path.KEEP_ALIVE, failure);

        assertSwallowed(tick);
        assertStoppedByErrorPolicy(tick, String.format(STOPPED_BY_CREDENTIALS, "SaslAuthenticationException"));
        assertNotLogged(tick, "poll: FAILED");
        assertNextTickDoesNotPoll(tick);
    }

    // --- static member fenced (KIP-345): close, then cool down before the rebuild ---

    @Test
    public void aFencedInstanceCoolsDownTheEmitPoll() throws Exception {
        FencedInstanceIdException failure = new FencedInstanceIdException("fenced");
        Tick tick = tick(Path.EMIT, failure);

        Assert.assertSame(failure, tick.thrown);
        assertFencedCooldown(tick);
        assertFailedLineFirst(tick, "poll: FencedInstanceIdException");
        assertNextTickDoesNotPoll(tick);
    }

    @Test
    public void aFencedInstanceCoolsDownTheKeepAlivePoll() throws Exception {
        FencedInstanceIdException failure = new FencedInstanceIdException("fenced");
        Tick tick = tick(Path.KEEP_ALIVE, failure);

        assertSwallowed(tick);
        assertFencedCooldown(tick);
        assertNotLogged(tick, "poll: FAILED");
        assertNextTickDoesNotPoll(tick);
    }

    // --- the adapter's class space was replaced by an update: stop polling until a redeploy ---

    @Test
    public void aGoneClassSpaceStopsTheEmitPoll() throws Exception {
        NoClassDefFoundError failure = new NoClassDefFoundError(
                "org/apache/kafka/clients/consumer/internals/ConsumerCoordinator$3");
        failure.initCause(new ClassNotFoundException(WIRING_GONE));
        Tick tick = tick(Path.EMIT, failure);

        Assert.assertTrue(String.valueOf(tick.thrown), tick.thrown instanceof RuntimeException);
        Assert.assertEquals("Non-Exception Throwable from poll: java.lang.NoClassDefFoundError",
                tick.thrown.getMessage());
        Assert.assertSame(failure, tick.thrown.getCause());
        assertStoppedByClassSpace(tick);
        assertFailedLineFirst(tick, "poll.bundle-wiring-invalid");
        assertNextTickDoesNotPoll(tick);
    }

    @Test
    public void aGoneClassSpaceStopsTheKeepAlivePoll() throws Exception {
        KafkaException failure = new KafkaException("Unexpected error in join group response",
                new ClassNotFoundException(WIRING_GONE));
        Tick tick = tick(Path.KEEP_ALIVE, failure);

        assertSwallowed(tick);
        assertStoppedByClassSpace(tick);
        assertNotLogged(tick, "poll: FAILED");
        assertNextTickDoesNotPoll(tick);
    }

    /**
     * The keep-alive poll catches {@code Exception} only, so an {@code Error} such as a
     * {@code NoClassDefFoundError} leaves it without being classified (finding N1 of the
     * refactoring plan, open). Fixing that is a change in behaviour and changes this test.
     */
    @Test
    public void anErrorLeavesTheKeepAlivePollUnclassified() throws Exception {
        NoClassDefFoundError failure = new NoClassDefFoundError(
                "org/apache/kafka/clients/consumer/internals/ConsumerCoordinator$3");
        failure.initCause(new ClassNotFoundException(WIRING_GONE));
        Tick tick = tick(Path.KEEP_ALIVE, failure);

        Assert.assertSame(failure, tick.thrown);
        assertUntouched(tick);
        Assert.assertTrue(tick.tracing.published.isEmpty());
        assertNotLogged(tick, "poll.bundle-wiring-invalid");
        assertNotLogged(tick, "keepAlivePoll:");
    }

    // --- anything else: the emit poll counts it towards a reconnect, the keep-alive only warns ---

    @Test
    public void anyOtherFailureCountsTowardsAReconnectOnTheEmitPoll() throws Exception {
        TimeoutException failure = new TimeoutException("broker timeout");
        Tick tick = tick(Path.EMIT, failure);

        Assert.assertSame(failure, tick.thrown);
        Assert.assertEquals(Boolean.FALSE, tick.field("stoppedByErrorPolicy"));
        Assert.assertEquals(0L, tick.field("fencedCooldownUntilMs"));
        Assert.assertEquals(1, tick.field("consecutivePollFailures"));
        long firstFailure = (Long) tick.field("firstPollFailureMs");
        Assert.assertTrue(firstFailure >= tick.startedMs && firstFailure <= tick.endedMs);
        assertClientKept(tick);
        assertPublishedOnce(tick, failure);
        assertLogged(tick, failedLine(failure));
        assertNotLogged(tick, "poll: closing consumer for reconnect");
        assertNotLogged(tick, "does not use TLS");
    }

    @Test
    public void anyOtherFailureNamesAMissingTlsOnTheEmitPoll() throws Exception {
        TimeoutException failure = new TimeoutException("broker timeout");
        Tick tick = tick(Path.EMIT, failure, "PLAINTEXT");

        Assert.assertSame(failure, tick.thrown);
        Assert.assertEquals(1, tick.field("consecutivePollFailures"));
        assertPublishedOnce(tick, failure);
        int failed = indexOf(tick, failedLine(failure));
        int hint = indexOf(tick, "WARN " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] poll: securityProtocol=PLAINTEXT "
                + "does not use TLS.");
        Assert.assertTrue(tick.log(), failed >= 0 && hint > failed);
    }

    @Test
    public void anyOtherFailureIsOnlyWarnedAboutByTheKeepAlivePoll() throws Exception {
        TimeoutException failure = new TimeoutException("broker timeout");
        Tick tick = tick(Path.KEEP_ALIVE, failure, "PLAINTEXT");

        assertSwallowed(tick);
        assertUntouched(tick);
        Assert.assertTrue(tick.tracing.published.isEmpty());
        assertLogged(tick, "WARN " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] keepAlivePoll: best-effort poll "
                + "failed: broker timeout");
        assertNotLogged(tick, "poll: FAILED");
        assertNotLogged(tick, "does not use TLS");
    }

    // --- a failure of more than one kind: the first kind in the order above wins, in both polls ---

    @Test
    public void rejectedCredentialsWinOverAFencedInstance() throws Exception {
        for (Path path : Path.values()) {
            Tick tick = tick(path, new SaslAuthenticationException("bad credentials",
                    new FencedInstanceIdException("fenced")));
            assertStoppedByErrorPolicy(tick, String.format(STOPPED_BY_CREDENTIALS, "SaslAuthenticationException"));
        }
    }

    @Test
    public void rejectedCredentialsWinOverAGoneClassSpace() throws Exception {
        for (Path path : Path.values()) {
            Tick tick = tick(path, new SslAuthenticationException("SSL handshake failed",
                    new ClassNotFoundException(WIRING_GONE)));
            assertStoppedByErrorPolicy(tick, String.format(STOPPED_BY_CREDENTIALS, "SslAuthenticationException"));
            assertNotLogged(tick, "poll.bundle-wiring-invalid");
        }
    }

    @Test
    public void aFencedInstanceWinsOverAGoneClassSpace() throws Exception {
        for (Path path : Path.values()) {
            Tick tick = tick(path, new FencedInstanceIdException("fenced", new ClassNotFoundException(WIRING_GONE)));
            assertFencedCooldown(tick);
            assertNotLogged(tick, "poll.bundle-wiring-invalid");
        }
    }

    // --- a wakeup on stop is not a failure ---

    @Test
    public void aWakeupEndsTheEmitPollWithoutAFailure() throws Exception {
        Tick tick = tick(Path.EMIT, new WakeupException());

        assertSwallowed(tick);
        assertUntouched(tick);
        Assert.assertTrue(tick.tracing.published.isEmpty());
        assertLogged(tick, "INFO " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] poll: received wakeup signal, stopping "
                + "poll loop");
        assertNotLogged(tick, "poll: FAILED");
    }

    @Test
    public void aWakeupEndsTheKeepAlivePollWithoutAFailure() throws Exception {
        Tick tick = tick(Path.KEEP_ALIVE, new WakeupException());

        assertSwallowed(tick);
        assertUntouched(tick);
        Assert.assertTrue(tick.tracing.published.isEmpty());
        assertLogged(tick, "INFO " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] keepAlivePoll: received wakeup signal");
    }

    // --- what each handler leaves behind ---

    private static void assertStoppedByErrorPolicy(Tick tick, String policyMessage) throws Exception {
        Throwable policyError = assertStopped(tick, policyMessage);
        assertLogged(tick, "ERROR " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] poll: NON-RETRYABLE failure state "
                + "active for topic='orders' group='g'. Polling remains stopped until operator intervention (fix "
                + "credentials/permissions/certificates and redeploy). policyErrorClass=java.lang.IllegalStateException "
                + "policyErrorMsg='" + policyError.getMessage() + "' topStack=");
    }

    private static void assertStoppedByClassSpace(Tick tick) throws Exception {
        assertStopped(tick, STOPPED_BY_CLASS_SPACE);
        int event = indexOf(tick, "ERROR " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] poll.bundle-wiring-invalid "
                + "topic=orders groupId=g classSpace=");
        int reminder = indexOf(tick, "ERROR " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] poll: NON-RETRYABLE failure "
                + "state active for topic='orders' group='g'.");
        Assert.assertTrue(tick.log(), event >= 0 && reminder > event);
        Assert.assertTrue(tick.lines.get(event), tick.lines.get(event).contains("action='redeploy the integration flow'"));
    }

    /** Stopped by error policy: closed, reported, and nothing counted towards a reconnect. */
    private static Throwable assertStopped(Tick tick, String policyMessage) throws Exception {
        assertNoOtherBranch(tick);
        Assert.assertEquals(Boolean.TRUE, tick.field("stoppedByErrorPolicy"));
        Throwable policyError = (Throwable) tick.field("stoppedByError");
        Assert.assertTrue(String.valueOf(policyError), policyError instanceof IllegalStateException);
        Assert.assertEquals(policyMessage, policyError.getMessage());
        Assert.assertSame(tick.failure, policyError.getCause());
        assertClientClosed(tick);
        Assert.assertEquals(0, tick.field("consecutivePollFailures"));
        Assert.assertEquals(0L, tick.field("firstPollFailureMs"));
        Assert.assertEquals(0L, tick.field("fencedCooldownUntilMs"));
        Assert.assertEquals(0, tick.field("fencedBackoffAttempt"));
        assertPublishedOnce(tick, policyError);
        return policyError;
    }

    /** Fenced: closed, reported, and a cooldown of 40 s plus up to 10 s jitter before the rebuild. */
    private static void assertFencedCooldown(Tick tick) throws Exception {
        assertNoOtherBranch(tick);
        Assert.assertEquals(Boolean.FALSE, tick.field("stoppedByErrorPolicy"));
        Assert.assertNull(tick.field("stoppedByError"));
        assertClientClosed(tick);
        Assert.assertEquals(0, tick.field("consecutivePollFailures"));
        Assert.assertEquals(0L, tick.field("firstPollFailureMs"));
        Assert.assertEquals(1, tick.field("fencedBackoffAttempt"));
        long until = (Long) tick.field("fencedCooldownUntilMs");
        Assert.assertTrue("cooldown until " + until, until >= tick.startedMs + 40_000L
                && until < tick.endedMs + 50_000L);
        assertPublishedOnce(tick, tick.failure);

        String prefix = "ERROR " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] poll: FencedInstanceIdException for "
                + "topic='orders' group='g' (static member id conflict, KIP-345). Cooling down for ";
        int line = indexOf(tick, prefix);
        Assert.assertTrue(tick.log(), line >= 0);
        String text = tick.lines.get(line);
        Matcher m = Pattern.compile("Cooling down for (\\d+)ms \\(attempt=1\\) before rebuilding the consumer, "
                + "instead of rebuilding immediately, so the stale broker-side session has time to expire\\. "
                + "exMsg='" + Pattern.quote(tick.failure.getMessage()) + "'$").matcher(text);
        Assert.assertTrue(text, m.find());
        long cooldownMs = Long.parseLong(m.group(1));
        Assert.assertTrue(text, cooldownMs >= 40_000L && cooldownMs < 50_000L);
    }

    /**
     * The keep-alive poll's "anything else" branch did not run as well. The emit poll's would show
     * in the reconnect counters, which the callers of this check expect at zero.
     */
    private static void assertNoOtherBranch(Tick tick) {
        assertNotLogged(tick, "keepAlivePoll: best-effort poll failed");
    }

    private static void assertClientClosed(Tick tick) throws Exception {
        Assert.assertEquals(Boolean.FALSE, tick.field("initialized"));
        Assert.assertEquals(1, tick.client.closes);
        Assert.assertNull(tick.field("kafkaConsumer"));
    }

    private static void assertClientKept(Tick tick) throws Exception {
        Assert.assertEquals(Boolean.TRUE, tick.field("initialized"));
        Assert.assertEquals(0, tick.client.closes);
        Assert.assertSame(tick.client, tick.field("kafkaConsumer"));
    }

    /** No handler ran: nothing stopped, nothing counted, the client kept. */
    private static void assertUntouched(Tick tick) throws Exception {
        Assert.assertEquals(Boolean.FALSE, tick.field("stoppedByErrorPolicy"));
        Assert.assertEquals(0L, tick.field("fencedCooldownUntilMs"));
        Assert.assertEquals(0, tick.field("fencedBackoffAttempt"));
        Assert.assertEquals(0, tick.field("consecutivePollFailures"));
        Assert.assertEquals(0L, tick.field("firstPollFailureMs"));
        assertClientKept(tick);
    }

    private static void assertSwallowed(Tick tick) {
        Assert.assertNull(String.valueOf(tick.thrown), tick.thrown);
        Assert.assertEquals(0, tick.returned);
    }

    private static void assertPublishedOnce(Tick tick, Throwable error) {
        Assert.assertEquals(1, tick.tracing.published.size());
        Assert.assertEquals(Boolean.FALSE, tick.tracing.published.get(0));
        Assert.assertSame(error, tick.tracing.errors.get(0));
    }

    /** The emit poll logs the failure before the handler's own line. */
    private static void assertFailedLineFirst(Tick tick, String handlerLine) {
        int failed = indexOf(tick, failedLine(tick.failure));
        int handled = -1;
        for (int i = 0; i < tick.lines.size(); i++) {
            if (tick.lines.get(i).contains(handlerLine)) {
                handled = i;
                break;
            }
        }
        Assert.assertTrue(tick.log(), failed >= 0 && handled > failed);
    }

    /** The handler's state holds on the next tick: it returns without polling a fresh client. */
    private void assertNextTickDoesNotPoll(Tick tick) throws Exception {
        FailingKafkaConsumer next = client(new IllegalStateException("must not be polled"));
        tick.set("kafkaConsumer", next);
        tick.set("initialized", true);
        Assert.assertEquals(0, tick.consumer.poll());
        Assert.assertEquals(0, next.polls);
    }

    // --- one tick ---

    private Tick tick(Path path, Throwable failure) throws Exception {
        return tick(path, failure, "SASL_SSL");
    }

    /** Runs one scheduler tick of a fresh, initialised consumer whose Kafka client fails its poll. */
    private Tick tick(Path path, Throwable failure, String securityProtocol) throws Exception {
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint("cpi-kafka-plus:orders"
                + "?bootstrapServers=localhost:9092&groupId=g&pollingIntervalSeconds=600"
                + "&securityProtocol=" + securityProtocol);
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, exchange -> { });
        Tick tick = new Tick(consumer, client(failure), new RecordingTracingHelper(endpoint), failure);
        tick.set("kafkaConsumer", tick.client);
        tick.set("initialized", true);
        tick.set("tracingHelper", tick.tracing);
        if (path == Path.KEEP_ALIVE) {
            // The last emit cycle just ended, so the next one is not due for pollingIntervalSeconds.
            tick.set("lastEmitTimeMs", System.currentTimeMillis());
        }

        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, "UTF-8"));
        tick.startedMs = System.currentTimeMillis();
        try {
            tick.returned = consumer.poll();
        } catch (Throwable t) {
            tick.thrown = t;
        } finally {
            tick.endedMs = System.currentTimeMillis();
            System.setErr(original);
        }
        // Kafka clients left behind by other tests keep logging on their own threads.
        String own = "[" + Thread.currentThread().getName() + "] ";
        for (String line : new String(captured.toByteArray(), StandardCharsets.UTF_8).split("\\R")) {
            if (line.startsWith(own)) {
                tick.lines.add(line.substring(own.length()));
            }
        }
        Assert.assertEquals("the tick must have polled the client once", 1, tick.client.polls);
        return tick;
    }

    private FailingKafkaConsumer client(Throwable failure) {
        FailingKafkaConsumer client = new FailingKafkaConsumer(failure);
        clients.add(client);
        return client;
    }

    private static String failedLine(Throwable failure) {
        Throwable cause = failure.getCause();
        return "ERROR " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] poll: FAILED topic='orders' group='g' exClass="
                + failure.getClass().getName() + " exMsg='" + failure.getMessage() + "' causeClass="
                + (cause != null ? cause.getClass().getName() : "null") + " causeMsg='"
                + (cause != null ? cause.getMessage() : "null") + "' topStack=";
    }

    private static int indexOf(Tick tick, String linePrefix) {
        for (int i = 0; i < tick.lines.size(); i++) {
            if (tick.lines.get(i).startsWith(linePrefix)) {
                return i;
            }
        }
        return -1;
    }

    private static void assertLogged(Tick tick, String linePrefix) {
        Assert.assertTrue("missing: " + linePrefix + "\nin:\n" + tick.log(), indexOf(tick, linePrefix) >= 0);
    }

    private static void assertNotLogged(Tick tick, String text) {
        for (String line : tick.lines) {
            Assert.assertFalse("unexpected: " + text + "\nin:\n" + tick.log(), line.contains(text));
        }
    }

    private static final class Tick {
        final CpiKafkaPlusConsumer consumer;
        final FailingKafkaConsumer client;
        final RecordingTracingHelper tracing;
        final Throwable failure;
        final List<String> lines = new ArrayList<>();
        long startedMs;
        long endedMs;
        int returned = -1;
        Throwable thrown;

        Tick(CpiKafkaPlusConsumer consumer, FailingKafkaConsumer client, RecordingTracingHelper tracing,
                Throwable failure) {
            this.consumer = consumer;
            this.client = client;
            this.tracing = tracing;
            this.failure = failure;
        }

        Object field(String name) throws Exception {
            Field f = CpiKafkaPlusConsumer.class.getDeclaredField(name);
            f.setAccessible(true);
            return f.get(consumer);
        }

        void set(String name, Object value) throws Exception {
            Field f = CpiKafkaPlusConsumer.class.getDeclaredField(name);
            f.setAccessible(true);
            f.set(consumer, value);
        }

        String log() {
            return String.join("\n", lines);
        }
    }

    /**
     * A Kafka client that never reaches a broker: {@code poll()} throws the given failure, and
     * {@code close()} is only counted, so the test still sees which handler closed it. The client
     * itself is released after the test.
     */
    private static final class FailingKafkaConsumer extends KafkaConsumer<byte[], byte[]> {
        private final Throwable failure;
        int polls;
        int closes;

        FailingKafkaConsumer(Throwable failure) {
            super(config(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
            this.failure = failure;
        }

        private static Properties config() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            return props;
        }

        @Override
        public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
            polls++;
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new AssertionError("poll() can only throw unchecked failures", failure);
        }

        @Override
        public void close(Duration timeout) {
            closes++;
        }

        void release() {
            super.close(Duration.ZERO);
        }
    }

    /** Records the connection status the consumer publishes to the CPI monitor. */
    private static final class RecordingTracingHelper extends AdapterTracingHelper {
        final List<Boolean> published = new ArrayList<>();
        final List<Throwable> errors = new ArrayList<>();

        RecordingTracingHelper(CpiKafkaPlusEndpoint endpoint) {
            super(endpoint);
        }

        @Override
        public void publishConnectionStatus(boolean success, Throwable error) {
            published.add(success);
            errors.add(error);
        }
    }
}
