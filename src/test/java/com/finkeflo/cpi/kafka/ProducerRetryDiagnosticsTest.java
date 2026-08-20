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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the logging contract of the producer outer retry.
 *
 * <p>The level is the whole point. The SAP CPI tenant trace file receives <b>only</b> ERROR, so a
 * retry reported at WARN or INFO would be invisible in exactly the production incident it exists to
 * explain — the same defect that made a recovering transactional send indistinguishable from a
 * continuing failure before 1.2.7. A test is used rather than a comment because a comment does not
 * fail a build.
 *
 * <p>Log output is captured from {@code System.err}, where the test SLF4J binding writes.
 */
public class ProducerRetryDiagnosticsTest {

    @Test
    public void everyRetryAttemptIsReportedAtErrorWithItsCauseChain() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx, 3);
            Exchange exchange = new DefaultExchange(ctx);
            Message in = exchange.getIn();
            in.setBody("payload");

            String logged;
            try (ScriptedKafkaProducer kafkaProducer = new ScriptedKafkaProducer(
                    Integer.MAX_VALUE, new NetworkException("Disconnected from node 3"), null)) {
                setField(producer, "kafkaProducer", kafkaProducer);
                logged = capture(() -> {
                    try {
                        invokeProcessSingle(producer, exchange, in, "orders");
                        Assert.fail("expected the send to fail after all attempts");
                    } catch (RuntimeException expected) {
                        // expected
                    }
                });
                Assert.assertEquals("all three attempts must reach the producer",
                        3, kafkaProducer.sendCalls());
            }

            List<String> attempts = retryLines(logged, "producer.retry.attempt");
            Assert.assertEquals("one line per repeated attempt, not per attempt", 2, attempts.size());
            for (String line : attempts) {
                assertErrorLineWithSingleMarker(line);
                Assert.assertTrue(line, line.contains("classification=RETRIABLE"));
                Assert.assertTrue(line, line.contains("maxAttempts=3"));
                Assert.assertTrue("the cause chain must be in the message text, not only in the "
                        + "discarded throwable argument: " + line,
                        line.contains("NetworkException"));
                Assert.assertTrue(line, line.contains("remainingBudgetMs="));
            }
            Assert.assertTrue(attempts.get(0).contains("attempt=1"));
            Assert.assertTrue(attempts.get(1).contains("attempt=2"));
        }
    }

    @Test
    public void exhaustedRunIsReportedOnceWithAttemptsAndRecoveredFalse() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx, 2);
            Exchange exchange = new DefaultExchange(ctx);
            Message in = exchange.getIn();
            in.setBody("payload");

            String logged;
            try (ScriptedKafkaProducer kafkaProducer = new ScriptedKafkaProducer(
                    Integer.MAX_VALUE, new NetworkException("Disconnected from node 3"), null)) {
                setField(producer, "kafkaProducer", kafkaProducer);
                logged = capture(() -> {
                    try {
                        invokeProcessSingle(producer, exchange, in, "orders");
                        Assert.fail("expected the send to fail after all attempts");
                    } catch (RuntimeException expected) {
                        // expected
                    }
                });
            }

            List<String> exhausted = retryLines(logged, "producer.retry.exhausted");
            Assert.assertEquals("exactly one outcome line per message", 1, exhausted.size());
            String line = exhausted.get(0);
            assertErrorLineWithSingleMarker(line);
            // Same field names as producer.retry.effect, so "how often does the retry rescue a
            // message" is a ratio of two greppable lines rather than a manual correlation.
            Assert.assertTrue(line, line.contains("attempts=2"));
            Assert.assertTrue(line, line.contains("recovered=false"));
            Assert.assertTrue(line, line.contains("stopReason=ATTEMPTS_EXHAUSTED"));
            Assert.assertTrue(line, line.contains("totalElapsedMs="));
            Assert.assertTrue(retryLines(logged, "producer.retry.skipped").isEmpty());
        }
    }

    @Test
    public void recoveryIsReportedWithAttemptsAndRecoveredTrue() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx, 3);
            Exchange exchange = new DefaultExchange(ctx);
            Message in = exchange.getIn();
            in.setBody("payload");

            RecordMetadata metadata = new RecordMetadata(
                    new TopicPartition("orders", 2), 77L, 0, 1234L, 0, 0);
            String logged;
            try (ScriptedKafkaProducer kafkaProducer = new ScriptedKafkaProducer(
                    1, new NetworkException("Disconnected from node 3"),
                    CompletableFuture.completedFuture(metadata))) {
                setField(producer, "kafkaProducer", kafkaProducer);
                logged = capture(() -> invokeProcessSingle(producer, exchange, in, "orders"));
            }

            Assert.assertEquals("OK", in.getHeader("CpiKafkaPlusStatus"));
            Assert.assertEquals("the iFlow must be able to see that a retry was needed",
                    2, in.getHeader("CamelKafkaPlusRetryAttempts"));

            List<String> effect = retryLines(logged, "producer.retry.effect");
            Assert.assertEquals(1, effect.size());
            String line = effect.get(0);
            assertErrorLineWithSingleMarker(line);
            Assert.assertTrue(line, line.contains("attempts=2"));
            Assert.assertTrue(line, line.contains("recovered=true"));
        }
    }

    @Test
    public void anErrorThatCannotBeRetriedIsReportedAsSkippedWithItsReason() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx, 3);
            Exchange exchange = new DefaultExchange(ctx);
            Message in = exchange.getIn();
            in.setBody("payload");

            String logged;
            try (ScriptedKafkaProducer kafkaProducer = new ScriptedKafkaProducer(
                    Integer.MAX_VALUE, new RecordTooLargeException("record too large"), null)) {
                setField(producer, "kafkaProducer", kafkaProducer);
                logged = capture(() -> {
                    try {
                        invokeProcessSingle(producer, exchange, in, "orders");
                        Assert.fail("expected the send to fail");
                    } catch (RuntimeException expected) {
                        // expected
                    }
                });
                Assert.assertEquals("a data error must not be attempted a second time",
                        1, kafkaProducer.sendCalls());
            }

            List<String> skipped = retryLines(logged, "producer.retry.skipped");
            Assert.assertEquals(1, skipped.size());
            String line = skipped.get(0);
            assertErrorLineWithSingleMarker(line);
            Assert.assertTrue(line, line.contains("stopReason=PERMANENT"));
            Assert.assertTrue(line, line.contains("classification=FATAL_DATA_ERROR"));
            Assert.assertTrue(line, line.contains("recovered=false"));
        }
    }

    @Test
    public void nothingIsWrittenWhenTheFeatureIsOff() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            // Default configuration: producerRetryMaxAttempts=1. Every ordinary failure would
            // otherwise gain a second, contentless line explaining that no retry happened.
            CpiKafkaPlusProducer producer = newProducer(ctx, 1);
            Exchange exchange = new DefaultExchange(ctx);
            Message in = exchange.getIn();
            in.setBody("payload");

            String logged;
            try (ScriptedKafkaProducer kafkaProducer = new ScriptedKafkaProducer(
                    Integer.MAX_VALUE, new NetworkException("Disconnected from node 3"), null)) {
                setField(producer, "kafkaProducer", kafkaProducer);
                logged = capture(() -> {
                    try {
                        invokeProcessSingle(producer, exchange, in, "orders");
                        Assert.fail("expected the send to fail");
                    } catch (RuntimeException expected) {
                        // expected
                    }
                });
                Assert.assertEquals(1, kafkaProducer.sendCalls());
            }

            Assert.assertTrue("no retry event may be written while the feature is off:\n" + logged,
                    retryLines(logged, "producer.retry.").isEmpty());
        }
    }

    @Test
    public void retryHistoryAppearsInTheMplStatusText() throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setTopic("orders");
        AdapterTracingHelper helper = new AdapterTracingHelper(endpoint);

        Map<String, String> context = new LinkedHashMap<>();
        context.put("retryAttempts", "3");
        context.put("stopReason", "ATTEMPTS_EXHAUSTED");

        String status = buildStatusMessage(helper, "KAFKA_SEND_FAILED",
                new NetworkException("Disconnected from node 3"), context);
        Assert.assertTrue(status, status.contains("after 3 retry attempts"));
        Assert.assertTrue(status, status.contains("stopReason=ATTEMPTS_EXHAUSTED"));

        Map<String, String> single = new LinkedHashMap<>();
        single.put("retryAttempts", "1");
        String unretried = buildStatusMessage(helper, "KAFKA_SEND_FAILED",
                new NetworkException("Disconnected from node 3"), single);
        Assert.assertFalse("a message that was tried once must not claim a retry history: " + unretried,
                unretried.contains("retry attempts"));
    }

    @Test
    public void noRetryEventIsGatedBehindFullDiagnostics() throws Exception {
        // FULL only unlocks the bounded thread dump; failure information belongs in STANDARD.
        String source = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(
                "src/main/java/com/finkeflo/cpi/kafka/CpiKafkaPlusProducer.java")),
                java.nio.charset.StandardCharsets.UTF_8);
        int retryEventIndex = source.indexOf("producer.retry.");
        Assert.assertTrue("retry events must exist in the producer", retryEventIndex > 0);
        for (String method : new String[] {"logRetryAttempt", "logRetryOutcome", "reportRetryRecovery",
                                           "logSingleRetryAttempt"}) {
            String body = methodBody(source, method);
            Assert.assertFalse(method + " must not depend on diagnosticsLevel",
                    body.contains("DiagnosticsLevel") || body.contains("getDiagnosticsLevel"));
        }
    }

    // --- helpers -----------------------------------------------------------------------------

    private static String methodBody(String source, String methodName) {
        int start = source.indexOf(" " + methodName + "(");
        Assert.assertTrue("method " + methodName + " not found", start > 0);
        int end = source.indexOf("\n    }", start);
        Assert.assertTrue("end of " + methodName + " not found", end > start);
        return source.substring(start, end);
    }

    private static void assertErrorLineWithSingleMarker(String line) {
        Assert.assertTrue("retry events must be ERROR — WARN and INFO never reach the tenant "
                + "trace file: " + line, line.contains("ERROR"));
        int first = line.indexOf(AdapterDiagnostics.MARKER);
        Assert.assertTrue(line, first >= 0);
        Assert.assertEquals("exactly one marker per line, so one grep retrieves a whole incident",
                -1, line.indexOf(AdapterDiagnostics.MARKER, first + 1));
    }

    private static List<String> retryLines(String logged, String event) {
        return logged.lines()
                .filter(l -> l.contains(AdapterDiagnostics.MARKER + " " + event))
                .collect(Collectors.toList());
    }

    private static String buildStatusMessage(AdapterTracingHelper helper, String errorCode,
                                             Exception e, Map<String, String> context) throws Exception {
        Method m = AdapterTracingHelper.class.getDeclaredMethod("buildStatusMessage",
                String.class, Exception.class, Map.class);
        m.setAccessible(true);
        return (String) m.invoke(helper, errorCode, e, context);
    }

    private static CpiKafkaPlusProducer newProducer(DefaultCamelContext ctx, int maxAttempts)
            throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(ctx);
        endpoint.setBootstrapServers("localhost:9092");
        endpoint.setTopic("orders");
        endpoint.setProducerBatchMode("NONE");
        endpoint.setProducerRetryMaxAttempts(maxAttempts);
        endpoint.setProducerRetryDelaySeconds(1);

        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        setField(producer, "tracingHelper", new AdapterTracingHelper(endpoint));
        // Keep these logging-focused tests free of producer rebuild side effects.
        setField(producer, "lastRebuildAttemptMs", System.currentTimeMillis());
        return producer;
    }

    private static void invokeProcessSingle(CpiKafkaPlusProducer producer, Exchange exchange,
                                            Message in, String topic) throws Exception {
        Method m = CpiKafkaPlusProducer.class.getDeclaredMethod(
                "processSingle", Exchange.class, Message.class, String.class);
        m.setAccessible(true);
        try {
            m.invoke(producer, exchange, in, topic);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static String capture(ThrowingRunnable action) throws Exception {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try {
            System.setErr(new PrintStream(captured, true, "UTF-8"));
            action.run();
        } finally {
            System.setErr(original);
        }
        return captured.toString("UTF-8");
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Fails the first {@code failures} sends with {@code failure}, then returns {@code success}. */
    private static final class ScriptedKafkaProducer extends KafkaProducer<byte[], byte[]> {
        private final int failures;
        private final RuntimeException failure;
        private final Future<RecordMetadata> success;
        private final AtomicInteger sendCalls = new AtomicInteger();

        ScriptedKafkaProducer(int failures, RuntimeException failure, Future<RecordMetadata> success) {
            super(testProducerProps(), new ByteArraySerializer(), new ByteArraySerializer());
            this.failures = failures;
            this.failure = failure;
            this.success = success;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            if (sendCalls.getAndIncrement() < failures) {
                throw failure;
            }
            return success;
        }

        int sendCalls() {
            return sendCalls.get();
        }
    }

    private static Properties testProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-retry-diagnostics-test");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        return props;
    }
}
