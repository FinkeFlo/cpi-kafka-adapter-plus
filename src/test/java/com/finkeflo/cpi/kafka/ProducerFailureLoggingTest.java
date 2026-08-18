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
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * Regression guard for the defect that made this adapter silent in production.
 *
 * <p>Send failures used to be logged only when the cause matched a three-entry "fatal" allow-list,
 * or after {@code MAX_CONSECUTIVE_SEND_FAILURES} <b>consecutive</b> failures. A failure that matched
 * neither — an unclassified runtime exception, with successful sends in between resetting the
 * counter — produced no log line whatsoever. These tests assert the property that fixes it: every
 * send failure yields exactly one ERROR line carrying its stack trace, from the very first
 * occurrence, regardless of classification.
 *
 * <p>Log output is captured from {@code System.err}, which is where the test SLF4J binding writes.
 */
public class ProducerFailureLoggingTest {

    private static CpiKafkaPlusProducer newProducer(DefaultCamelContext ctx) throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(ctx);
        endpoint.setBootstrapServers("broker.example.com:9092");
        endpoint.setTopic("test-topic");

        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        // Normally assigned in doStart(); injected here so the failure handlers can be exercised
        // without a broker.
        Field tracing = CpiKafkaPlusProducer.class.getDeclaredField("tracingHelper");
        tracing.setAccessible(true);
        tracing.set(producer, new AdapterTracingHelper(endpoint));
        return producer;
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

    private static Map<String, String> context() {
        Map<String, String> ctx = new LinkedHashMap<>();
        ctx.put("topic", "test-topic");
        ctx.put("batchMode", "JSON");
        ctx.put("recordCount", "3");
        return ctx;
    }

    private static long countErrorLines(String logged) {
        return logged.lines().filter(l -> l.contains("ERROR") && l.contains(AdapterDiagnostics.MARKER)).count();
    }

    @Test
    public void logsTheVeryFirstUnclassifiedFailure() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx);

            // Neither on the fatal allow-list nor a repeat: exactly the case that used to be silent.
            Exception unclassified = new IllegalStateException("something the allow-list never heard of");
            String logged = capture(() ->
                    producer.handleSendFailure(unclassified, "producer.batch.send", context()));

            Assert.assertEquals("exactly one ERROR line expected, got:\n" + logged,
                    1, countErrorLines(logged));
            Assert.assertTrue(logged, logged.contains("consecutiveFailures=1"));
            Assert.assertTrue(logged, logged.contains("fatalClassification=false"));
            Assert.assertTrue(logged, logged.contains("reconnectTriggered=false"));
            Assert.assertTrue(logged, logged.contains("producerPath=SHARED"));
            Assert.assertTrue(logged, logged.contains("topic=test-topic"));
            Assert.assertTrue(logged, logged.contains("java.lang.IllegalStateException"));
        }
    }

    @Test
    public void logsTheProductionIncidentSignatureOnFirstOccurrence() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx);

            IllegalMonitorStateException imse = new IllegalMonitorStateException("current thread is not owner");
            RuntimeException wrapper = new RuntimeException(
                    "Batch send failed at record index 0: " + imse.getMessage(), imse);

            String logged = capture(() ->
                    producer.handleSendFailure(wrapper, "producer.batch.send", context()));

            Assert.assertEquals("exactly one ERROR line expected, got:\n" + logged,
                    1, countErrorLines(logged));
            Assert.assertTrue(logged, logged.contains("current thread is not owner"));
            Assert.assertTrue(logged, logged.contains("java.lang.IllegalMonitorStateException"));
            // The cause chain must be in the message text, not only in the discarded Throwable.
            Assert.assertTrue(logged, logged.contains("CAUSED_BY"));
        }
    }

    @Test
    public void logsEveryFailureEvenWhenSuccessesResetTheCounter() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx);

            // The production pattern: interleaved successes kept the counter below the threshold
            // forever, so the old threshold-gated logging never fired.
            String logged = capture(() -> {
                for (int i = 0; i < 5; i++) {
                    producer.handleSendFailure(new IllegalStateException("failure " + i),
                            "producer.batch.send", context());
                    producer.recordSendSuccess();
                }
            });

            Assert.assertEquals("one ERROR line per failure expected, got:\n" + logged,
                    5, countErrorLines(logged));
            // Every one of them stayed below the reconnect threshold, which is the point.
            Assert.assertEquals(5, logged.lines().filter(l -> l.contains("reconnectTriggered=false")).count());
        }
    }

    @Test
    public void reconnectsOnlyAfterConsecutiveFailures() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx);

            String logged = capture(() -> {
                for (int i = 0; i < 3; i++) {
                    producer.handleSendFailure(new IllegalStateException("failure " + i),
                            "producer.batch.send", context());
                }
            });

            Assert.assertEquals(3, countErrorLines(logged));
            Assert.assertEquals("reconnect must trigger once, on the third consecutive failure",
                    1, logged.lines().filter(l -> l.contains("reconnectTriggered=true")).count());
        }
    }

    @Test
    public void reconnectsImmediatelyOnAFatalException() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx);

            Exception fatal = new org.apache.kafka.common.errors.AuthenticationException("bad credentials");
            String logged = capture(() ->
                    producer.handleSendFailure(fatal, "producer.batch.send", context()));

            Assert.assertEquals(1, countErrorLines(logged));
            Assert.assertTrue(logged, logged.contains("fatalClassification=true"));
            Assert.assertTrue(logged, logged.contains("reconnectTriggered=true"));
        }
    }

    @Test
    public void transactionalFailureCarriesTheThrowableNotJustItsClassName() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx);

            Exception cause = new IllegalStateException("producer fenced by a newer instance");
            Map<String, String> txnCtx = new LinkedHashMap<>();
            txnCtx.put("topic", "test-topic");
            txnCtx.put("slotId", "slot-0");

            String logged = capture(() -> producer.handleTxnSendFailure(cause, txnCtx));

            Assert.assertEquals(1, countErrorLines(logged));
            Assert.assertTrue(logged, logged.contains("producerPath=TRANSACTIONAL"));
            Assert.assertTrue(logged, logged.contains("slotId=slot-0"));
            // The old implementation logged only getClass().getSimpleName() and lost all of this.
            Assert.assertTrue(logged, logged.contains("producer fenced by a newer instance"));
            Assert.assertTrue(logged, logged.contains(ProducerFailureLoggingTest.class.getName()));
        }
    }

    @Test
    public void keepsTopicMetadataCachedFarBeyondTheClientDefault() {
        // The client default forgets the metadata of a topic idle for five minutes, and the next
        // send then has to fetch it on the calling thread inside ProducerMetadata.awaitUpdate() —
        // the method carrying the KAFKA-10902 monitor defect. A flow producing less often than that
        // would enter the vulnerable path on every single message.
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setBootstrapServers("broker.example.com:9092");
        endpoint.setTopic("test-topic");

        java.util.Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);

        Assert.assertEquals(ProducerConfigFactory.METADATA_MAX_IDLE_MS,
                props.get(org.apache.kafka.clients.producer.ProducerConfig.METADATA_MAX_IDLE_CONFIG));
        Assert.assertTrue("must be well above the 5 minute client default",
                ProducerConfigFactory.METADATA_MAX_IDLE_MS > 300_000L);
    }
}
