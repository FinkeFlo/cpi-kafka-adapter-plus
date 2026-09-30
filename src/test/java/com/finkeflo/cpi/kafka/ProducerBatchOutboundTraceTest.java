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

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the outbound MPL trace of the batch producer (issue #166).
 *
 * <p>Before the fix only {@code processSingle} wrote a {@code RECEIVER_OUTBOUND} trace; a batch
 * send in either path left no record of the payload that was handed to Kafka. The contract pinned
 * here: exactly one trace per message, written before the send, carrying the batch body as it was
 * received — not the send summary that replaces the body afterwards, and not one per transactional
 * retry attempt.
 */
public class ProducerBatchOutboundTraceTest {

    private static final String BATCH =
            "[{\"key\": \"k1\", \"value\": \"hello\"}, {\"key\": \"k2\", \"value\": \"world\"}]";

    @After
    public void resetProbe() {
        TlsListenerProbe.clearCacheForTests();
    }

    @Test
    public void nonTransactionalBatchTracesTheReceivedBodyOnce() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = newEndpoint(ctx, false);
            CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
            RecordingTracingHelper tracing = new RecordingTracingHelper(endpoint);
            setField(producer, "tracingHelper", tracing);

            Exchange exchange = new DefaultExchange(ctx);
            Message in = exchange.getIn();
            in.setBody(BATCH);

            try (StubProducer kafkaProducer = new StubProducer(0)) {
                setField(producer, "kafkaProducer", kafkaProducer);
                invokeProcessBatch(producer, exchange, in, "orders", "JSON_ARRAY");
                Assert.assertEquals(2, kafkaProducer.sendCalls.get());
            }

            Assert.assertEquals("exactly one outbound trace per batch", 1, tracing.outbound.size());
            Assert.assertEquals("the trace must carry the batch as received, not the send summary",
                    BATCH, new String(tracing.outbound.get(0), StandardCharsets.UTF_8));
            Assert.assertNotEquals("precondition: the body was replaced by the send summary",
                    BATCH, in.getBody(String.class));
        }
    }

    @Test
    public void streamBodyIsTracedWithTheContentThatWasParsed() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = newEndpoint(ctx, false);
            CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
            RecordingTracingHelper tracing = new RecordingTracingHelper(endpoint);
            setField(producer, "tracingHelper", tracing);

            Exchange exchange = new DefaultExchange(ctx);
            Message in = exchange.getIn();
            // A one-shot stream: a second read of the body would come back empty.
            in.setBody(new ByteArrayInputStream(BATCH.getBytes(StandardCharsets.UTF_8)));

            try (StubProducer kafkaProducer = new StubProducer(0)) {
                setField(producer, "kafkaProducer", kafkaProducer);
                invokeProcessBatch(producer, exchange, in, "orders", "JSON_ARRAY");
                Assert.assertEquals(2, kafkaProducer.sendCalls.get());
            }

            Assert.assertEquals(1, tracing.outbound.size());
            Assert.assertEquals(BATCH, new String(tracing.outbound.get(0), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void retriedTransactionalBatchIsTracedOnlyOnce() throws Exception {
        TlsListenerProbe.setProbeRunnerForTests(address -> TlsListenerProbe.Verdict.INCONCLUSIVE);
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = newEndpoint(ctx, true);
            CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
            RecordingTracingHelper tracing = new RecordingTracingHelper(endpoint);
            setField(producer, "tracingHelper", tracing);
            setField(producer, "txnSlotSemaphore", new java.util.concurrent.Semaphore(1, true));
            setField(producer, "txnSlotInUse", new boolean[1]);
            setField(producer, "topicHash", "0000abcd");
            setField(producer, "resolvedMemberSuffix", "test");

            // The first attempt fails in the SEND phase with a retriable error, the second succeeds.
            AtomicInteger attempts = new AtomicInteger();
            List<StubProducer> created = new ArrayList<>();
            producer.txnProducerFactory = props -> {
                StubProducer p = new StubProducer(attempts.getAndIncrement() == 0 ? 1 : 0);
                created.add(p);
                return p;
            };

            Exchange exchange = new DefaultExchange(ctx);
            Message in = exchange.getIn();
            in.setBody(BATCH);

            invokeProcessBatch(producer, exchange, in, "orders", "JSON_ARRAY");

            Assert.assertEquals("precondition: the transaction was retried", 2, attempts.get());
            Assert.assertTrue("precondition: the second attempt committed", created.get(1).committed);
            Assert.assertEquals("a retried transaction must not trace the payload again",
                    1, tracing.outbound.size());
            Assert.assertEquals(BATCH, new String(tracing.outbound.get(0), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void unparseableBatchIsNotTracedAndFailsAsBefore() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = newEndpoint(ctx, false);
            CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
            RecordingTracingHelper tracing = new RecordingTracingHelper(endpoint);
            setField(producer, "tracingHelper", tracing);

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("not a json array");

            try {
                invokeProcessBatch(producer, exchange, exchange.getIn(), "orders", "JSON_ARRAY");
                Assert.fail("expected the batch parser to reject the body");
            } catch (IllegalArgumentException expected) {
                // expected: the parser's exception, unchanged
            }
            Assert.assertTrue("no 'Sending' trace for a batch that was never sent",
                    tracing.outbound.isEmpty());
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static CpiKafkaPlusEndpoint newEndpoint(DefaultCamelContext ctx, boolean transactional) {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(ctx);
        endpoint.setBootstrapServers("localhost:9092");
        endpoint.setSecurityProtocol("PLAINTEXT");
        endpoint.setTopic("orders");
        endpoint.setProducerBatchMode("JSON_ARRAY");
        endpoint.setEnableTransactions(transactional);
        endpoint.setProducerRetryMaxAttempts(2);
        endpoint.setProducerRetryDelaySeconds(0);
        return endpoint;
    }

    private static void invokeProcessBatch(CpiKafkaPlusProducer producer, Exchange exchange,
                                           Message in, String topic, String batchMode) throws Exception {
        Method m = CpiKafkaPlusProducer.class.getDeclaredMethod(
                "processBatch", Exchange.class, Message.class, String.class, String.class);
        m.setAccessible(true);
        try {
            m.invoke(producer, exchange, in, topic, batchMode);
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

    /** Records outbound traces instead of writing them to an MPL that does not exist off-platform. */
    private static final class RecordingTracingHelper extends AdapterTracingHelper {
        final List<byte[]> outbound = new ArrayList<>();

        RecordingTracingHelper(CpiKafkaPlusEndpoint endpoint) {
            super(endpoint);
        }

        @Override
        public void traceOutbound(Exchange exchange, byte[] body) {
            outbound.add(body);
        }
    }

    /**
     * Acknowledges every send immediately, after failing the first {@code failures} sends with a
     * retriable error. Transaction calls are no-ops, so the same stub serves both producer paths.
     */
    private static final class StubProducer extends KafkaProducer<byte[], byte[]> {
        private final int failures;
        final AtomicInteger sendCalls = new AtomicInteger();
        volatile boolean committed;

        StubProducer(int failures) {
            super(stubProps(), new ByteArraySerializer(), new ByteArraySerializer());
            this.failures = failures;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            if (sendCalls.getAndIncrement() < failures) {
                throw new NetworkException("Disconnected from node 1");
            }
            return CompletableFuture.completedFuture(new RecordMetadata(
                    new TopicPartition(record.topic(), 0), sendCalls.get(), 0,
                    System.currentTimeMillis(), 0, 0));
        }

        @Override
        public void initTransactions() {
        }

        @Override
        public void beginTransaction() {
        }

        @Override
        public void commitTransaction() {
            committed = true;
        }

        @Override
        public void close(Duration timeout) {
            super.close(Duration.ZERO);
        }
    }

    private static Properties stubProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-batch-outbound-trace-test");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        return props;
    }
}
