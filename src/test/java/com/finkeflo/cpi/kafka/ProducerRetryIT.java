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

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for the producer outer retry against a real Kafka broker.
 *
 * <p>Faults are injected through {@code CpiKafkaPlusProducer.txnProducerFactory} rather than by
 * disturbing the broker. Two reasons, both decisive: {@code KafkaTestInfrastructure} shares one
 * static {@code ConfluentKafkaContainer} across every IT class in the JVM, so pausing it would pull
 * the ground from under the tests running in parallel and change the bootstrap servers on restart;
 * and only a seam can produce a failure in a <em>specific</em> transaction phase with a
 * <em>specific</em> exception class, which is the whole point of the rules under test. The broker
 * stays real, so the assertion that matters — {@code read_committed} sees the batch exactly once —
 * is verified against Kafka rather than against a mock.
 */
public class ProducerRetryIT {

    private static DefaultCamelContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        KafkaTestInfrastructure.requireDockerAvailable();
        KafkaTestInfrastructure.startKafka();

        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (ctx != null) {
            ctx.stop();
        }
    }

    @Test
    public void transactionalBatchRecoversOnTheSecondAttemptAndIsVisibleExactlyOnce() throws Exception {
        String topic = "it-retry-recover-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        CpiKafkaPlusProducer producer = createProducer(topic, retryParams(2, 30));
        AtomicInteger attempts = new AtomicInteger();
        // First attempt: the very first send() fails the way the production incident did.
        producer.txnProducerFactory = props -> new FaultInjectingProducer(props,
                attempts.incrementAndGet() == 1 ? Fault.failFirstSend() : Fault.none());

        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("[{\"key\":\"k1\",\"value\":\"msg1\"},{\"key\":\"k2\",\"value\":\"msg2\"}]");
            producer.process(exchange);

            Assert.assertEquals("the retry must have needed exactly two attempts", 2, attempts.get());
            Assert.assertEquals("the iFlow must be able to see that a retry was needed",
                    2, exchange.getIn().getHeader("CamelKafkaPlusRetryAttempts"));
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records = consumeReadCommitted(topic, 2, 20000);
        Assert.assertEquals("read_committed must see the batch exactly once, not twice",
                2, records.size());
    }

    @Test
    public void recordsOfAFailedAttemptAreNeverCommitted() throws Exception {
        String topic = "it-retry-no-duplicates-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        CpiKafkaPlusProducer producer = createProducer(topic, retryParams(2, 30));
        AtomicInteger attempts = new AtomicInteger();
        // First attempt: record 0 really is appended to the open transaction, record 1 then fails.
        // If close() committed instead of aborting — the assumption this test exists to check —
        // record 0 would be visible in addition to the two records of the successful attempt.
        producer.txnProducerFactory = props -> new FaultInjectingProducer(props,
                attempts.incrementAndGet() == 1 ? Fault.failSendNumber(2) : Fault.none());

        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("[{\"key\":\"k1\",\"value\":\"msg1\"},{\"key\":\"k2\",\"value\":\"msg2\"}]");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        // Deliberately asks for three: if the aborted attempt had leaked a record, the extra one
        // would be waiting and the assertion below would catch it.
        List<ConsumerRecord<String, String>> records = consumeReadCommitted(topic, 3, 10000);
        Assert.assertEquals("only the records of the successful attempt may be visible",
                2, records.size());
    }

    @Test
    public void aFailureInsideTheCommitIsNeverRetried() throws Exception {
        String topic = "it-retry-commit-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        CpiKafkaPlusProducer producer = createProducer(topic, retryParams(3, 60));
        AtomicInteger attempts = new AtomicInteger();
        producer.txnProducerFactory = props -> {
            attempts.incrementAndGet();
            return new FaultInjectingProducer(props, Fault.failCommit());
        };

        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("[{\"key\":\"k1\",\"value\":\"msg1\"}]");
            try {
                producer.process(exchange);
                Assert.fail("expected the commit failure to surface");
            } catch (Exception expected) {
                // expected
            }
            // The outcome of a commit is unknowable: it may have succeeded broker-side while only
            // the response was lost, so a second attempt could write the batch twice.
            Assert.assertEquals("a commit-phase failure must not be repeated", 1, attempts.get());
        } finally {
            producer.doStop();
        }
    }

    @Test
    public void theTotalBudgetStopsTheLoopBeforeTheAttemptsAreUsedUp() throws Exception {
        String topic = "it-retry-budget-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = retryParams(5, 300);
        CpiKafkaPlusProducer producer = createProducer(topic, params);
        AtomicInteger attempts = new AtomicInteger();
        // An unreachable broker does not fail instantly, it blocks until a timeout expires. The
        // delay is what makes the budget the binding constraint: without it an attempt costs only
        // the retry delay and all five would fit into the budget, which is precisely the situation
        // the budget is not meant to police.
        producer.txnProducerFactory = props -> {
            attempts.incrementAndGet();
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new NetworkException("injected: broker unreachable");
        };

        try {
            producer.doStart();
            // Tightened after the start-up check, which validates the *configured* worst case: the
            // point here is the runtime guard, i.e. that the loop ends on the clock rather than on
            // the attempt count.
            ((CpiKafkaPlusEndpoint) producer.getEndpoint()).setProducerRetryTotalBudgetSeconds(5);

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("[{\"key\":\"k1\",\"value\":\"msg1\"}]");
            long startedAt = System.currentTimeMillis();
            try {
                producer.process(exchange);
                Assert.fail("expected the send to fail");
            } catch (Exception expected) {
                // expected
            }
            long elapsed = System.currentTimeMillis() - startedAt;

            Assert.assertTrue("the budget must stop the loop before all 5 attempts are used, was "
                    + attempts.get(), attempts.get() < 5);
            Assert.assertTrue("the loop must stay inside the budget, took " + elapsed + "ms",
                    elapsed < 10_000);
        } finally {
            producer.doStop();
        }
    }

    @Test
    public void theTransactionSlotIsFreeAgainAfterAnExhaustedRetry() throws Exception {
        String topic = "it-retry-slot-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = retryParams(2, 30);
        params.put("maxConcurrentTransactions", "1");
        CpiKafkaPlusProducer producer = createProducer(topic, params);
        AtomicInteger attempts = new AtomicInteger();
        // Fail every attempt of the first message, then behave normally.
        producer.txnProducerFactory = props -> new FaultInjectingProducer(props,
                attempts.incrementAndGet() <= 2 ? Fault.failFirstSend() : Fault.none());

        try {
            producer.doStart();

            Exchange failing = new DefaultExchange(ctx);
            failing.getIn().setBody("[{\"key\":\"k-fail\",\"value\":\"never-committed\"}]");
            try {
                producer.process(failing);
                Assert.fail("expected the send to fail after both attempts");
            } catch (Exception expected) {
                // expected
            }

            // The single slot must have been released even though every attempt closed its own
            // producer — the deadlock TXN_PRODUCER_CLOSE_TIMEOUT exists to prevent.
            Exchange ok = new DefaultExchange(ctx);
            ok.getIn().setBody("[{\"key\":\"k-ok\",\"value\":\"committed\"}]");
            producer.process(ok);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records = consumeReadCommitted(topic, 1, 15000);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("committed", records.get(0).value());
    }

    @Test
    public void allAttemptsOfOneMessageCountAsASingleFailure() throws Exception {
        String topic = "it-retry-counter-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        CpiKafkaPlusProducer producer = createProducer(topic, retryParams(3, 60));
        producer.txnProducerFactory = props -> new FaultInjectingProducer(props, Fault.failFirstSend());

        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("[{\"key\":\"k1\",\"value\":\"msg1\"}]");
            try {
                producer.process(exchange);
                Assert.fail("expected the send to fail after all attempts");
            } catch (Exception expected) {
                // expected
            }

            // Three attempts for one message must not look like three failed messages: the counter
            // feeds the auto-pause / node-fault escalation, which would otherwise trip early.
            Field counter = CpiKafkaPlusProducer.class.getDeclaredField("consecutiveTxnSendFailures");
            counter.setAccessible(true);
            Assert.assertEquals("one message must count as one failure regardless of its attempts",
                    1, ((Number) counter.get(producer)).intValue());
        } finally {
            producer.doStop();
        }
    }

    // -----------------------------------------------------------------------
    //  Helpers
    // -----------------------------------------------------------------------

    /**
     * @param budgetSeconds must cover the configured worst case, which on the transactional path is
     *                      roughly {@code attempts x (3 x max.block.ms + deliveryTimeout + close)}
     */
    private static Map<String, String> retryParams(int maxAttempts, int budgetSeconds) {
        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        params.put("enableTransactions", "true");
        params.put("transactionalIdPrefix", "it-retry-" + System.nanoTime());
        params.put("maxConcurrentTransactions", "2");
        params.put("deliveryTimeoutSeconds", "2");
        params.put("producerRetryMaxAttempts", String.valueOf(maxAttempts));
        params.put("producerRetryDelaySeconds", "1");
        params.put("producerRetryTotalBudgetSeconds", String.valueOf(budgetSeconds));
        return params;
    }

    private CpiKafkaPlusProducer createProducer(String topic, Map<String, String> params)
            throws Exception {
        String uri = KafkaTestInfrastructure.buildEndpointUri(topic, "unused-group", params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
        return new CpiKafkaPlusProducer(endpoint);
    }

    private static List<ConsumerRecord<String, String>> consumeReadCommitted(
            String topic, int expectedCount, long timeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaTestInfrastructure.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-retry-consumer-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (result.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    result.add(record);
                }
            }
        }
        return result;
    }

    /** Which call of an attempt fails, and how. */
    private static final class Fault {
        private final int failSendNumber;   // 1-based send call to fail, 0 = none
        private final boolean failCommit;

        private Fault(int failSendNumber, boolean failCommit) {
            this.failSendNumber = failSendNumber;
            this.failCommit = failCommit;
        }

        static Fault none() {
            return new Fault(0, false);
        }

        static Fault failFirstSend() {
            return new Fault(1, false);
        }

        /** Lets the earlier records reach the open transaction before failing. */
        static Fault failSendNumber(int n) {
            return new Fault(n, false);
        }

        static Fault failCommit() {
            return new Fault(0, true);
        }
    }

    private static final class FaultInjectingProducer extends KafkaProducer<byte[], byte[]> {
        private final Fault fault;
        private final AtomicInteger sendCalls = new AtomicInteger();

        FaultInjectingProducer(Properties props, Fault fault) {
            super(props, new ByteArraySerializer(), new ByteArraySerializer());
            this.fault = fault;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            if (sendCalls.incrementAndGet() == fault.failSendNumber) {
                throw new NetworkException("injected: Disconnected from node 3");
            }
            return super.send(record);
        }

        @Override
        public void commitTransaction() {
            if (fault.failCommit) {
                // The exact shape Kafka reports when the acknowledgement of a commit is lost:
                // "CommitTransaction timed out ... within max.block.ms" (KafkaProducer.java:256).
                throw new TimeoutException("injected: CommitTransaction timed out");
            }
            super.commitTransaction();
        }
    }
}
