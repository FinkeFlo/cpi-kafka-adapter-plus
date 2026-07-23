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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for transactional batching (ADR 0001) with a real Kafka broker.
 * Covers the successful commit path, slot reuse across sequential sends (regression test for the
 * txn-slot-leak / hanging-close bug), and that a failure in the transactional path does not affect
 * the completely independent shared (non-transactional) producer.
 */
public class ProducerTransactionalBatchIT {

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
    public void testTransactionalBatchCommitIsVisibleWithReadCommitted() throws Exception {
        String topic = "it-txn-commit-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        params.put("enableTransactions", "true");
        params.put("transactionalIdPrefix", "it-txn-commit");
        params.put("maxConcurrentTransactions", "2");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        try {
            producer.doStart();

            String json = "[{\"key\": \"k1\", \"value\": \"msg1\"},"
                    + "{\"key\": \"k2\", \"value\": \"msg2\"}]";

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody(json);
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records = consumeAllMessagesReadCommitted(topic, 2, 15000);
        Assert.assertEquals("Both records of the committed transaction must be visible", 2, records.size());
    }

    /**
     * Regression test for the txnProducer.close() hang / permanent slot leak: with only a single
     * slot available, several sequential transactional batches must all succeed — proving the slot
     * is released (and the per-batch producer closed) after every single call, not just the first.
     */
    @Test
    public void testTransactionalSlotIsReusedAcrossSequentialSends() throws Exception {
        String topic = "it-txn-slot-reuse-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        params.put("enableTransactions", "true");
        params.put("transactionalIdPrefix", "it-txn-slot-reuse");
        params.put("maxConcurrentTransactions", "1");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        int batches = 5;
        try {
            producer.doStart();
            for (int i = 0; i < batches; i++) {
                String json = "[{\"key\": \"k" + i + "\", \"value\": \"msg" + i + "\"}]";
                Exchange exchange = new DefaultExchange(ctx);
                exchange.getIn().setBody(json);
                producer.process(exchange);
            }
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                consumeAllMessagesReadCommitted(topic, batches, 20000);
        Assert.assertEquals("All " + batches + " sequential single-slot transactions must succeed",
                batches, records.size());
    }

    /**
     * A failed/aborted transactional batch must not leave the producer in a broken state: the
     * slot must be released and the per-batch producer closed (bounded by
     * {@code TXN_PRODUCER_CLOSE_TIMEOUT}) so that subsequent transactional batches keep working.
     * Also verifies that none of the aborted transaction's records become visible.
     */
    @Test
    public void testAbortedTransactionDoesNotCommitAndSlotRemainsUsable() throws Exception {
        String topic = "it-txn-abort-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        params.put("enableTransactions", "true");
        params.put("transactionalIdPrefix", "it-txn-abort");
        params.put("maxConcurrentTransactions", "1");
        // Short timeouts so the forced mid-transaction failure (unknown partition) surfaces quickly.
        params.put("deliveryTimeoutSeconds", "2");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        try {
            producer.doStart();

            // Force a failure inside the transaction: topic has only 1 partition, but we target
            // partition 99, which will never resolve -> sendBatch throws -> abortTransaction().
            Exchange failingExchange = new DefaultExchange(ctx);
            failingExchange.getIn().setHeader("kafka.PARTITION_KEY", "99");
            failingExchange.getIn().setBody("[{\"key\": \"k-aborted\", \"value\": \"should-not-commit\"}]");
            try {
                producer.process(failingExchange);
                Assert.fail("Expected the batch targeting a non-existent partition to fail");
            } catch (Exception expected) {
                // expected — transaction must have been aborted
            }

            // The single slot must have been released and the aborted producer closed promptly;
            // a subsequent, valid transactional batch must succeed.
            Exchange okExchange = new DefaultExchange(ctx);
            okExchange.getIn().setBody("[{\"key\": \"k-ok\", \"value\": \"committed-after-abort\"}]");
            producer.process(okExchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records = consumeAllMessagesReadCommitted(topic, 1, 15000);
        Assert.assertEquals("Only the committed record must be visible, not the aborted one",
                1, records.size());
        Assert.assertEquals("committed-after-abort", records.get(0).value());
    }

    // -----------------------------------------------------------------------
    //  Helpers
    // -----------------------------------------------------------------------

    private CpiKafkaPlusProducer createProducer(String topic, Map<String, String> params)
            throws Exception {
        String uri = KafkaTestInfrastructure.buildEndpointUri(topic, "unused-group", params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
        return new CpiKafkaPlusProducer(endpoint);
    }

    /**
     * Like {@link KafkaTestInfrastructure#consumeAllMessages}, but with isolation.level=read_committed
     * so aborted-transaction records are correctly filtered out — required to assert on
     * transactional commit/abort semantics.
     */
    private static List<ConsumerRecord<String, String>> consumeAllMessagesReadCommitted(
            String topic, int expectedCount, long timeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaTestInfrastructure.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-txn-consumer-" + System.nanoTime());
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
}
