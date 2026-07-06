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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Realistic load integration test that produces 2,000 messages across 3 Kafka
 * partitions and verifies 9 correctness invariants using the drain loop.
 */
public class RealisticLoadIT {

    private static final int TOTAL_MESSAGES = 2000;
    private static final int MAX_POLL_RECORDS = 200;
    private static final int BATCH_SIZE = 50;
    private static final int MIN_BACKLOG = 10;
    private static final int PARTITIONS = 3;
    private static final double MAIN_DRAIN_THROUGHPUT_FLOOR_MSG_PER_SEC = 100.0d;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static DefaultCamelContext ctx;
    private static String mainTopic;
    private static List<Exchange> captured;
    private static PollResult mainDrainResult;
    private static double mainDrainThroughputMsgPerSec;

    @BeforeClass
    public static void setUp() throws Exception {
        KafkaTestInfrastructure.requireDockerAvailable();
        KafkaTestInfrastructure.startKafka();

        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();

        mainTopic = "it-realistic-load-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(mainTopic, PARTITIONS);

        produceMessages(mainTopic, TOTAL_MESSAGES);

        captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(mainTopic,
                "grp-realistic-" + System.nanoTime(), buildParams(), captured);
        try {
            mainDrainResult = startAndPoll(consumer);
            mainDrainThroughputMsgPerSec =
                    messagesPerSecond(mainDrainResult.recordsProcessed, mainDrainResult.elapsedNanos);
            System.out.println("[S10] RealisticLoadIT main drain throughput: "
                    + formatThroughput(mainDrainThroughputMsgPerSec)
                    + " msg/s (" + mainDrainResult.recordsProcessed + " records in "
                    + mainDrainResult.elapsedNanos + " ns)");
        } finally {
            consumer.doStop();
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (ctx != null) {
            ctx.stop();
        }
    }

    // -----------------------------------------------------------------------
    //  Test 1: No Record Loss
    // -----------------------------------------------------------------------

    @Test
    public void testNoRecordLoss() throws Exception {
        int totalRecords = 0;
        for (Exchange ex : captured) {
            Integer count = ex.getIn().getHeader("CpiKafkaPlusRecordCount", Integer.class);
            Assert.assertNotNull("Each exchange must have CpiKafkaPlusRecordCount header", count);
            totalRecords += count;
        }
        Assert.assertEquals("Sum of CpiKafkaPlusRecordCount headers must equal " + TOTAL_MESSAGES,
                TOTAL_MESSAGES, totalRecords);
    }

    // -----------------------------------------------------------------------
    //  Test 2: No Duplicates
    // -----------------------------------------------------------------------

    @Test
    public void testNoDuplicates() throws Exception {
        Set<String> allKeys = new HashSet<>();
        for (Exchange ex : captured) {
            String body = ex.getIn().getBody(String.class);
            JsonNode array = MAPPER.readTree(body).path("kafkaRecords").path("record");
            Assert.assertTrue("Body must contain kafkaRecords.record array", array.isArray());
            for (JsonNode record : array) {
                String key = record.get("key").asText();
                allKeys.add(key);
            }
        }
        Assert.assertEquals("Unique keys must equal " + TOTAL_MESSAGES,
                TOTAL_MESSAGES, allKeys.size());
    }

    // -----------------------------------------------------------------------
    //  Test 3: Correct Batching
    // -----------------------------------------------------------------------

    @Test
    public void testCorrectBatching() throws Exception {
        for (int i = 0; i < captured.size(); i++) {
            Exchange ex = captured.get(i);
            Integer count = ex.getIn().getHeader("CpiKafkaPlusRecordCount", Integer.class);
            Assert.assertNotNull("Exchange " + i + " must have CpiKafkaPlusRecordCount", count);
            Assert.assertTrue("Exchange " + i + " record count must be > 0", count > 0);
            Assert.assertTrue("Exchange " + i + " record count (" + count + ") must be <= " + BATCH_SIZE,
                    count <= BATCH_SIZE);
        }
    }

    // -----------------------------------------------------------------------
    //  Test 4: Drain Executes Multiple Cycles
    // -----------------------------------------------------------------------

    @Test
    public void testDrainExecutesMultipleCycles() throws Exception {
        // A single poll returns at most maxPollRecords (200) across 3 partitions.
        // With batchSize=50, a single poll could produce at most ceil(200/50) * 3 = 12 exchanges
        // (worst case: records split evenly across 3 partitions, each needing ceil(66/50)=2 batches = 6,
        //  but the real max from a single poll is around 12 exchanges).
        // With 2000 messages total, we need multiple drain cycles, so exchange count must be > 12.
        int maxExchangesFromSinglePoll = (MAX_POLL_RECORDS / BATCH_SIZE + 1) * PARTITIONS;
        Assert.assertTrue(
                "Exchange count (" + captured.size() + ") must exceed what a single poll could produce ("
                        + maxExchangesFromSinglePoll + "), proving drain ran multiple cycles",
                captured.size() > maxExchangesFromSinglePoll);
    }

    // -----------------------------------------------------------------------
    //  Test 5: MinBacklog Stops Drain — Offsets Committed
    // -----------------------------------------------------------------------

    @Test
    public void testMinBacklogStopsDrainWithCommit() throws Exception {
        String topic = "it-minbacklog-commit-" + System.nanoTime();
        String groupId = "grp-minbacklog-commit-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        produceMessages(topic, 100);

        Map<String, String> params = buildParams();
        params.put("minBacklogToDrain", "10");

        List<Exchange> localCaptured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, groupId, params, localCaptured);
        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        // Verify offsets were committed: a second consumer with the same group should get 0 records
        List<Exchange> secondCaptured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer2 = createConsumer(topic, groupId, params, secondCaptured);
        try {
            startAndPoll(consumer2);
            Assert.assertEquals("Second consumer with same group should get 0 records after minBacklog drain",
                    0, secondCaptured.size());
        } finally {
            consumer2.doStop();
        }
    }

    // -----------------------------------------------------------------------
    //  Test 6: Offset Commit Without MinBacklog
    // -----------------------------------------------------------------------

    @Test
    public void testOffsetCommitWithoutMinBacklog() throws Exception {
        String topic = "it-no-minbacklog-commit-" + System.nanoTime();
        String groupId = "grp-no-minbacklog-commit-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        produceMessages(topic, 100);

        Map<String, String> params = buildParams();
        params.put("minBacklogToDrain", "0");

        List<Exchange> localCaptured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, groupId, params, localCaptured);
        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        // Verify offsets were committed: second consumer with same group gets 0 records
        List<Exchange> secondCaptured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer2 = createConsumer(topic, groupId, params, secondCaptured);
        try {
            startAndPoll(consumer2);
            Assert.assertEquals("Second consumer with same group should get 0 records after full drain",
                    0, secondCaptured.size());
        } finally {
            consumer2.doStop();
        }
    }

    // -----------------------------------------------------------------------
    //  Test 7: Partition Ordering
    // -----------------------------------------------------------------------

    @Test
    public void testPartitionOrdering() throws Exception {
        // Track last seen offset per partition across all exchanges
        Map<Integer, Long> lastOffsetByPartition = new HashMap<>();

        for (Exchange ex : captured) {
            String body = ex.getIn().getBody(String.class);
            JsonNode array = MAPPER.readTree(body).path("kafkaRecords").path("record");
            Assert.assertTrue("Body must contain kafkaRecords.record array", array.isArray());

            for (JsonNode record : array) {
                int partition = record.get("partition").asInt();
                long offset = record.get("offset").asLong();

                Long lastOffset = lastOffsetByPartition.get(partition);
                if (lastOffset != null) {
                    Assert.assertTrue(
                            "Offsets must be monotonically increasing within partition " + partition
                                    + ": last=" + lastOffset + " current=" + offset,
                            offset > lastOffset);
                }
                lastOffsetByPartition.put(partition, offset);
            }
        }

        // Verify we actually saw records from multiple partitions
        Assert.assertTrue("Should have records from at least 2 partitions",
                lastOffsetByPartition.size() >= 2);
    }

    // -----------------------------------------------------------------------
    //  Test 8: Payload Integrity
    // -----------------------------------------------------------------------

    @Test
    public void testPayloadIntegrity() throws Exception {
        Assert.assertFalse("Must have at least one captured exchange", captured.isEmpty());

        // Verify first batch
        String firstBody = captured.get(0).getIn().getBody(String.class);
        JsonNode firstArray = MAPPER.readTree(firstBody).path("kafkaRecords").path("record");
        Assert.assertTrue("First body must contain kafkaRecords.record array", firstArray.isArray());
        Assert.assertTrue("First array must not be empty", firstArray.size() > 0);

        JsonNode firstRecord = firstArray.get(0);
        Assert.assertTrue("Record must have 'key' field", firstRecord.has("key"));
        Assert.assertTrue("Record must have 'value' field", firstRecord.has("value"));
        Assert.assertTrue("Record must have 'topic' field", firstRecord.has("topic"));
        Assert.assertTrue("Record must have 'partition' field", firstRecord.has("partition"));
        Assert.assertTrue("Record must have 'offset' field", firstRecord.has("offset"));
        Assert.assertTrue("Record must have 'timestamp' field", firstRecord.has("timestamp"));

        // Verify the value structure
        JsonNode firstValue = firstRecord.get("value");
        Assert.assertTrue("Value must be an object", firstValue.isObject());
        Assert.assertTrue("Value must have 'index' field", firstValue.has("index"));
        Assert.assertTrue("Value must have 'id' field", firstValue.has("id"));
        Assert.assertTrue("Value must have 'data' field", firstValue.has("data"));

        // Verify last batch
        String lastBody = captured.get(captured.size() - 1).getIn().getBody(String.class);
        JsonNode lastArray = MAPPER.readTree(lastBody).path("kafkaRecords").path("record");
        Assert.assertTrue("Last body must contain kafkaRecords.record array", lastArray.isArray());
        Assert.assertTrue("Last array must not be empty", lastArray.size() > 0);

        JsonNode lastRecord = lastArray.get(lastArray.size() - 1);
        Assert.assertTrue("Last record must have 'value' field", lastRecord.has("value"));
        JsonNode lastValue = lastRecord.get("value");
        Assert.assertTrue("Last value must have 'index' field", lastValue.has("index"));
    }

    // -----------------------------------------------------------------------
    //  Test 9: Partial Failure Mid-Drain with DLQ
    // -----------------------------------------------------------------------

    @Test
    public void testPartialFailureMidDrain() throws Exception {
        String topic = "it-partial-fail-" + System.nanoTime();
        String dlqTopic = "it-partial-fail-dlq-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        produceMessages(topic, 200);

        Map<String, String> params = buildParams();
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", "0");
        // Single partition to make batch counting deterministic
        params.put("maxPollRecords", "200");
        params.put("batchSize", "50");

        // Track successful batch exchanges and a "poison" flag
        final List<Exchange> successCaptured = new ArrayList<>();
        // poisoned becomes true after 2 successful batches; all subsequent calls throw
        final AtomicInteger successBatchCount = new AtomicInteger(0);

        String uri = KafkaTestInfrastructure.buildEndpointUri(topic,
                "grp-partial-fail-" + System.nanoTime(), params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);

        // Processor that succeeds for the first 2 batches, then always throws.
        // When batch #3 fails, the consumer falls back to individual record processing
        // with DLQ. Each individual retry also throws (poison flag is set), so
        // those records get routed to DLQ after dlqMaxRetries=0 attempts.
        Processor failingProcessor = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                // Batch exchanges have CpiKafkaPlusRecordCount header; individual retries do not
                Integer recordCount = exchange.getIn().getHeader("CpiKafkaPlusRecordCount", Integer.class);
                boolean isBatchExchange = (recordCount != null);

                if (isBatchExchange && successBatchCount.get() < 2) {
                    // Allow first 2 batch exchanges through
                    successBatchCount.incrementAndGet();
                    successCaptured.add(exchange);
                    return;
                }

                // After 2 successful batches, always throw (for both batch and individual calls)
                throw new RuntimeException("Simulated failure after 2 successful batches");
            }
        };

        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, failingProcessor);
        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        // With DLQ enabled and batch failure on batch #3+, the consumer falls back to
        // individual record processing. Each individual retry also throws, so records
        // are routed to DLQ (dlqMaxRetries=0 means no retries, direct to DLQ).
        // Batches 3 and 4 each have 50 records = 100 records expected in DLQ.
        List<ConsumerRecord<String, String>> dlqRecords =
                KafkaTestInfrastructure.consumeAllMessages(dlqTopic, 50, 15000);
        Assert.assertTrue("DLQ topic should have received records from the failed batches",
                dlqRecords.size() > 0);

        // Verify that the first 2 batches were successfully processed
        Assert.assertEquals("Should have 2 successful batch exchanges",
                2, successCaptured.size());
        int successRecords = 0;
        for (Exchange ex : successCaptured) {
            Integer count = ex.getIn().getHeader("CpiKafkaPlusRecordCount", Integer.class);
            Assert.assertNotNull("Successful exchange must have CpiKafkaPlusRecordCount", count);
            successRecords += count;
        }
        Assert.assertEquals("First 2 successful batches should have 100 records",
                100, successRecords);

        // Total: successful records + DLQ records should account for all 200 messages
        int totalAccountedFor = successRecords + dlqRecords.size();
        Assert.assertEquals("Successful + DLQ records must account for all 200 messages",
                200, totalAccountedFor);
    }

    // -----------------------------------------------------------------------
    //  Test 10: Main Drain Throughput Regression Guard
    // -----------------------------------------------------------------------

    @Test
    public void testMainDrainThroughputRegressionGuard() throws Exception {
        // S10 calibration (2026-07-02, Docker Desktop): measured 838.8 msg/s for
        // the 2,000-message main drain. The 100 msg/s floor is ~8.4x lower, leaving
        // wide CI variance headroom while catching catastrophic drain slowdowns.
        Assert.assertTrue(
                "Main drain throughput (" + formatThroughput(mainDrainThroughputMsgPerSec)
                        + " msg/s) must stay above conservative floor "
                        + formatThroughput(MAIN_DRAIN_THROUGHPUT_FLOOR_MSG_PER_SEC) + " msg/s",
                mainDrainThroughputMsgPerSec >= MAIN_DRAIN_THROUGHPUT_FLOOR_MSG_PER_SEC);
    }

    // -----------------------------------------------------------------------
    //  Helper Methods
    // -----------------------------------------------------------------------

    private static Map<String, String> buildParams() {
        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "true");
        params.put("batchOutputFormat", "JSON_ARRAY");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("drainEnabled", "true");
        params.put("maxPollRecords", String.valueOf(MAX_POLL_RECORDS));
        params.put("batchSize", String.valueOf(BATCH_SIZE));
        params.put("minBacklogToDrain", String.valueOf(MIN_BACKLOG));
        params.put("batchTimeout", "2000");
        return params;
    }

    private static CpiKafkaPlusConsumer createConsumer(String topic, String groupId,
                                                        Map<String, String> params,
                                                        final List<Exchange> captured) throws Exception {
        String uri = KafkaTestInfrastructure.buildEndpointUri(topic, groupId, params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);

        Processor capturingProcessor = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                captured.add(exchange);
            }
        };

        return new CpiKafkaPlusConsumer(endpoint, capturingProcessor);
    }

    private static void produceMessages(String topic, int count) {
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            keys.add("msg-" + i);
            values.add("{\"index\":" + i + ",\"id\":\"uuid-" + i + "\",\"data\":\"test-" + i + "\"}");
        }
        KafkaTestInfrastructure.produceStringMessagesAsync(topic, keys, values);
    }

    private static PollResult startAndPoll(CpiKafkaPlusConsumer consumer) throws Exception {
        consumer.doStart();

        Method pollMethod = CpiKafkaPlusConsumer.class.getDeclaredMethod("poll");
        pollMethod.setAccessible(true);

        // Poll multiple times to allow partition assignment, then let drain do its work
        PollResult lastResult = new PollResult(0, 0L);
        for (int i = 0; i < 15; i++) {
            long startedNanos = System.nanoTime();
            int polled = (Integer) pollMethod.invoke(consumer);
            long elapsedNanos = System.nanoTime() - startedNanos;
            lastResult = new PollResult(polled, elapsedNanos);
            if (polled > 0) {
                return lastResult;
            }
            Thread.sleep(500);
        }
        return lastResult;
    }

    private static double messagesPerSecond(int messages, long elapsedNanos) {
        if (messages <= 0 || elapsedNanos <= 0L) {
            return 0.0d;
        }
        return messages * 1000000000.0d / elapsedNanos;
    }

    private static String formatThroughput(double throughput) {
        return String.format(Locale.ROOT, "%.1f", throughput);
    }

    private static final class PollResult {
        private final int recordsProcessed;
        private final long elapsedNanos;

        private PollResult(int recordsProcessed, long elapsedNanos) {
            this.recordsProcessed = recordsProcessed;
            this.elapsedNanos = elapsedNanos;
        }
    }
}
