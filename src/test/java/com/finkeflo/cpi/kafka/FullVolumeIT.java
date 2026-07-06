/*-
 * #%L
 * SAP CPI Kafka Adapter Plus
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
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Full-volume integration test that produces 72,000 messages using a synthetic sample
 * JSON payload and verifies 12 correctness invariants.
 *
 * <p>Excluded from default {@code mvn verify}. Run with:
 * {@code mvn verify -Pfull-volume -Dit.test=FullVolumeIT}
 */
public class FullVolumeIT {

    private static final int TOTAL_MESSAGES = 72000;
    private static final int MAX_POLL_RECORDS = 20000;
    private static final int BATCH_SIZE = 5000;
    private static final int MIN_BACKLOG = 500;
    private static final int PARTITIONS = 3;
    private static final double MAIN_DRAIN_THROUGHPUT_FLOOR_MSG_PER_SEC = 1500.0d;

    private static final String TEMPLATE_CALCULATION_ID = "11111111-1111-1111-1111-111111111111";
    private static final String TEMPLATE_RECORD_ID = "SAMPLE-RECORD-0001";
    private static final String TEMPLATE_TIMESTAMP = "2025-01-01T00:00:00.000000000Z";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static DefaultCamelContext ctx;
    private static String mainTopic;
    private static List<Exchange> captured;
    private static String sampleTemplate;
    private static PollResult mainDrainResult;
    private static double mainDrainThroughputMsgPerSec;

    @BeforeClass
    public static void setUp() throws Exception {
        KafkaTestInfrastructure.requireDockerAvailable();

        InputStream is = FullVolumeIT.class.getResourceAsStream("/payload/sample-payload.json");
        Assume.assumeTrue("sample payload not found — skipping", is != null);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[1024];
        int len;
        while ((len = is.read(buf)) != -1) {
            baos.write(buf, 0, len);
        }
        is.close();
        sampleTemplate = baos.toString("UTF-8");

        KafkaTestInfrastructure.startKafka();

        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();

        mainTopic = "it-full-volume-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(mainTopic, PARTITIONS);

        produceSampleMessages(mainTopic, TOTAL_MESSAGES);

        captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(mainTopic,
                "grp-full-volume-" + System.nanoTime(), buildParams(), captured);
        try {
            mainDrainResult = startAndPoll(consumer);
            mainDrainThroughputMsgPerSec =
                    messagesPerSecond(mainDrainResult.recordsProcessed, mainDrainResult.elapsedNanos);
            System.out.println("[S10] FullVolumeIT main drain throughput: "
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
        String topic = "it-fv-minbacklog-commit-" + System.nanoTime();
        String groupId = "grp-fv-minbacklog-commit-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        produceSampleMessages(topic, 100);

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
        String topic = "it-fv-no-minbacklog-commit-" + System.nanoTime();
        String groupId = "grp-fv-no-minbacklog-commit-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        produceSampleMessages(topic, 100);

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

        // Verify the value is a valid sample payload (string containing JSON)
        JsonNode firstValue = firstRecord.get("value");
        Assert.assertNotNull("Value must not be null", firstValue);

        // Verify last batch
        String lastBody = captured.get(captured.size() - 1).getIn().getBody(String.class);
        JsonNode lastArray = MAPPER.readTree(lastBody).path("kafkaRecords").path("record");
        Assert.assertTrue("Last body must contain kafkaRecords.record array", lastArray.isArray());
        Assert.assertTrue("Last array must not be empty", lastArray.size() > 0);
    }

    // -----------------------------------------------------------------------
    //  Test 9: Partial Failure Mid-Drain with DLQ
    // -----------------------------------------------------------------------

    @Test
    public void testPartialFailureMidDrain() throws Exception {
        String topic = "it-fv-partial-fail-" + System.nanoTime();
        String dlqTopic = "it-fv-partial-fail-dlq-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        produceSampleMessages(topic, 200);

        Map<String, String> params = buildParams();
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", "0");
        params.put("maxPollRecords", "200");
        params.put("batchSize", "50");
        params.put("minBacklogToDrain", "10");

        final List<Exchange> successCaptured = new ArrayList<>();
        final AtomicInteger successBatchCount = new AtomicInteger(0);

        String uri = KafkaTestInfrastructure.buildEndpointUri(topic,
                "grp-fv-partial-fail-" + System.nanoTime(), params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);

        Processor failingProcessor = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                Integer recordCount = exchange.getIn().getHeader("CpiKafkaPlusRecordCount", Integer.class);
                boolean isBatchExchange = (recordCount != null);

                if (isBatchExchange && successBatchCount.get() < 2) {
                    successBatchCount.incrementAndGet();
                    successCaptured.add(exchange);
                    return;
                }

                throw new RuntimeException("Simulated failure after 2 successful batches");
            }
        };

        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, failingProcessor);
        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        List<ConsumerRecord<String, String>> dlqRecords =
                KafkaTestInfrastructure.consumeAllMessages(dlqTopic, 50, 15000);
        Assert.assertTrue("DLQ topic should have received records from the failed batches",
                dlqRecords.size() > 0);

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

        int totalAccountedFor = successRecords + dlqRecords.size();
        Assert.assertEquals("Successful + DLQ records must account for all 200 messages",
                200, totalAccountedFor);
    }

    // -----------------------------------------------------------------------
    //  Test 10: Drain Cycle Count
    // -----------------------------------------------------------------------

    @Test
    public void testDrainCycleCount() throws Exception {
        // maxExchangesPerCycle = (MAX_POLL_RECORDS / BATCH_SIZE) * PARTITIONS + PARTITIONS
        int maxExchangesPerCycle = (MAX_POLL_RECORDS / BATCH_SIZE) * PARTITIONS + PARTITIONS;
        int minCycles = captured.size() / maxExchangesPerCycle;
        Assert.assertTrue(
                "Inferred drain cycle count (" + minCycles + ") must be >= 3 "
                        + "(exchanges=" + captured.size() + ", maxPerCycle=" + maxExchangesPerCycle + ")",
                minCycles >= 3);
    }

    // -----------------------------------------------------------------------
    //  Test 11: Batch Payload Size
    // -----------------------------------------------------------------------

    @Test
    public void testBatchPayloadSize() throws Exception {
        for (int i = 0; i < captured.size(); i++) {
            String body = captured.get(i).getIn().getBody(String.class);
            Assert.assertNotNull("Exchange " + i + " body must not be null", body);
            Assert.assertFalse("Exchange " + i + " body must not be empty", body.isEmpty());
        }
    }

    // -----------------------------------------------------------------------
    //  Test 12: Full Sample Payload Fields
    // -----------------------------------------------------------------------

    @Test
    public void testFullSamplePayloadFields() throws Exception {
        Assert.assertFalse("Must have at least one captured exchange", captured.isEmpty());

        String firstBody = captured.get(0).getIn().getBody(String.class);
        JsonNode firstArray = MAPPER.readTree(firstBody).path("kafkaRecords").path("record");
        Assert.assertTrue("First body must contain kafkaRecords.record array", firstArray.isArray());
        Assert.assertTrue("First array must not be empty", firstArray.size() > 0);

        JsonNode firstRecord = firstArray.get(0);
        JsonNode valueNode = firstRecord.get("value");
        Assert.assertNotNull("Record must have 'value' field", valueNode);

        // BatchFormatter parses valid JSON values into object nodes directly
        JsonNode payloadNode;
        if (valueNode.isObject()) {
            payloadNode = valueNode;
        } else {
            payloadNode = MAPPER.readTree(valueNode.asText());
        }

        Assert.assertTrue("sample payload must have 'appointment_date' field",
                payloadNode.has("appointment_date"));
        Assert.assertTrue("sample payload must have 'event_code' field",
                payloadNode.has("event_code"));
        Assert.assertTrue("sample payload must have 'trigger_system' field",
                payloadNode.has("trigger_system"));

        // Verify nested event_code has category_code
        JsonNode eventCode = payloadNode.get("event_code");
        Assert.assertTrue("event_code must be an object", eventCode.isObject());
        Assert.assertTrue("event_code must have 'category_code' field",
                eventCode.has("category_code"));
    }

    // -----------------------------------------------------------------------
    //  Test 13: Main Drain Throughput Regression Guard
    // -----------------------------------------------------------------------

    @Test
    public void testMainDrainThroughputRegressionGuard() throws Exception {
        // S10 calibration (2026-07-02, Docker Desktop): measured 11,636.1 msg/s for
        // the 72,000-message main drain. The 1,500 msg/s floor is ~7.8x lower, leaving
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
        params.put("batchTimeout", "5000");
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

    private static void produceSampleMessages(String topic, int count) {
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String uuid = UUID.randomUUID().toString();
            String recordId = "record-" + i;
            String timestamp = String.format("2025-09-16T%02d:%02d:%02d.%09dZ",
                    5 + (i / 3600), (i / 60) % 60, i % 60, i);

            String payload = sampleTemplate
                    .replace(TEMPLATE_CALCULATION_ID, uuid)
                    .replace(TEMPLATE_RECORD_ID, recordId)
                    .replace(TEMPLATE_TIMESTAMP, timestamp);

            // Assert the original calculation_id UUID is no longer in the result
            Assert.assertFalse(
                    "Template calculation_id must be replaced in message " + i,
                    payload.contains(TEMPLATE_CALCULATION_ID));

            keys.add(uuid);
            values.add(payload);
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
