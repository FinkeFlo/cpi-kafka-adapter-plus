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

import static org.awaitility.Awaitility.await;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultCamelContext;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for the drain loop, maxPollRecords, minBacklogToDrain,
 * and maxPartitionFetchSizeKb with a real Kafka broker.
 */
public class DrainAndFetchSizeIT {

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

    // -----------------------------------------------------------------------
    //  Drain Mode Tests
    // -----------------------------------------------------------------------

    /**
     * Without drain mode, only one poll cycle should execute.
     * Even if there are more records left in the topic, a single poll returns
     * at most maxPollRecords and then stops.
     */
    @Test
    public void testNoDrainOnlySinglePoll() throws Exception {
        String topic = "it-nodrain-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        // Produce 20 messages, but maxPollRecords=5 → single poll gets at most 5
        produceMessages(topic, 20);

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("drainEnabled", "false");
        params.put("maxPollRecords", "5");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-nodrain-" + System.nanoTime(), params, captured);

        try {
            int polled = startAndPollOnce(consumer);
            Assert.assertTrue("Without drain, should process at most maxPollRecords (5) in one poll",
                    captured.size() <= 5);
        } finally {
            consumer.doStop();
        }
    }

    /**
     * With drain enabled, the consumer should poll repeatedly until the topic
     * is empty, processing all records across multiple drain cycles.
     */
    @Test
    public void testDrainConsumesAllRecords() throws Exception {
        String topic = "it-drain-all-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        // Produce 25 messages with maxPollRecords=5 → needs multiple drain cycles
        produceMessages(topic, 25);

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("drainEnabled", "true");
        params.put("maxPollRecords", "5");
        params.put("batchTimeout", "2000");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-drain-all-" + System.nanoTime(), params, captured);

        try {
            startAndPoll(consumer);
            Assert.assertEquals("Drain should consume all 25 records across multiple cycles",
                    25, captured.size());
        } finally {
            consumer.doStop();
        }
    }

    /**
     * With drain enabled, a second poll() call should return 0 because
     * the first drain already consumed everything.
     */
    @Test
    public void testDrainLeavesTopicEmpty() throws Exception {
        String topic = "it-drain-empty-" + System.nanoTime();
        String group = "grp-drain-empty-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        produceMessages(topic, 10);

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("drainEnabled", "true");
        params.put("maxPollRecords", "3");
        params.put("batchTimeout", "2000");

        // First consumer drains everything
        List<Exchange> captured1 = new ArrayList<>();
        CpiKafkaPlusConsumer consumer1 = createConsumer(topic, group, params, captured1);
        try {
            startAndPoll(consumer1);
            Assert.assertEquals("First drain should consume all 10 records", 10, captured1.size());
        } finally {
            consumer1.doStop();
        }

        // Second consumer with same group should get nothing
        List<Exchange> captured2 = new ArrayList<>();
        CpiKafkaPlusConsumer consumer2 = createConsumer(topic, group, params, captured2);
        try {
            startAndPoll(consumer2);
            Assert.assertEquals("Second poll should find no records after drain", 0, captured2.size());
        } finally {
            consumer2.doStop();
        }
    }

    // -----------------------------------------------------------------------
    //  minBacklogToDrain Tests
    // -----------------------------------------------------------------------

    /**
     * When minBacklogToDrain is set, the drain should stop once an EXTRA drain cycle
     * (cycle &gt; 1) returns fewer records than the threshold. Those records are seeked
     * back (un-polled) so they are processed in the next polling interval, avoiding
     * tiny tail MPLs.
     */
    @Test
    public void testMinBacklogToDrainStopsEarly() throws Exception {
        String topic = "it-minbacklog-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        // Produce 12 messages, maxPollRecords=5, minBacklogToDrain=5
        // Cycle 1 (first poll, always processes): 5 records → process
        // Cycle 2: 5 records (>= minBacklog) → process
        // Cycle 3: 2 records (< minBacklog=5) → seek back, skip process, stop drain
        // Total processed: 10 (the trailing 2 wait for the next polling interval)
        produceMessages(topic, 12);

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("drainEnabled", "true");
        params.put("maxPollRecords", "5");
        params.put("minBacklogToDrain", "5");
        params.put("batchTimeout", "2000");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-minbacklog-" + System.nanoTime(), params, captured);

        try {
            startAndPoll(consumer);
            Assert.assertEquals("Should consume only 10 of 12 records: extra drain cycle below minBacklog "
                            + "is seeked back and processed on next poll",
                    10, captured.size());
        } finally {
            consumer.doStop();
        }
    }

    /**
     * The very first poll of a scheduled run must always process its records, even if
     * fewer than minBacklogToDrain are returned. Otherwise, mini-batches would wait an
     * entire polling interval, which defeats the point of low-latency consumption.
     */
    @Test
    public void testFirstPollAlwaysProcessesEvenBelowMinBacklog() throws Exception {
        String topic = "it-firstpoll-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        // Only 2 records in topic, minBacklogToDrain=100. First poll returns 2 records,
        // which is below the threshold but must still be processed.
        produceMessages(topic, 2);

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("drainEnabled", "true");
        params.put("maxPollRecords", "100");
        params.put("minBacklogToDrain", "100");
        params.put("batchTimeout", "2000");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-firstpoll-" + System.nanoTime(), params, captured);

        try {
            startAndPoll(consumer);
            Assert.assertEquals("First poll must always process, regardless of minBacklog",
                    2, captured.size());
        } finally {
            consumer.doStop();
        }
    }

    /**
     * With minBacklogToDrain=0 (default), the drain should continue until
     * the topic is completely empty (poll returns 0 records).
     */
    @Test
    public void testMinBacklogZeroDrainsCompletely() throws Exception {
        String topic = "it-minbacklog0-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        produceMessages(topic, 8);

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("drainEnabled", "true");
        params.put("maxPollRecords", "3");
        params.put("minBacklogToDrain", "0");
        params.put("batchTimeout", "2000");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-minbacklog0-" + System.nanoTime(), params, captured);

        try {
            startAndPoll(consumer);
            Assert.assertEquals("minBacklog=0 should drain all 8 records", 8, captured.size());
        } finally {
            consumer.doStop();
        }
    }

    // -----------------------------------------------------------------------
    //  maxPollRecords Tests
    // -----------------------------------------------------------------------

    /**
     * maxPollRecords limits how many records a single Kafka poll() returns.
     * Without drain, this caps total records per poll() invocation.
     */
    @Test
    public void testMaxPollRecordsLimitsSinglePoll() throws Exception {
        String topic = "it-maxpoll-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        produceMessages(topic, 50);

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("drainEnabled", "false");
        params.put("maxPollRecords", "10");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-maxpoll-" + System.nanoTime(), params, captured);

        try {
            startAndPollOnce(consumer);
            Assert.assertTrue("Single poll should return at most maxPollRecords (10)",
                    captured.size() <= 10);
            Assert.assertTrue("Single poll should return at least 1 record",
                    captured.size() >= 1);
        } finally {
            consumer.doStop();
        }
    }

    /**
     * With drain enabled and maxPollRecords set, the drain should execute
     * multiple cycles of maxPollRecords each until topic is empty.
     */
    @Test
    public void testMaxPollRecordsWithDrain() throws Exception {
        String topic = "it-maxpoll-drain-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        produceMessages(topic, 30);

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("drainEnabled", "true");
        params.put("maxPollRecords", "7");
        params.put("batchTimeout", "2000");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-maxpoll-drain-" + System.nanoTime(), params, captured);

        try {
            startAndPoll(consumer);
            Assert.assertEquals("Drain with maxPollRecords=7 should consume all 30 records",
                    30, captured.size());
        } finally {
            consumer.doStop();
        }
    }

    // -----------------------------------------------------------------------
    //  maxPartitionFetchSizeKb Tests
    // -----------------------------------------------------------------------

    /**
     * Verify that maxPartitionFetchSizeKb is correctly wired to the Kafka
     * consumer property max.partition.fetch.bytes.
     */
    @Test
    public void testMaxPartitionFetchSizeKbProperty() throws Exception {
        String topic = "it-fetchsize-prop-" + System.nanoTime();

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("maxPartitionFetchSizeKb", "5120");

        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, "grp-fetch-prop-" + System.nanoTime(), params));
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, null);

        java.util.Properties props = consumer.buildConsumerProperties();

        int expectedBytes = 5120 * 1024; // 5 MB
        Assert.assertEquals("max.partition.fetch.bytes should be 5 MB in bytes",
                expectedBytes, props.get("max.partition.fetch.bytes"));
    }

    /**
     * With a very small maxPartitionFetchSizeKb (approximated via small fetch size),
     * the broker returns fewer records per poll because the byte limit is hit
     * before the record count limit. This forces more drain cycles.
     *
     * We test this by producing large messages that exceed 1 MB together,
     * and verifying that with a small fetch size, more poll cycles are needed
     * (all records still consumed with drain enabled).
     */
    @Test
    public void testSmallFetchSizeForcesMoreDrainCycles() throws Exception {
        String topic = "it-fetchsize-small-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        // Produce 10 messages, each ~200 KB → total ~2 MB
        // With maxPartitionFetchSizeKb=1024 (1 MB), broker can return ~5 per poll
        // With maxPollRecords=100 (high), the byte limit is the bottleneck
        String largeValue = createLargeValue(200 * 1024); // 200 KB

        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            keys.add("key-" + i);
            values.add(largeValue);
        }
        KafkaTestInfrastructure.produceStringMessages(topic, keys, values);

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("drainEnabled", "true");
        params.put("maxPollRecords", "100");
        params.put("maxPartitionFetchSizeKb", "1024");
        params.put("batchTimeout", "2000");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-fetchsize-small-" + System.nanoTime(), params, captured);

        try {
            startAndPoll(consumer);
            // All 10 records should be consumed even with byte size limit
            Assert.assertEquals("All 10 large records should be consumed via drain",
                    10, captured.size());
        } finally {
            consumer.doStop();
        }
    }

    /**
     * With a large maxPartitionFetchSizeKb, all records can be fetched
     * in a single poll even if there's a lot of data.
     */
    @Test
    public void testLargeFetchSizeAllowsBigBatch() throws Exception {
        String topic = "it-fetchsize-large-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        // Produce 10 messages, each ~200 KB → total ~2 MB
        String largeValue = createLargeValue(200 * 1024);

        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            keys.add("key-" + i);
            values.add(largeValue);
        }
        KafkaTestInfrastructure.produceStringMessages(topic, keys, values);

        // maxPartitionFetchSizeKb=10240 (10 MB) → can fetch all 2 MB in one poll
        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("drainEnabled", "false");
        params.put("maxPollRecords", "100");
        params.put("maxPartitionFetchSizeKb", "10240");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-fetchsize-large-" + System.nanoTime(), params, captured);

        try {
            startAndPollOnce(consumer);
            // With 10 MB fetch size and only 2 MB of data, all should fit in one poll
            Assert.assertEquals("Large fetch size should allow all 10 records in a single poll",
                    10, captured.size());
        } finally {
            consumer.doStop();
        }
    }

    // -----------------------------------------------------------------------
    //  Drain + Batch Mode Tests
    // -----------------------------------------------------------------------

    /**
     * Drain mode should also work correctly in batch mode (JSON_ARRAY),
     * producing multiple batch exchanges across drain cycles.
     */
    @Test
    public void testDrainWithBatchMode() throws Exception {
        String topic = "it-drain-batch-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        // 15 messages, maxPollRecords=5, batchSize=5 → 3 batch exchanges
        produceMessages(topic, 15);

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "true");
        params.put("batchOutputFormat", "JSON_ARRAY");
        params.put("batchSize", "100");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("drainEnabled", "true");
        params.put("maxPollRecords", "5");
        params.put("batchTimeout", "2000");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-drain-batch-" + System.nanoTime(), params, captured);

        try {
            startAndPoll(consumer);

            // Count total records across all batch exchanges
            int totalRecords = 0;
            for (Exchange ex : captured) {
                Integer count = ex.getIn().getHeader("CpiKafkaPlusRecordCount", Integer.class);
                Assert.assertNotNull("Each batch should have CpiKafkaPlusRecordCount header", count);
                totalRecords += count;
            }
            Assert.assertEquals("Total records across all drain batches should be 15",
                    15, totalRecords);
        } finally {
            consumer.doStop();
        }
    }

    // -----------------------------------------------------------------------
    //  Helpers
    // -----------------------------------------------------------------------

    private CpiKafkaPlusConsumer createConsumer(String topic, String groupId,
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

    private void startAndPoll(CpiKafkaPlusConsumer consumer) throws Exception {
        consumer.doStart();

        Method pollMethod = CpiKafkaPlusConsumer.class.getDeclaredMethod("poll");
        pollMethod.setAccessible(true);

        try {
            await().atMost(Duration.ofMillis(7500))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> (Integer) pollMethod.invoke(consumer) > 0);
        } catch (ConditionTimeoutException ignored) {
            // Some call sites intentionally verify that a follow-up poll stays empty.
        }
    }

    /**
     * Start consumer and invoke poll() exactly once (after partition assignment).
     * Returns the number of records processed by that single poll invocation.
     */
    private int startAndPollOnce(CpiKafkaPlusConsumer consumer) throws Exception {
        consumer.doStart();

        Method pollMethod = CpiKafkaPlusConsumer.class.getDeclaredMethod("poll");
        pollMethod.setAccessible(true);

        final int[] polled = new int[1];
        try {
            await().atMost(Duration.ofMillis(7500))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> {
                        polled[0] = (Integer) pollMethod.invoke(consumer);
                        return polled[0] > 0;
                    });
        } catch (ConditionTimeoutException ignored) {
            // Callers assert on the returned value and captured exchanges.
        }
        return polled[0];
    }

    private void produceMessages(String topic, int count) {
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            keys.add("key-" + i);
            values.add("{\"index\":" + i + ",\"msg\":\"test-message-" + i + "\"}");
        }
        KafkaTestInfrastructure.produceStringMessages(topic, keys, values);
    }

    private String createLargeValue(int sizeBytes) {
        StringBuilder sb = new StringBuilder(sizeBytes + 20);
        sb.append("{\"data\":\"");
        // Fill with 'A' characters, leaving room for JSON wrapper
        for (int i = 0; i < sizeBytes - 12; i++) {
            sb.append('A');
        }
        sb.append("\"}");
        return sb.toString();
    }
}
