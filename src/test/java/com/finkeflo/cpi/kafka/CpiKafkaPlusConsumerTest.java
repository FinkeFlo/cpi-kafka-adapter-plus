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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for CpiKafkaPlusConsumer helper methods.
 * Focuses on partition-aware batching logic which does not require a running Kafka broker.
 */
public class CpiKafkaPlusConsumerTest {

    @Test
    public void testGroupByPartitionSinglePartition() {
        ConsumerRecords<byte[], byte[]> records = buildRecords(
                rec("topic-a", 0, 0),
                rec("topic-a", 0, 1),
                rec("topic-a", 0, 2)
        );

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> grouped =
                RecordProcessor.groupByPartition(records);

        Assert.assertEquals("Should have 1 partition group", 1, grouped.size());

        TopicPartition tp0 = new TopicPartition("topic-a", 0);
        Assert.assertTrue("Should contain partition 0", grouped.containsKey(tp0));
        Assert.assertEquals("Partition 0 should have 3 records", 3, grouped.get(tp0).size());

        // Verify offset order is preserved
        Assert.assertEquals(0, grouped.get(tp0).get(0).offset());
        Assert.assertEquals(1, grouped.get(tp0).get(1).offset());
        Assert.assertEquals(2, grouped.get(tp0).get(2).offset());
    }

    @Test
    public void testGroupByPartitionMultiplePartitions() {
        ConsumerRecords<byte[], byte[]> records = buildRecords(
                rec("topic-a", 0, 10),
                rec("topic-a", 1, 20),
                rec("topic-a", 0, 11),
                rec("topic-a", 2, 30),
                rec("topic-a", 1, 21),
                rec("topic-a", 0, 12)
        );

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> grouped =
                RecordProcessor.groupByPartition(records);

        Assert.assertEquals("Should have 3 partition groups", 3, grouped.size());

        // Partition 0: offsets 10, 11, 12
        List<ConsumerRecord<byte[], byte[]>> p0 = grouped.get(new TopicPartition("topic-a", 0));
        Assert.assertNotNull("Partition 0 should exist", p0);
        Assert.assertEquals(3, p0.size());
        Assert.assertEquals(10, p0.get(0).offset());
        Assert.assertEquals(11, p0.get(1).offset());
        Assert.assertEquals(12, p0.get(2).offset());

        // Partition 1: offsets 20, 21
        List<ConsumerRecord<byte[], byte[]>> p1 = grouped.get(new TopicPartition("topic-a", 1));
        Assert.assertNotNull("Partition 1 should exist", p1);
        Assert.assertEquals(2, p1.size());
        Assert.assertEquals(20, p1.get(0).offset());
        Assert.assertEquals(21, p1.get(1).offset());

        // Partition 2: offset 30
        List<ConsumerRecord<byte[], byte[]>> p2 = grouped.get(new TopicPartition("topic-a", 2));
        Assert.assertNotNull("Partition 2 should exist", p2);
        Assert.assertEquals(1, p2.size());
        Assert.assertEquals(30, p2.get(0).offset());
    }

    @Test
    public void testGroupByPartitionMultipleTopics() {
        ConsumerRecords<byte[], byte[]> records = buildRecords(
                rec("topic-a", 0, 1),
                rec("topic-b", 0, 1),
                rec("topic-a", 0, 2)
        );

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> grouped =
                RecordProcessor.groupByPartition(records);

        Assert.assertEquals("Should have 2 groups (different topics)", 2, grouped.size());
        Assert.assertEquals(2, grouped.get(new TopicPartition("topic-a", 0)).size());
        Assert.assertEquals(1, grouped.get(new TopicPartition("topic-b", 0)).size());
    }

    @Test
    public void testGroupByPartitionReturnsLinkedHashMap() {
        // Verify groupByPartition returns a LinkedHashMap (preserves encounter order)
        ConsumerRecords<byte[], byte[]> records = buildRecords(
                rec("t", 0, 0),
                rec("t", 1, 0)
        );

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> grouped =
                RecordProcessor.groupByPartition(records);

        Assert.assertTrue("Should return a LinkedHashMap for deterministic iteration",
                grouped instanceof LinkedHashMap);
    }

    @Test
    public void testBuildConsumerPropertiesContainsTimeoutSettings() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=test-group");
            CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, null);

            Properties props = consumer.buildConsumerProperties();

            Assert.assertEquals("socket.connection.setup.timeout.ms should be 10000",
                    10000, props.get("socket.connection.setup.timeout.ms"));
            Assert.assertEquals("socket.connection.setup.timeout.max.ms should be 30000",
                    30000, props.get("socket.connection.setup.timeout.max.ms"));
            Assert.assertEquals("request.timeout.ms should be 30000",
                    30000, props.get(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG));
            Assert.assertEquals("default.api.timeout.ms should be 60000",
                    60000, props.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
            ctx.stop();
        }
    }

    @Test
    public void testBuildConsumerPropertiesContainsGroupMembershipSettings() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=test-group");
            CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, null);

            Properties props = consumer.buildConsumerProperties();

            Assert.assertEquals("partition.assignment.strategy should be CooperativeStickyAssignor",
                    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor",
                    props.get(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG));
            Assert.assertEquals("session.timeout.ms should be 30000 (relaxed from 10s hotfix to avoid false-positive evictions, issue #45)",
                    30000, props.get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG));
            Assert.assertEquals("heartbeat.interval.ms should be 10000 (≤ session.timeout/3)",
                    10000, props.get(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG));
            ctx.stop();
        }
    }

    @Test
    public void testPollingIntervalDefault() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=test-group");

            CpiKafkaPlusConsumer consumer = (CpiKafkaPlusConsumer) endpoint.createConsumer(
                    new org.apache.camel.Processor() {
                        @Override
                        public void process(org.apache.camel.Exchange exchange) {}
                    });

            Assert.assertEquals("Default polling interval should be 5s",
                    5000, consumer.getDelay());
            ctx.stop();
        }
    }

    @Test
    public void testStaticMembershipPropAddedWhenSuffixAvailable() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=test-group");
            CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, null);

            Properties props = consumer.buildConsumerProperties();
            String suffix = CpiKafkaPlusConsumer.resolveStaticMemberSuffix();

            if (suffix != null) {
                Assert.assertEquals("group.instance.id should combine groupId with the resolved suffix",
                        "test-group-" + suffix,
                        props.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
            } else {
                Assert.assertNull("group.instance.id must be absent when no env var is available",
                        props.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
            }
            ctx.stop();
        }
    }

    @Test
    public void testResolveStaticMemberSuffixPrefersCfInstanceIndex() {
        // The helper just reads env vars; whatever the test JVM has is fine,
        // we only assert the contract: non-empty -> returned, empty -> null fallback.
        String result = CpiKafkaPlusConsumer.resolveStaticMemberSuffix();
        if (result != null) {
            Assert.assertFalse("returned suffix must not be empty", result.isEmpty());
        }
    }

    @Test
    public void testSchedulerDelayCappedForLongInterval() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=test-group"
                    + "&pollingIntervalSeconds=900");

            CpiKafkaPlusConsumer consumer = (CpiKafkaPlusConsumer) endpoint.createConsumer(
                    new org.apache.camel.Processor() {
                        @Override
                        public void process(org.apache.camel.Exchange exchange) {}
                    });

            // Scheduler-Takt = min(pollingIntervalSeconds, 60). Bei einem 15-Minuten-Intervall
            // tickt der Scheduler alle 60s (Keep-Alive-Poll); pollingIntervalSeconds bleibt
            // das Emit-Intervall.
            Assert.assertEquals("scheduler delay should be capped at the 60s keep-alive tick",
                    60000, consumer.getDelay());
            ctx.stop();
        }
    }

    @Test
    public void testSchedulerDelayUncappedForShortInterval() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=test-group"
                    + "&pollingIntervalSeconds=30");

            CpiKafkaPlusConsumer consumer = (CpiKafkaPlusConsumer) endpoint.createConsumer(
                    new org.apache.camel.Processor() {
                        @Override
                        public void process(org.apache.camel.Exchange exchange) {}
                    });

            // Scheduler-Takt = min(pollingIntervalSeconds, 60). 30 <= 60, daher wird das
            // konfigurierte Intervall unveraendert uebernommen — keine Deckelung.
            Assert.assertEquals(
                    "intervals at or below the 60s keep-alive tick are used unchanged (no cap)",
                    30000, consumer.getDelay());
            ctx.stop();
        }
    }

    @Test
    public void testInitialDelayIsAlwaysFiveSeconds() throws Exception {
        // First poll must happen ~5s after start so the Kafka consumer joins the group
        // immediately, not after a full pollingIntervalSeconds interval. Regression test:
        // previously initialDelay = pollingIntervalSeconds*1000, leaving the consumer
        // group EMPTY for the full polling interval after deploy.
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=test-group"
                    + "&pollingIntervalSeconds=600");

            CpiKafkaPlusConsumer consumer = (CpiKafkaPlusConsumer) endpoint.createConsumer(
                    new org.apache.camel.Processor() {
                        @Override
                        public void process(org.apache.camel.Exchange exchange) {}
                    });

            Assert.assertEquals("initialDelay must be 5000ms regardless of pollingIntervalSeconds",
                    5000, consumer.getInitialDelay());
            // Scheduler-Takt ist auf den 60s-Keep-Alive-Takt gedeckelt: min(600, 60) = 60s.
            Assert.assertEquals("delay must be capped at the 60s keep-alive tick",
                    60000, consumer.getDelay());
            ctx.stop();
        }
    }

    @Test
    public void testIsRetryableWithConnectException() {
        Assert.assertTrue(RecordProcessor.isRetryable(
                new java.net.ConnectException("Connection refused")));
    }

    @Test
    public void testIsRetryableWithSocketTimeoutException() {
        Assert.assertTrue(RecordProcessor.isRetryable(
                new java.net.SocketTimeoutException("Read timed out")));
    }

    @Test
    public void testIsRetryableWithTimeoutException() {
        Assert.assertTrue(RecordProcessor.isRetryable(
                new java.util.concurrent.TimeoutException("Timed out")));
    }

    @Test
    public void testIsRetryableWithSocketException() {
        Assert.assertTrue(RecordProcessor.isRetryable(
                new java.net.SocketException("Connection reset")));
    }

    @Test
    public void testIsRetryableWithUnknownHostException() {
        Assert.assertTrue(RecordProcessor.isRetryable(
                new java.net.UnknownHostException("unknown.host")));
    }

    @Test
    public void testIsRetryableWithWrappedConnectException() {
        Assert.assertTrue(RecordProcessor.isRetryable(
                new RuntimeException("Wrapped",
                        new java.net.ConnectException("Connection refused"))));
    }

    @Test
    public void testIsNotRetryableWithNullPointerException() {
        Assert.assertFalse(RecordProcessor.isRetryable(
                new NullPointerException("null")));
    }

    @Test
    public void testIsNotRetryableWithIllegalArgumentException() {
        Assert.assertFalse(RecordProcessor.isRetryable(
                new IllegalArgumentException("bad arg")));
    }

    @Test
    public void testIsNotRetryableWithFileNotFoundException() {
        Assert.assertFalse(RecordProcessor.isRetryable(
                new RuntimeException("Wrapped",
                        new java.io.FileNotFoundException("not found"))));
    }

    @Test
    public void testIsNotRetryableWithClassCastException() {
        Assert.assertFalse(RecordProcessor.isRetryable(
                new ClassCastException("bad cast")));
    }

    // --- Issue #45: max.poll.interval.ms scaling ---

    @Test
    public void computeMaxPollIntervalMs_shortPolling_returnsBufferOnly() {
        // pollingIntervalSeconds=10 -> 10_000 + 600_000 = 610_000 (10 min buffer dominates)
        Assert.assertEquals(610_000, CpiKafkaPlusConsumer.computeMaxPollIntervalMs(10));
    }

    @Test
    public void computeMaxPollIntervalMs_oneSecond_returnsBufferPlusPolling() {
        // pollingIntervalSeconds=1 -> 1_000 + 600_000 = 601_000
        // Confirms the 10-min buffer always exceeds the (now removed) Kafka default of 300_000.
        Assert.assertEquals(601_000, CpiKafkaPlusConsumer.computeMaxPollIntervalMs(1));
    }

    @Test
    public void computeMaxPollIntervalMs_thirtyMinutes_returnsPollingPlusBuffer() {
        // pollingIntervalSeconds=1800 (30 min) -> 1_800_000 + 600_000 = 2_400_000
        // This is the regression test for the PRD T2A bug (issue #45).
        Assert.assertEquals(2_400_000, CpiKafkaPlusConsumer.computeMaxPollIntervalMs(1800));
    }

    @Test
    public void computeMaxPollIntervalMs_sixtyMinutes_returnsPollingPlusBuffer() {
        // pollingIntervalSeconds=3600 (60 min) -> 3_600_000 + 600_000 = 4_200_000
        // No longer at the hard cap after the cap was raised to 6 h 10 min.
        Assert.assertEquals(4_200_000, CpiKafkaPlusConsumer.computeMaxPollIntervalMs(3600));
    }

    @Test
    public void computeMaxPollIntervalMs_sixHours_clampsToHardCap() {
        // pollingIntervalSeconds=21600 (6 h, max allowed)
        // -> 21_600_000 + 600_000 = 22_200_000 == hard cap (6 h 10 min)
        Assert.assertEquals(22_200_000, CpiKafkaPlusConsumer.computeMaxPollIntervalMs(21600));
    }

    // --- Issue #45 follow-up: Keep-Alive-Poll ---

    @Test
    public void computeSchedulerDelaySeconds_shortInterval_returnsInterval() {
        // pollingInterval unter dem Keep-Alive-Takt -> unveraendert (Verhalten wie heute)
        Assert.assertEquals(5L, CpiKafkaPlusConsumer.computeSchedulerDelaySeconds(5));
    }

    @Test
    public void computeSchedulerDelaySeconds_atBoundary_returnsInterval() {
        Assert.assertEquals(60L, CpiKafkaPlusConsumer.computeSchedulerDelaySeconds(60));
    }

    @Test
    public void computeSchedulerDelaySeconds_longInterval_clampsToKeepAlive() {
        Assert.assertEquals(60L, CpiKafkaPlusConsumer.computeSchedulerDelaySeconds(61));
        Assert.assertEquals(60L, CpiKafkaPlusConsumer.computeSchedulerDelaySeconds(7200));
    }

    @Test
    public void isEmitDue_firstTick_returnsTrue() {
        // lastEmitTimeMs == 0 -> erster Tick nach Start -> Emit
        Assert.assertTrue(CpiKafkaPlusConsumer.isEmitDue(0L, 1_000_000L, 7_200_000L));
    }

    @Test
    public void isEmitDue_justEmitted_returnsFalse() {
        // vor 5 s emittiert, Intervall 2 h -> nicht faellig -> Keep-Alive
        Assert.assertFalse(CpiKafkaPlusConsumer.isEmitDue(1_000_000L, 1_005_000L, 7_200_000L));
    }

    @Test
    public void isEmitDue_intervalExactlyElapsed_returnsTrue() {
        // exakt 2 h seit letztem Emit -> faellig
        Assert.assertTrue(CpiKafkaPlusConsumer.isEmitDue(1_000_000L, 8_200_000L, 7_200_000L));
    }

    @Test
    public void isEmitDue_intervalExceeded_returnsTrue() {
        Assert.assertTrue(CpiKafkaPlusConsumer.isEmitDue(1_000_000L, 9_000_000L, 7_200_000L));
    }

    @Test
    public void isEmitDue_sameTimestamp_returnsFalse() {
        // nowMs == lastEmitTimeMs -> 0 ms vergangen -> nicht faellig
        Assert.assertFalse(CpiKafkaPlusConsumer.isEmitDue(1_000_000L, 1_000_000L, 7_200_000L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void validatePollingIntervalSeconds_zero_throws() {
        CpiKafkaPlusConsumer.validatePollingIntervalSeconds(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void validatePollingIntervalSeconds_negative_throws() {
        CpiKafkaPlusConsumer.validatePollingIntervalSeconds(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void validatePollingIntervalSeconds_above21600_throws() {
        CpiKafkaPlusConsumer.validatePollingIntervalSeconds(21601);
    }

    @Test
    public void validatePollingIntervalSeconds_21600_doesNotThrow() {
        // 21600 (6 h) is the inclusive upper bound — must not throw.
        CpiKafkaPlusConsumer.validatePollingIntervalSeconds(21600);
    }

    @Test
    public void validatePollingIntervalSeconds_3600_doesNotThrow() {
        // 60 min must still be accepted (was previously the upper bound).
        CpiKafkaPlusConsumer.validatePollingIntervalSeconds(3600);
    }

    @Test
    public void validatePollingIntervalSeconds_5_doesNotThrow() {
        // Default value, must not throw.
        CpiKafkaPlusConsumer.validatePollingIntervalSeconds(5);
    }

    @Test
    public void testBuildConsumerPropertiesScalesMaxPollInterval() throws Exception {
        // Integration check: properties built with pollingIntervalSeconds=1800
        // contain max.poll.interval.ms=2_400_000 (regression test for issue #45).
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=test-group"
                    + "&pollingIntervalSeconds=1800");
            CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, null);

            Properties props = consumer.buildConsumerProperties();

            Assert.assertEquals("max.poll.interval.ms must scale with pollingIntervalSeconds=1800",
                    2_400_000, props.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
            ctx.stop();
        }
    }

    // --- Helper methods ---

    private static ConsumerRecord<byte[], byte[]> rec(String topic, int partition, long offset) {
        return new ConsumerRecord<>(topic, partition, offset, null, "value".getBytes());
    }

    @SafeVarargs
    private static ConsumerRecords<byte[], byte[]> buildRecords(ConsumerRecord<byte[], byte[]>... records) {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = new HashMap<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            List<ConsumerRecord<byte[], byte[]>> list = map.get(tp);
            if (list == null) {
                list = new ArrayList<>();
                map.put(tp, list);
            }
            list.add(record);
        }
        return new ConsumerRecords<>(map);
    }
}
