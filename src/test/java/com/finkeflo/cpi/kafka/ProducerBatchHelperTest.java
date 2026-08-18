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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.Assert;
import org.junit.Test;

import com.finkeflo.cpi.kafka.ProducerBatchHelper.ProducerPath;

public class ProducerBatchHelperTest {

    private static final long SHORT_BUDGET_MS = 150L;

    private static RecordMetadata metadata(long offset) {
        return new RecordMetadata(new TopicPartition("orders", 0), offset, 0, 0L, 0, 0);
    }

    @Test
    public void allRecordsCompleteWithoutFlush() throws Exception {
        StubProducer producer = new StubProducer(
                CompletableFuture.completedFuture(metadata(10L)),
                CompletableFuture.completedFuture(metadata(11L)));

        ProducerBatchHelper.BatchSendResult result = ProducerBatchHelper.sendBatch(
                producer,
                Arrays.asList(new BatchRecord("k1", "v1"), new BatchRecord("k2", "v2")),
                "orders", null, null, null, null, null, null, null,
                ProducerSendGuard.of(SHORT_BUDGET_MS, "SASL_SSL"),
                ProducerPath.SHARED, "test-client-id");

        Assert.assertEquals(2, producer.sentRecords.size());
        Assert.assertEquals("sendBatch must not call KafkaProducer.flush()", 0, producer.flushCount);
        Assert.assertEquals(2, result.getRecordCount());
        Assert.assertEquals(10L, result.getFirstOffset());
        Assert.assertEquals(11L, result.getLastOffset());
        Assert.assertEquals("0", result.getPartitions());
    }

    @Test
    public void stalledRecordRaisesSendStalledException() throws Exception {
        StubProducer producer = new StubProducer(new CompletableFuture<RecordMetadata>());

        try {
            ProducerBatchHelper.sendBatch(
                    producer,
                    Collections.singletonList(new BatchRecord("k1", "v1")),
                    "orders", null, null, null, null, null, null, null,
                    ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"),
                    ProducerPath.SHARED, "test-client-id");
            Assert.fail("Expected the stalled future to be reported directly");
        } catch (ProducerSendGuard.SendStalledException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("record index 0"));
        }
    }

    @Test
    public void abortDrainUsesOneSharedDeadline() throws Exception {
        StubProducer producer = new StubProducer(
                new CompletableFuture<RecordMetadata>(),
                new CompletableFuture<RecordMetadata>());
        producer.failOnSendIndex = 2;

        long startMs = System.currentTimeMillis();
        try {
            ProducerBatchHelper.sendBatch(
                    producer,
                    Arrays.asList(new BatchRecord("k1", "v1"),
                            new BatchRecord("k2", "v2"),
                            new BatchRecord("k3", "v3")),
                    "orders", null, null, null, null, null, null, null,
                    ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"),
                    ProducerPath.SHARED, "test-client-id");
            Assert.fail("Expected send() failure");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("record index 2"));
        }
        long elapsedMs = System.currentTimeMillis() - startMs;

        Assert.assertTrue("Abort path waited " + elapsedMs + " ms for two buffered records with a "
                + SHORT_BUDGET_MS + " ms budget", elapsedMs < SHORT_BUDGET_MS * 2);
        Assert.assertEquals("flush() would be an unbounded abort-path wait", 0, producer.flushCount);
    }

    private static final class StubProducer implements Producer<byte[], byte[]> {
        private final List<Future<RecordMetadata>> futures;
        private final List<ProducerRecord<byte[], byte[]>> sentRecords = new ArrayList<>();
        private int flushCount;
        private int failOnSendIndex = -1;

        @SafeVarargs
        StubProducer(Future<RecordMetadata>... futures) {
            this.futures = new ArrayList<>(Arrays.asList(futures));
        }

        @Override
        public void initTransactions() {
        }

        @Override
        public void beginTransaction() throws KafkaException {
        }

        @Override
        public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                             ConsumerGroupMetadata groupMetadata) throws KafkaException {
        }

        @Override
        public void commitTransaction() throws KafkaException {
        }

        @Override
        public void abortTransaction() throws KafkaException {
        }

        @Override
        public void registerMetricForSubscription(KafkaMetric metric) {
        }

        @Override
        public void unregisterMetricFromSubscription(KafkaMetric metric) {
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            if (sentRecords.size() == failOnSendIndex) {
                throw new KafkaException("send rejected");
            }
            sentRecords.add(record);
            return futures.remove(0);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            return send(record);
        }

        @Override
        public void flush() {
            flushCount++;
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return Collections.emptyList();
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return Collections.emptyMap();
        }

        @Override
        public Uuid clientInstanceId(Duration timeout) {
            return Uuid.randomUuid();
        }

        @Override
        public void close() {
        }

        @Override
        public void close(Duration timeout) {
        }
    }
}
