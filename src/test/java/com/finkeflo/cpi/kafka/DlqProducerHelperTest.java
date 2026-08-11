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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class DlqProducerHelperTest {

    private static final long SHORT_BUDGET_MS = 150L;

    @After
    public void clearProbeCache() {
        TlsListenerProbe.clearCacheForTests();
    }

    @Test
    public void sendToDlqUsesABoundedWait() {
        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", new StallingProducer(),
                ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"));

        long startMs = System.currentTimeMillis();
        try {
            helper.sendToDlq(new ConsumerRecord<byte[], byte[]>(
                    "orders", 0, 42L, "k".getBytes(), "v".getBytes()),
                    new RuntimeException("boom"), 0);
            Assert.fail("Expected stalled DLQ send to fail");
        } catch (RuntimeException e) {
            long elapsedMs = System.currentTimeMillis() - startMs;
            Assert.assertTrue("DLQ send waited " + elapsedMs + " ms on a "
                    + SHORT_BUDGET_MS + " ms budget", elapsedMs < SHORT_BUDGET_MS * 10);
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("orders-dlq"));
            Assert.assertTrue(KafkaErrorHelper.describeChain(e).contains("DLQ send"));
        }
    }

    @Test
    public void deserializationFailureSendUsesABoundedWait() {
        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", new StallingProducer(),
                ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"));

        long startMs = System.currentTimeMillis();
        try {
            helper.sendDeserializationFailure(new TopicPartition("orders", 0), 42L,
                    null, "bad".getBytes(), new RecordHeaders(), -1L,
                    new RuntimeException("bad bytes"));
            Assert.fail("Expected stalled DLQ send to fail");
        } catch (RuntimeException e) {
            long elapsedMs = System.currentTimeMillis() - startMs;
            Assert.assertTrue("DLQ deser send waited " + elapsedMs + " ms on a "
                    + SHORT_BUDGET_MS + " ms budget", elapsedMs < SHORT_BUDGET_MS * 10);
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("orders-dlq"));
        }
    }

    @Test
    public void constructorRefusesPlaintextAgainstTlsListenerBeforeCreatingProducer() {
        TlsListenerProbe.setProbeRunnerForTests(new TlsListenerProbe.ProbeRunner() {
            @Override
            public TlsListenerProbe.Verdict probe(String address) {
                return TlsListenerProbe.Verdict.TLS;
            }
        });
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setDlqTopic("orders-dlq");
        endpoint.setBootstrapServers("broker:9092");
        endpoint.setSecurityProtocol("PLAINTEXT");

        try {
            new DlqProducerHelper(endpoint);
            Assert.fail("Expected the TLS mismatch to be refused before KafkaProducer creation");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("requires TLS"));
        }
    }

    private static final class StallingProducer implements Producer<byte[], byte[]> {
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
            return new CompletableFuture<>();
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            return send(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public java.util.List<PartitionInfo> partitionsFor(String topic) {
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
