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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.camel.Message;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends a list of {@link BatchRecord} to Kafka using async sends and bounded waits.
 * Sets response headers and XML summary body.
 */
public final class ProducerBatchHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerBatchHelper.class);

    private ProducerBatchHelper() {}

    /**
     * Producer path identifier for diagnostics. Every diagnostic line from the producer path must
     * include this tag so the first question in any investigation ("which path was in play?") is
     * answered immediately.
     */
    public enum ProducerPath {
        /** The shared, non-transactional producer reused across exchanges. */
        SHARED,
        /** A per-transaction, short-lived producer created for exactly-once batches. */
        TRANSACTIONAL
    }


    /**
     * Result of a batch send operation.
     */
    public static final class BatchSendResult {
        private final int recordCount;
        private final long firstOffset;
        private final long lastOffset;
        private final String partitions;
        private final long durationMs;

        BatchSendResult(int recordCount, long firstOffset, long lastOffset,
                        String partitions, long durationMs) {
            this.recordCount = recordCount;
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.partitions = partitions;
            this.durationMs = durationMs;
        }

        public int getRecordCount() { return recordCount; }
        public long getFirstOffset() { return firstOffset; }
        public long getLastOffset() { return lastOffset; }
        public String getPartitions() { return partitions; }
        public long getDurationMs() { return durationMs; }
    }

    /**
     * Send all records asynchronously, then evaluate their futures against one shared deadline.
     *
     * @param producer      Kafka producer instance
     * @param records       parsed batch records
     * @param topic         target topic
     * @param fallbackKey   fallback key from kafka.KEY header (may be null)
     * @param partition     partition from kafka.PARTITION_KEY header (may be null)
     * @param timestamp     timestamp from kafka.OVERRIDE_TIMESTAMP header (may be null)
     * @param message       exchange message for adding record headers
     * @param headerAdder   function to add exchange headers to each ProducerRecord
     * @param sendGuard     bounds the wait for the send results of this batch
     * @param producerPath  identifies whether this is the shared or transactional path (c2)
     * @param clientId      the Kafka client.id for correlation with broker-side logs (c3)
     * @return BatchSendResult with offsets and timing
     */
    public static BatchSendResult sendBatch(
            Producer<byte[], byte[]> producer,
            List<BatchRecord> records,
            String topic,
            String fallbackKey,
            Integer partition,
            Long timestamp,
            Message message,
            RecordHeaderAdder headerAdder,
            ByteSerializer valueSerializer,
            ByteSerializer keySerializer,
            ProducerSendGuard sendGuard,
            ProducerPath producerPath,
            String clientId) throws Exception {

        long startMs = System.currentTimeMillis();
        // One deadline for the whole batch: waiting per record would multiply the budget by the
        // record count and re-open the very hang this guard exists to prevent.
        long deadlineMs = sendGuard.newDeadline();

        List<Future<RecordMetadata>> futures = sendRecordsAsync(
                producer, records, topic, fallbackKey, partition, timestamp,
                message, headerAdder, valueSerializer, keySerializer, sendGuard, deadlineMs,
                startMs, producerPath, clientId);

        // No producer.flush() here: flush() waits on the sender thread with no timeout of its own,
        // so a dead sender thread would block before any future could be evaluated. With
        // linger.ms=0 the records are handed to the sender immediately anyway, and the bounded
        // waits below provide the same "all records completed" guarantee.

        long firstOffset = -1;
        long lastOffset = -1;
        Set<Integer> partitionSet = new LinkedHashSet<>();

        for (int i = 0; i < futures.size(); i++) {
            RecordMetadata metadata;
            try {
                metadata = sendGuard.await(futures.get(i), deadlineMs,
                        "Batch send to topic '" + topic + "' (record index " + i + ")");
            } catch (ProducerSendGuard.SendStalledException e) {
                throw e;
            } catch (Exception e) {
                AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.batch.record.await")
                        .with("producerPath", producerPath)
                        .withOptional("clientId", clientId)
                        .with("phase", "AWAIT_FUTURE")
                        .with("topic", topic)
                        .with("recordIndex", i)
                        .with("batchSize", futures.size())
                        .with("elapsedMs", System.currentTimeMillis() - startMs)
                        .with("thread", Thread.currentThread().getName()), e);
                throw new RuntimeException(
                        "Batch send failed at record index " + i + " (phase=AWAIT_FUTURE): "
                                + e.getMessage(), e);
            }

            if (i == 0) {
                firstOffset = metadata.offset();
            }
            lastOffset = metadata.offset();
            partitionSet.add(metadata.partition());
        }

        long durationMs = System.currentTimeMillis() - startMs;

        StringBuilder partStr = new StringBuilder();
        for (Integer p : partitionSet) {
            if (partStr.length() > 0) {
                partStr.append(",");
            }
            partStr.append(p);
        }

        LOG.info("[CPI-KAFKA-PLUS-DIAG] Batch send complete: {} records to topic '{}', "
                + "offsets {}-{}, partitions [{}], {}ms",
                records.size(), topic, firstOffset, lastOffset, partStr, durationMs);

        return new BatchSendResult(records.size(), firstOffset, lastOffset,
                partStr.toString(), durationMs);
    }

    private static List<Future<RecordMetadata>> sendRecordsAsync(
            Producer<byte[], byte[]> producer,
            List<BatchRecord> records,
            String topic,
            String fallbackKey,
            Integer partition,
            Long timestamp,
            Message message,
            RecordHeaderAdder headerAdder,
            ByteSerializer valueSerializer,
            ByteSerializer keySerializer,
            ProducerSendGuard sendGuard,
            long deadlineMs,
            long batchStartMs,
            ProducerPath producerPath,
            String clientId) {

        List<Future<RecordMetadata>> futures = new ArrayList<>(records.size());
        // One allowance for the whole batch, so the per-record limit cannot be multiplied by the
        // record count.
        MonitorFaultRetry.Budget batchBudget = new MonitorFaultRetry.Budget();

        for (int i = 0; i < records.size(); i++) {
            final int recordIndex = i;
            BatchRecord record = records.get(i);

            String keyStr = record.getKey();
            if (keyStr == null) {
                keyStr = fallbackKey;
            }

            byte[] key = null;
            if (keyStr != null) {
                key = keySerializer != null
                        ? keySerializer.serialize(topic, keyStr)
                        : keyStr.getBytes(StandardCharsets.UTF_8);
            }
            byte[] value = null;
            if (record.getValue() != null) {
                value = valueSerializer != null
                        ? valueSerializer.serialize(topic, record.getValue())
                        : record.getValue().getBytes(StandardCharsets.UTF_8);
            }

            ProducerRecord<byte[], byte[]> pr = new ProducerRecord<>(
                    topic, partition, timestamp, key, value);

            if (headerAdder != null) {
                headerAdder.addHeaders(pr, message);
            }

            if (record.getHeaders() != null) {
                for (java.util.Map.Entry<String, String> entry : record.getHeaders().entrySet()) {
                    if (entry.getValue() != null) {
                        pr.headers().remove(entry.getKey()); // overwrite if added by headerAdder
                        pr.headers().add(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8));
                    }
                }
            }

            try {
                futures.add(MonitorFaultRetry.execute(
                        () -> producer.send(pr), batchBudget, deadlineMs, topic, recordIndex));
            } catch (Exception e) {
                AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.batch.record.send")
                        .with("producerPath", producerPath)
                        .withOptional("clientId", clientId)
                        .with("phase", "SYNC_SEND")
                        .with("topic", topic)
                        .with("recordIndex", i)
                        .with("batchSize", records.size())
                        .with("bufferedRecords", futures.size())
                        .with("elapsedMs", System.currentTimeMillis() - batchStartMs)
                        .with("thread", Thread.currentThread().getName()), e);
                // Bounded drain instead of flush(): flush() has no timeout and would hang here if
                // the sender thread is what caused this failure in the first place.
                sendGuard.awaitAllQuietly(futures, deadlineMs);
                throw new RuntimeException(
                        "Batch send failed at record index " + i + " (phase=SYNC_SEND): "
                                + e.getMessage(), e);
            }
        }

        return futures;
    }

    /**
     * Set response headers and XML summary body on the exchange message.
     */
    public static void setResponseHeadersAndBody(Message message, String topic,
                                                  String batchMode, BatchSendResult result) {
        message.setHeader("SAP_Receiver", topic);
        message.setHeader("CamelKafkaTopic", topic);
        message.setHeader("CpiKafkaPlusTopic", topic);
        message.setHeader("CpiKafkaPlusStatus", "OK");
        message.setHeader("CpiKafkaPlusRecordCount", result.getRecordCount());
        message.setHeader("CpiKafkaPlusBatchInputFormat", batchMode);
        message.setHeader("CpiKafkaPlusFirstOffset", result.getFirstOffset());
        message.setHeader("CpiKafkaPlusLastOffset", result.getLastOffset());
        message.setHeader("CpiKafkaPlusPartitions", result.getPartitions());

        // XML summary body
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        sb.append("<kafkaBatchResult>\n");
        sb.append("  <status>OK</status>\n");
        sb.append("  <topic>").append(BatchFormatter.escapeXml(topic)).append("</topic>\n");
        sb.append("  <recordCount>").append(result.getRecordCount()).append("</recordCount>\n");
        sb.append("  <firstOffset>").append(result.getFirstOffset()).append("</firstOffset>\n");
        sb.append("  <lastOffset>").append(result.getLastOffset()).append("</lastOffset>\n");
        sb.append("  <partitions>").append(result.getPartitions()).append("</partitions>\n");
        sb.append("  <durationMs>").append(result.getDurationMs()).append("</durationMs>\n");
        sb.append("</kafkaBatchResult>");

        message.setBody(sb.toString());
    }

    /** Functional interface for adding exchange headers to a ProducerRecord. */
    public interface RecordHeaderAdder {
        void addHeaders(ProducerRecord<byte[], byte[]> record, Message message);
    }

    /** Functional interface for custom value/key serialization (e.g. Avro). */
    public interface ByteSerializer {
        byte[] serialize(String topic, String data);
    }
}
