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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles everything that happens to a Kafka record after it is polled:
 * deserialization, validation, processing via Camel pipeline, retry with
 * exponential backoff, DLQ routing, header mapping, and offset commits.
 *
 * Single-threaded: one instance per Consumer, called exclusively from the
 * ScheduledPollConsumer poll thread. No synchronization needed.
 */
final class RecordProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RecordProcessor.class);
    private static final long MAX_RETRY_DELAY_SECONDS = 300; // 5 minutes

    /**
     * Hard bound for the offset commit that runs inside the partition-revocation callback.
     * Kafka does not clamp this commit to the {@code close(Duration)} timer and would
     * otherwise wait for {@code default.api.timeout.ms} (60 s) on an unreachable broker,
     * blocking consumer shutdown/reconnect far beyond its 5 s / 15 s budgets (Issue #49).
     */
    static final Duration REVOKE_COMMIT_TIMEOUT = Duration.ofSeconds(5);

    private static final Class<?> KAFKA_RETRIABLE_EXCEPTION = resolveKafkaRetriableException();

    private static Class<?> resolveKafkaRetriableException() {
        try {
            return Class.forName("org.apache.kafka.common.errors.RetriableException");
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    /**
     * Callback interface to decouple RecordProcessor from the Camel Consumer.
     */
    interface ConsumerCallback {
        /**
         * Process the exchange through the Camel pipeline.
         * MUST throw if exchange.getException() is non-null after processing.
         */
        void processExchange(Exchange exchange) throws Exception;
        void handleException(String message, Exchange exchange, Exception e);
        Exchange createExchange();
    }

    /**
     * Executes the actual Kafka offset commit. Decouples the commit orchestration
     * (tracker bookkeeping + rebalance handling) from the concrete
     * {@code KafkaConsumer.commitSync(...)} call so the orchestration is unit-testable
     * without a live consumer.
     */
    interface CommitAction {
        void commit(Map<TopicPartition, OffsetAndMetadata> offsets);
    }

    private final CpiKafkaPlusEndpoint endpoint;
    private final AdapterTracingHelper tracingHelper;
    private final JsonSchemaValidator jsonSchemaValidator;
    private final DlqProducerHelper dlqHelper;
    private final AvroDeserializerHelper avroHelper;
    private final ConsumerCallback callback;
    private final OffsetCommitTracker offsetTracker;

    RecordProcessor(CpiKafkaPlusEndpoint endpoint,
                    AdapterTracingHelper tracingHelper,
                    JsonSchemaValidator jsonSchemaValidator,
                    DlqProducerHelper dlqHelper,
                    AvroDeserializerHelper avroHelper,
                    ConsumerCallback callback,
                    OffsetCommitTracker offsetTracker) {
        this.endpoint = endpoint;
        this.tracingHelper = tracingHelper;
        this.jsonSchemaValidator = jsonSchemaValidator;
        this.dlqHelper = dlqHelper;
        this.avroHelper = avroHelper;
        this.callback = callback;
        this.offsetTracker = offsetTracker;
    }

    // --- Public API ---

    int processBatchRecords(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                            ConsumerRecords<byte[], byte[]> records,
                            boolean commitAfterSuccess) throws Exception {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> byPartition = groupByPartition(records);

        LOG.debug("[CPI-KAFKA-PLUS-DIAG] processBatch: {} records across {} partition(s)",
                records.count(), byPartition.size());

        IdentityHashMap<byte[], String> cache = new IdentityHashMap<>();
        int[] filterCounts = filterInvalidRecords(kafkaConsumer, byPartition, commitAfterSuccess, cache);
        int schemaValidationFailures = filterCounts[0];
        int dlqCount = filterCounts[1];

        int batchSize = endpoint.getBatchSize();
        int totalProcessed = 0;

        for (Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> entry : byPartition.entrySet()) {
            TopicPartition tp = entry.getKey();
            List<ConsumerRecord<byte[], byte[]>> partitionRecords = entry.getValue();

            LOG.debug("[CPI-KAFKA-PLUS-DIAG] processBatch: partition {} has {} records",
                    tp, partitionRecords.size());

            if (partitionRecords.isEmpty()) {
                continue;
            }

            for (int i = 0; i < partitionRecords.size(); i += batchSize) {
                List<ConsumerRecord<byte[], byte[]>> batch = partitionRecords.subList(
                        i, Math.min(i + batchSize, partitionRecords.size()));

                totalProcessed += processOneBatch(kafkaConsumer, batch, commitAfterSuccess,
                        schemaValidationFailures, dlqCount, cache);
            }
        }

        return totalProcessed;
    }

    /**
     * Pre-filters records that fail JSON Schema validation. Invalid records are routed
     * to DLQ (if enabled), reported to MPL, and removed from the partition lists.
     *
     * @return int[2] with {schemaValidationFailures, dlqCount}
     */
    private int[] filterInvalidRecords(
            KafkaConsumer<byte[], byte[]> kafkaConsumer,
            Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> byPartition,
            boolean commitAfterSuccess,
            IdentityHashMap<byte[], String> cache) {
        if (jsonSchemaValidator == null) {
            return new int[]{0, 0};
        }

        int schemaValidationFailures = 0;
        int dlqCount = 0;

        for (Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> entry : byPartition.entrySet()) {
            List<ConsumerRecord<byte[], byte[]>> partitionRecords = entry.getValue();
            List<ConsumerRecord<byte[], byte[]>> validRecords = new ArrayList<>();

            for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                String value = deserializeValue(record.topic(), record.value(), cache);
                if (value != null) {
                    String validationError = jsonSchemaValidator.validate(value);
                    if (validationError != null) {
                        schemaValidationFailures++;
                        LOG.warn("[CPI-KAFKA-PLUS-DIAG] JSON Schema validation failed for record at offset={} partition={}: {}",
                                record.offset(), record.partition(), validationError);
                        if (endpoint.isJsonSchemaReportError()) {
                            reportValidationErrorToMpl(value, validationError, record);
                        }
                        if (dlqHelper != null) {
                            try {
                                dlqHelper.sendToDlq(record, new RuntimeException(validationError), 0);
                                dlqCount++;
                            } catch (Exception dlqEx) {
                                LOG.error("[CPI-KAFKA-PLUS-DIAG] DLQ send failed for validation-failed record in batch", dlqEx);
                            }
                        }
                        Exchange errorExchange = callback.createExchange();
                        callback.handleException(
                                "JSON Schema validation failed at offset " + record.offset(),
                                errorExchange, new RuntimeException(validationError));
                        if (commitAfterSuccess) {
                            commitSingleOffset(kafkaConsumer, record);
                        }
                        continue;
                    }
                }
                validRecords.add(record);
            }
            entry.setValue(validRecords);
        }

        return new int[]{schemaValidationFailures, dlqCount};
    }

    int processSingleRecords(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                             ConsumerRecords<byte[], byte[]> records,
                             boolean commitAfterSuccess) throws Exception {
        int processedCount = 0;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            processedCount += processRecordWithRetry(kafkaConsumer, record, commitAfterSuccess);
        }
        return processedCount;
    }

    // --- Error classification (package-private for testability) ---

    /**
     * Walks the exception cause chain to determine if the error is transient.
     * Returns true if any exception in the chain is a known network/connection
     * exception type. Returns false for permanent errors (NPE, ClassCastException,
     * FileNotFoundException, etc.) that would fail identically on retry.
     */
    static boolean isRetryable(Exception e) {
        Throwable current = e;
        while (current != null) {
            if (current instanceof java.net.ConnectException
                    || current instanceof java.net.SocketException
                    || current instanceof java.net.SocketTimeoutException
                    || current instanceof java.net.UnknownHostException
                    || current instanceof java.util.concurrent.TimeoutException) {
                return true;
            }
            if (KAFKA_RETRIABLE_EXCEPTION != null
                    && KAFKA_RETRIABLE_EXCEPTION.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Group records by TopicPartition, preserving offset order within each partition.
     * Package-private for testability.
     */
    static Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> groupByPartition(
            ConsumerRecords<byte[], byte[]> records) {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> grouped = new LinkedHashMap<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            List<ConsumerRecord<byte[], byte[]>> list = grouped.get(tp);
            if (list == null) {
                list = new ArrayList<>();
                grouped.put(tp, list);
            }
            list.add(record);
        }
        return grouped;
    }

    // --- Private processing methods ---

    private int processOneBatch(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                List<ConsumerRecord<byte[], byte[]>> batch,
                                boolean commitAfterSuccess,
                                int schemaValidationFailures, int dlqCount,
                                IdentityHashMap<byte[], String> cache) throws Exception {
        Exchange exchange = callback.createExchange();

        String body;
        try {
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] processOneBatch: formatting {} records (partition {})...",
                    batch.size(), batch.get(0).partition());
            body = formatBatch(batch, cache);
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] processOneBatch: formatted OK, bodyLength={}",
                    body != null ? body.length() : 0);
        } catch (Throwable t) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] processOneBatch: formatBatch FAILED: {} ({})",
                    t.getMessage(), t.getClass().getName(), t);
            if (dlqHelper != null) {
                LOG.info("[CPI-KAFKA-PLUS-DIAG] processOneBatch: formatBatch failed, retrying batch records individually for poison-pill isolation");
                return processRecordsIndividually(kafkaConsumer, batch, commitAfterSuccess);
            }
            throw (t instanceof Exception) ? (Exception) t : new RuntimeException(t);
        }

        tracingHelper.traceInbound(exchange, body);

        exchange.getIn().setBody(body);
        setBatchHeaders(exchange.getIn(), batch, schemaValidationFailures, dlqCount,
                body != null ? body.getBytes(StandardCharsets.UTF_8).length : 0);

        try {
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] processOneBatch: calling processor...");
            callback.processExchange(exchange);
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] processOneBatch: process() returned OK");

            if (commitAfterSuccess) {
                commitOffsets(kafkaConsumer, batch);
                LOG.debug("[CPI-KAFKA-PLUS-DIAG] processOneBatch: offsets committed for partition {}",
                        batch.get(0).partition());
            }
            return batch.size();
        } catch (Exception e) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] processOneBatch: EXCEPTION during processing (partition {}): {}",
                    batch.get(0).partition(), e.getMessage(), e);
            if (dlqHelper != null) {
                LOG.info("[CPI-KAFKA-PLUS-DIAG] processOneBatch: DLQ enabled, retrying batch records individually");
                return processRecordsIndividually(kafkaConsumer, batch, commitAfterSuccess);
            }
            callback.handleException(
                    "Error processing batch of " + batch.size() + " Kafka records from partition "
                            + batch.get(0).partition(), exchange, e);
            return 0;
        }
    }

    private int processRecordWithRetry(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                       ConsumerRecord<byte[], byte[]> record,
                                       boolean commitAfterSuccess) {
        String value;
        String key;
        try {
            value = deserializeValue(record.topic(), record.value());
            key = deserializeKey(record.topic(), record.key());
        } catch (Exception deserErr) {
            return handleDeserializationFailure(kafkaConsumer, record, deserErr, commitAfterSuccess);
        }

        if (handleSchemaValidationFailure(kafkaConsumer, record, value, commitAfterSuccess)) {
            return 1;
        }

        int maxRetries = (dlqHelper != null) ? endpoint.getDlqMaxRetries() : 0;
        Exception lastError = null;
        int actualRetries = 0;
        boolean permanentError = false;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            Exchange exchange = callback.createExchange();

            tracingHelper.traceInbound(exchange, value);

            exchange.getIn().setBody(value);
            setSingleRecordHeaders(exchange.getIn(), record, key,
                    value != null ? value.getBytes(StandardCharsets.UTF_8).length : 0);

            try {
                callback.processExchange(exchange);

                if (commitAfterSuccess) {
                    commitSingleOffset(kafkaConsumer, record);
                }
                return 1;
            } catch (Exception e) {
                lastError = e;

                if (endpoint.isRetryOnlyTransientErrors() && !isRetryable(e)) {
                    LOG.info("[CPI-KAFKA-PLUS-DIAG] Permanent error detected, "
                            + "skipping {} remaining retries -> DLQ: offset={} partition={} error='{}'",
                            maxRetries - attempt, record.offset(), record.partition(), e.getMessage());
                    permanentError = true;
                    break;
                }

                actualRetries = attempt;

                if (attempt < maxRetries && !sleepWithBackoff(attempt, record)) {
                    break; // interrupted
                }
            }
        }

        return handleRetryExhausted(kafkaConsumer, record, lastError,
                actualRetries, permanentError, commitAfterSuccess);
    }

    /**
     * Validates a single record against JSON Schema. If validation fails, routes to
     * DLQ (if enabled), reports to MPL, and commits the offset.
     *
     * @return true if validation failed (record should be skipped), false if OK
     */
    private boolean handleSchemaValidationFailure(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                                   ConsumerRecord<byte[], byte[]> record,
                                                   String value, boolean commitAfterSuccess) {
        if (jsonSchemaValidator == null || value == null) {
            return false;
        }
        String validationError = jsonSchemaValidator.validate(value);
        if (validationError == null) {
            return false;
        }

        LOG.warn("[CPI-KAFKA-PLUS-DIAG] JSON Schema validation failed for record at offset={} partition={}: {}",
                record.offset(), record.partition(), validationError);
        if (endpoint.isJsonSchemaReportError()) {
            reportValidationErrorToMpl(value, validationError, record);
        }
        if (dlqHelper != null) {
            try {
                dlqHelper.sendToDlq(record, new RuntimeException(validationError), 0);
            } catch (Exception dlqEx) {
                LOG.error("[CPI-KAFKA-PLUS-DIAG] DLQ send failed for validation-failed record", dlqEx);
            }
        }
        if (commitAfterSuccess) {
            commitSingleOffset(kafkaConsumer, record);
        }
        return true;
    }

    /**
     * Sleeps with exponential backoff between retry attempts.
     *
     * @return true to continue retrying, false if interrupted
     */
    private boolean sleepWithBackoff(int attempt, ConsumerRecord<byte[], byte[]> record) {
        int delaySeconds = endpoint.getRetryDelaySeconds();
        int maxRetries = (dlqHelper != null) ? endpoint.getDlqMaxRetries() : 0;

        if (delaySeconds > 0) {
            int clampedAttempt = Math.min(attempt, 30);
            long backoff = Math.min(
                    (long) delaySeconds * (1L << clampedAttempt),
                    MAX_RETRY_DELAY_SECONDS);
            LOG.info("[CPI-KAFKA-PLUS-DIAG] Transient error, retrying in {}s "
                    + "(attempt {}/{}): offset={} partition={}",
                    backoff, attempt + 1, maxRetries + 1,
                    record.offset(), record.partition());
            try {
                Thread.sleep(backoff * 1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] Retry delay interrupted, "
                        + "aborting retries for offset={} partition={}",
                        record.offset(), record.partition());
                return false;
            }
        } else {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] Record processing failed "
                    + "(attempt {}/{}), retrying: offset={} partition={}",
                    attempt + 1, maxRetries + 1,
                    record.offset(), record.partition());
        }
        return true;
    }

    /**
     * Handles the outcome after all retries are exhausted or a permanent error is detected.
     * Routes to DLQ if available, otherwise delegates to the exception handler.
     */
    private int handleRetryExhausted(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                      ConsumerRecord<byte[], byte[]> record,
                                      Exception lastError, int actualRetries,
                                      boolean permanentError, boolean commitAfterSuccess) {
        String errorType = null;
        if (endpoint.isRetryOnlyTransientErrors()) {
            errorType = permanentError ? "PERMANENT" : "TRANSIENT";
        }

        if (dlqHelper != null) {
            try {
                dlqHelper.sendToDlq(record, lastError, actualRetries, errorType);
                if (commitAfterSuccess) {
                    commitSingleOffset(kafkaConsumer, record);
                }
                return 1;
            } catch (Exception dlqEx) {
                LOG.error("[CPI-KAFKA-PLUS-DIAG] DLQ send failed, delegating to exception handler", dlqEx);
            }
        }

        Exchange errorExchange = callback.createExchange();
        callback.handleException(
                "Error processing Kafka record at offset " + record.offset(), errorExchange, lastError);
        return 0;
    }

    private int processRecordsIndividually(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                           List<ConsumerRecord<byte[], byte[]>> batch,
                                           boolean commitAfterSuccess) {
        int processed = 0;
        for (ConsumerRecord<byte[], byte[]> record : batch) {
            processed += processRecordWithRetry(kafkaConsumer, record, commitAfterSuccess);
        }
        return processed;
    }

    /**
     * Routes a record that failed deserialization (poison-pill) to the DLQ topic
     * with the original raw bytes preserved, then advances past the failing offset
     * by committing it. Without this, the consumer would otherwise loop forever
     * on the same bad offset (KAFKA-16507 win, applied at the adapter layer
     * because the Kafka client itself receives raw bytes via ByteArrayDeserializer).
     *
     * <p>If {@code dlqHelper} is null, or DLQ send itself fails, the original
     * deserialization error is propagated so the caller's reconnect logic kicks in.
     */
    private int handleDeserializationFailure(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                              ConsumerRecord<byte[], byte[]> record,
                                              Exception cause,
                                              boolean commitAfterSuccess) {
        if (dlqHelper == null) {
            throw cause instanceof RuntimeException
                    ? (RuntimeException) cause
                    : new RuntimeException(cause);
        }
        LOG.warn("[CPI-KAFKA-PLUS-DIAG] poison-pill: deserialization failed at topic='{}' partition={} offset={}: {}",
                record.topic(), record.partition(), record.offset(), cause.getMessage());
        try {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            dlqHelper.sendDeserializationFailure(tp, record.offset(),
                    record.key(), record.value(),
                    record.headers(), record.timestamp(), cause);
            if (commitAfterSuccess) {
                commitSingleOffset(kafkaConsumer, record);
            }
            return 1;
        } catch (Exception dlqEx) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] poison-pill: DLQ routing FAILED, propagating original error: dlqExClass={} dlqExMsg='{}'",
                    dlqEx.getClass().getName(), dlqEx.getMessage(), dlqEx);
            throw cause instanceof RuntimeException
                    ? (RuntimeException) cause
                    : new RuntimeException(cause);
        }
    }

    // --- Formatting and deserialization ---

    private String formatBatch(List<ConsumerRecord<byte[], byte[]>> batch,
                               final IdentityHashMap<byte[], String> cache) throws Exception {
        java.util.function.BiFunction<String, byte[], String> cachedDeserializer =
                new java.util.function.BiFunction<String, byte[], String>() {
                    @Override
                    public String apply(String topic, byte[] data) {
                        return deserializeValue(topic, data, cache);
                    }
                };
        String format = endpoint.getBatchOutputFormat();
        if ("XML_LIST".equalsIgnoreCase(format)) {
            return BatchFormatter.toXml(batch, this::deserializeKey, cachedDeserializer, true);
        }
        return BatchFormatter.toJsonArray(batch, this::deserializeKey, cachedDeserializer);
    }

    String deserializeValue(String topic, byte[] data) {
        return deserializeValue(topic, data, null);
    }

    private String deserializeValue(String topic, byte[] data,
                                     IdentityHashMap<byte[], String> cache) {
        if (data == null) return null;
        if (cache != null && cache.containsKey(data)) {
            return cache.get(data);
        }
        String result;
        if (endpoint.isSchemaRegistryEnabled() && endpoint.isAvroValueDeserialization() && avroHelper != null) {
            result = avroHelper.deserialize(topic, data);
        } else {
            result = new String(data, StandardCharsets.UTF_8);
        }
        if (cache != null) {
            cache.put(data, result);
        }
        return result;
    }

    String deserializeKey(String topic, byte[] data) {
        if (data == null) return null;
        return new String(data, StandardCharsets.UTF_8);
    }

    // --- Header mapping ---

    /**
     * @param payloadSize size of the formatted Camel body in UTF-8 bytes
     */
    void setBatchHeaders(Message message, List<ConsumerRecord<byte[], byte[]>> batch,
                         int schemaValidationFailures, int dlqCount, int payloadSize) {
        message.setHeader("SAP_Sender", endpoint.getEffectiveTopic());
        message.setHeader("CpiKafkaPlusRecordCount", batch.size());
        message.setHeader("CpiKafkaPlusPayloadSize", payloadSize);
        message.setHeader("CpiKafkaPlusTopic", endpoint.getEffectiveTopic());
        message.setHeader("CpiKafkaPlusBatchOutputFormat", endpoint.getBatchOutputFormat());
        message.setHeader("CpiKafkaPlusConsumerGroup", endpoint.getGroupId());
        message.setHeader("CpiKafkaPlusCommitStrategy", endpoint.getCommitStrategy());
        if (!batch.isEmpty()) {
            message.setHeader("CpiKafkaPlusFirstOffset", batch.get(0).offset());
            message.setHeader("CpiKafkaPlusLastOffset", batch.get(batch.size() - 1).offset());
            message.setHeader("CpiKafkaPlusPartition", batch.get(0).partition());
        }
        if (endpoint.isDlqEnabled()) {
            message.setHeader("CpiKafkaPlusDlqCount", dlqCount);
        }
        if (endpoint.isJsonSchemaValidation()) {
            message.setHeader("CpiKafkaPlusSchemaValidationFailures", schemaValidationFailures);
        }
    }

    /**
     * @param payloadSize size of the deserialized Camel body in UTF-8 bytes
     */
    void setSingleRecordHeaders(Message message, ConsumerRecord<byte[], byte[]> record, String key, int payloadSize) {
        message.setHeader("SAP_Sender", record.topic());
        message.setHeader("CpiKafkaPlusTopic", record.topic());
        message.setHeader("CpiKafkaPlusPayloadSize", payloadSize);
        message.setHeader("CpiKafkaPlusPartition", record.partition());
        message.setHeader("CpiKafkaPlusOffset", record.offset());
        message.setHeader("CpiKafkaPlusKey", key);
        message.setHeader("CpiKafkaPlusTimestamp", record.timestamp());
        message.setHeader("CpiKafkaPlusConsumerGroup", endpoint.getGroupId());
        message.setHeader("CpiKafkaPlusCommitStrategy", endpoint.getCommitStrategy());
        if (record.headers() != null) {
            record.headers().forEach(header -> {
                String headerName = "kafka.header." + header.key();
                message.setHeader(headerName, header.value() != null ? new String(header.value(), StandardCharsets.UTF_8) : null);
            });
        }
    }

    // --- Offset management ---

    private void commitOffsets(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                               List<ConsumerRecord<byte[], byte[]>> batch) {
        for (ConsumerRecord<byte[], byte[]> record : batch) {
            offsetTracker.markProcessed(
                    new TopicPartition(record.topic(), record.partition()), record.offset());
        }
        boolean committed = commitTracked(offsets -> kafkaConsumer.commitSync(offsets),
                "batch commit, records=" + batch.size());
        LOG.debug("Batch commit attempt after successful processing: committed={} pendingRemaining={}",
                committed, !offsetTracker.isEmpty());
    }

    private void commitSingleOffset(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                    ConsumerRecord<byte[], byte[]> record) {
        offsetTracker.markProcessed(
                new TopicPartition(record.topic(), record.partition()), record.offset());
        commitTracked(offsets -> kafkaConsumer.commitSync(offsets),
                "single offset commit, topic=" + record.topic()
                        + " partition=" + record.partition()
                        + " offset=" + record.offset());
    }

    /**
     * Commits all offsets currently pending in the {@link OffsetCommitTracker}. On a
     * successful commit the committed offsets are cleared from the tracker; on a
     * rebalance-in-progress failure they are <em>retained</em> so the next cycle (or a
     * partition-revocation) re-commits them. This is the fix for the phantom-lag /
     * lost-commit bug where a swallowed commit left the committed offset behind the
     * actually-processed position.
     *
     * <p>Tracker lifecycle guarantee: a pending offset is retained only until it is
     * committed, its partition is revoked ({@link #commitOnRevoke}), or its partition is
     * lost ({@link #dropLost}). It is never carried across a consumer reconnect — a fresh
     * tracker is built on re-initialization, matching the fresh partition assignment.
     *
     * @return {@code true} if the commit went through, {@code false} if it was skipped
     *         due to an in-progress rebalance
     */
    boolean commitTracked(CommitAction action, String context) {
        if (offsetTracker.isEmpty()) {
            return true;
        }
        Map<TopicPartition, OffsetAndMetadata> snapshot = offsetTracker.snapshot();
        boolean committed = commitWithRebalanceHandling(() -> action.commit(snapshot), context);
        if (committed) {
            offsetTracker.confirm(snapshot);
        }
        return committed;
    }

    /**
     * Commits only the pending offsets for {@code partitions}. Used from the rebalance
     * listener's {@code onPartitionsRevoked} callback to durably commit processed work
     * while the partitions are still owned, before they are handed to another member.
     */
    boolean commitTrackedFor(CommitAction action, Collection<TopicPartition> partitions,
                             String context) {
        Map<TopicPartition, OffsetAndMetadata> snapshot = offsetTracker.snapshotFor(partitions);
        if (snapshot.isEmpty()) {
            return true;
        }
        boolean committed = commitWithRebalanceHandling(() -> action.commit(snapshot), context);
        if (committed) {
            offsetTracker.confirm(snapshot);
        }
        return committed;
    }

    /**
     * Re-attempts any offset commit that a previous cycle could not complete because a
     * rebalance was in progress. Called at the start of each emit cycle, after
     * {@code poll()} has driven the rebalance to completion.
     */
    void recommitPending(KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        if (offsetTracker.isEmpty()) {
            return;
        }
        boolean committed = commitTracked(offsets -> kafkaConsumer.commitSync(offsets),
                "re-commit pending offsets");
        if (committed) {
            LOG.info("[CPI-KAFKA-PLUS-DIAG] re-committed previously-pending offsets after rebalance");
        }
    }

    /**
     * Commits pending offsets for partitions being revoked in a rebalance, then
     * unconditionally drops them from the tracker.
     *
     * <p>Revocation is an ownership boundary: after it returns this consumer no longer
     * owns {@code revoked}, so it must never commit those offsets again. A later commit
     * would carry a stale position and rewind the group offset the new owner has already
     * advanced (Issue #49 — mass re-delivery / poisoned future commits). The pending
     * entries are therefore dropped whether the revoke commit succeeded, was skipped for
     * a rebalance, or failed outright; uncommitted records are redelivered to the new
     * owner under the at-least-once contract.
     *
     * <p>The commit runs inside Kafka's rebalance callback, from which any escaping
     * exception is re-thrown as a {@link org.apache.kafka.common.KafkaException} out of
     * {@code poll()} — turning a routine rebalance into a reported connection error. All
     * {@code KafkaException}s (e.g. {@code TimeoutException} on an unreachable broker) are
     * therefore swallowed here; the {@code finally} drop still runs.
     *
     * @param action  the concrete commit call (the caller applies its own commit timeout)
     * @param revoked the partitions being revoked; {@code null} is a no-op
     */
    void commitOnRevoke(CommitAction action, Collection<TopicPartition> revoked) {
        if (revoked == null) {
            return;
        }
        try {
            commitTrackedFor(action, revoked, "revoke commit, partitions=" + revoked.size());
        } catch (org.apache.kafka.common.KafkaException e) {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] revoke commit failed ({}): {} -- offsets for revoked "
                    + "partitions dropped, records will be redelivered to the new owner",
                    e.getClass().getSimpleName(), e.getMessage());
        } finally {
            Map<TopicPartition, OffsetAndMetadata> stillPending = offsetTracker.snapshotFor(revoked);
            if (!stillPending.isEmpty()) {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] dropping {} uncommitted offset(s) for revoked "
                        + "partitions (commit did not confirm them): {}",
                        stillPending.size(), stillPending);
            }
            offsetTracker.drop(revoked);
        }
    }

    /**
     * Drops pending offsets for partitions that were lost (not cleanly revoked) in a
     * rebalance. They cannot be committed from this consumer and will be redelivered to
     * whichever member now owns them.
     */
    void dropLost(Collection<TopicPartition> lost) {
        if (lost != null) {
            offsetTracker.drop(lost);
        }
    }

    /**
     * Runs a Kafka commit operation and treats a commit that fails because a
     * rebalance is in progress as a benign signal. The records that triggered
     * this commit attempt will be redelivered to (potentially another) consumer
     * after the rebalance completes, so there is nothing useful to do here other
     * than log it. Without this guard, the exception bubbles up into the generic
     * batch error handler, which then incorrectly routes the records to DLQ
     * individually -- causing the cascade observed in issue #45 (one rebalance
     * triggers a multi-record DLQ storm with each retry also failing on commit).
     *
     * <p>Two distinct exceptions signal this condition and both are swallowed:
     * <ul>
     *   <li>{@link CommitFailedException} -- the group already rebalanced and the
     *       partition was reassigned to another member (eager rebalance).</li>
     *   <li>{@link RebalanceInProgressException} -- {@code commitSync()} was called
     *       while a rebalance is still in progress. This is the common case with
     *       the {@code CooperativeStickyAssignor}, whose incremental rebalance
     *       spans multiple poll cycles.</li>
     * </ul>
     *
     * <p>All other exceptions are propagated unchanged so genuine commit failures
     * (auth, network, broker errors) keep their existing behavior.
     *
     * <p>Package-private and static for direct unit testing.
     *
     * @return {@code true} if the commit ran without a rebalance-related failure,
     *         {@code false} if it was skipped because a rebalance was in progress
     *         (the caller keeps the offsets pending for a later re-commit)
     */
    static boolean commitWithRebalanceHandling(Runnable commitOp, String context) {
        try {
            commitOp.run();
            return true;
        } catch (CommitFailedException | RebalanceInProgressException e) {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] commitSync skipped because a rebalance is in progress: {} -- {}. "
                    + "Offsets retained and will be re-committed after the rebalance completes.",
                    context, e.getMessage());
            return false;
        }
    }

    // --- MPL error reporting ---

    private void reportValidationErrorToMpl(String value, String validationError,
                                            ConsumerRecord<byte[], byte[]> record) {
        String errorMsg = "JSON Schema validation failed for record at topic=" + record.topic()
                + " partition=" + record.partition() + " offset=" + record.offset()
                + ": " + validationError;
        try {
            Exchange mplExchange = callback.createExchange();
            mplExchange.getIn().setBody(value);
            mplExchange.getIn().setHeader("CpiKafkaPlusTopic", record.topic());
            mplExchange.getIn().setHeader("CpiKafkaPlusPartition", record.partition());
            mplExchange.getIn().setHeader("CpiKafkaPlusOffset", record.offset());
            mplExchange.getIn().setHeader("SAP_Sender", record.topic());
            tracingHelper.traceInbound(mplExchange, value);
            mplExchange.setProperty(Exchange.ROUTE_STOP, Boolean.TRUE);
            mplExchange.setException(new RuntimeException(errorMsg));
            callback.processExchange(mplExchange);
        } catch (Exception e) {
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] reportValidationErrorToMpl failed for offset={}: {}",
                    record.offset(), e.getMessage());
        }
    }
}
