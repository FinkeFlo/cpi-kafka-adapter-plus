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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.WakeupException;
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

        /** @return true once the consumer is stopping; no further record is started then */
        default boolean isStopRequested() {
            return false;
        }
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

    int processBatchRecords(Consumer<byte[], byte[]> kafkaConsumer,
                            ConsumerRecords<byte[], byte[]> records,
                            boolean commitAfterSuccess, PollProgress progress) throws Exception {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> byPartition = groupByPartition(records);

        LOG.debug("[CPI-KAFKA-PLUS-DIAG] processBatch: {} records across {} partition(s)",
                records.count(), byPartition.size());

        IdentityHashMap<byte[], String> cache = new IdentityHashMap<>();
        int[] filterCounts = filterInvalidRecords(kafkaConsumer, byPartition, cache, progress);
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
                // A block set by the schema filter still lets the valid records before it run.
                Long blockedAt = progress.blockedOffset(tp);
                if (stopRequested(progress) || (blockedAt != null && batch.get(0).offset() >= blockedAt)) {
                    break;
                }

                totalProcessed += processOneBatch(kafkaConsumer, batch, commitAfterSuccess,
                        schemaValidationFailures, dlqCount, cache, progress);
            }
        }

        // Records the schema filter resolved after the last batch of a partition are only
        // committable now, once everything before them is resolved as well.
        if (commitAfterSuccess) {
            for (TopicPartition tp : byPartition.keySet()) {
                if (progress.isStopped()) {
                    markProgress(tp, progress);
                } else {
                    commitProgress(kafkaConsumer, tp, progress);
                }
            }
        }
        return totalProcessed;
    }

    /**
     * Pre-filters records that fail JSON Schema validation. Invalid records are routed
     * to DLQ (if enabled), reported to MPL, and removed from the partition lists.
     *
     * <p>Nothing is committed here: the filter runs before any batch of the poll, and committing an
     * invalid record's offset would commit past the earlier records of its partition before they
     * were processed (#173). The filtered records are resolved in {@code progress} and committed
     * together with the batches around them. A record whose DLQ write fails blocks its partition:
     * only the valid records before it stay in the list.
     *
     * @return int[2] with {schemaValidationFailures, dlqCount}
     */
    private int[] filterInvalidRecords(
            Consumer<byte[], byte[]> kafkaConsumer,
            Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> byPartition,
            IdentityHashMap<byte[], String> cache,
            PollProgress progress) {
        if (jsonSchemaValidator == null) {
            return new int[]{0, 0};
        }

        int schemaValidationFailures = 0;
        int dlqCount = 0;

        for (Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> entry : byPartition.entrySet()) {
            TopicPartition tp = entry.getKey();
            List<ConsumerRecord<byte[], byte[]>> partitionRecords = entry.getValue();
            List<ConsumerRecord<byte[], byte[]>> validRecords = new ArrayList<>();

            for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                progress.processing(tp, record.offset());
                String value;
                try {
                    value = deserializeValue(record.topic(), record.value(), cache);
                } catch (Exception deserErr) {
                    // Committed with the batches around it, never from here.
                    handleDeserializationFailure(kafkaConsumer, record, deserErr, false, progress);
                    if (progress.isBlocked(tp)) {
                        break;
                    }
                    continue;
                }
                if (value != null) {
                    String validationError = jsonSchemaValidator.validate(value);
                    if (validationError != null) {
                        schemaValidationFailures++;
                        LOG.warn("[CPI-KAFKA-PLUS-DIAG] JSON Schema validation failed for record at offset={} partition={}: {}",
                                record.offset(), record.partition(), validationError);
                        if (endpoint.isJsonSchemaReportError()) {
                            reportValidationErrorToMpl(value, validationError, record);
                        }
                        boolean dlqFailed = false;
                        if (dlqHelper != null) {
                            try {
                                dlqHelper.sendToDlq(record, new RuntimeException(validationError), 0);
                                dlqCount++;
                            } catch (Exception dlqEx) {
                                AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("dlq.send.failed")
                                        .with("trigger", "schemaValidation")
                                        .with("mode", "batch")
                                        .with("topic", record.topic())
                                        .with("partition", record.partition())
                                        .with("offset", record.offset())
                                        .with("dlqTopic", endpoint.getDlqTopic())
                                        .with("consequence", "offset not committed, record will be retried"), dlqEx);
                                dlqFailed = true;
                            }
                        }
                        Exchange errorExchange = callback.createExchange();
                        callback.handleException(
                                "JSON Schema validation failed at offset " + record.offset(),
                                errorExchange, new RuntimeException(validationError));
                        if (dlqFailed) {
                            progress.blocked(tp, record.offset());
                            break;
                        }
                        progress.resolved(tp, record.offset());
                        continue;
                    }
                }
                validRecords.add(record);
            }
            entry.setValue(validRecords);
        }

        return new int[]{schemaValidationFailures, dlqCount};
    }

    int processSingleRecords(Consumer<byte[], byte[]> kafkaConsumer,
                             ConsumerRecords<byte[], byte[]> records,
                             boolean commitAfterSuccess, PollProgress progress) throws Exception {
        int processedCount = 0;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            if (stopRequested(progress)) {
                break;
            }
            if (progress.isBlocked(partitionOf(record))) {
                continue; // nothing after a record that has to be retried
            }
            processedCount += processRecordWithRetry(kafkaConsumer, record, commitAfterSuccess, false, progress);
        }
        return processedCount;
    }

    /**
     * Seeks every partition of the poll back to its first unresolved record. {@code poll()} has
     * already moved the position past all of them; without the seek they would be skipped — and
     * with {@code commitStrategy=AUTO} even committed by the next auto-commit.
     */
    void rewind(Consumer<byte[], byte[]> kafkaConsumer, PollProgress progress) {
        for (Map.Entry<TopicPartition, Long> e : progress.rewindPositions().entrySet()) {
            try {
                kafkaConsumer.seek(e.getKey(), e.getValue());
                LOG.debug("[CPI-KAFKA-PLUS-DIAG] rewind: partition {} back to offset {}", e.getKey(), e.getValue());
            } catch (RuntimeException seekError) {
                // Typically a partition revoked during this poll; its new owner starts from the
                // committed offset, which never passes an unresolved record.
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] rewind: could not seek partition {} back to offset {}: {}",
                        e.getKey(), e.getValue(), seekError.getMessage());
            }
        }
    }

    private boolean stopRequested(PollProgress progress) {
        if (!progress.isStopped() && callback.isStopRequested()) {
            progress.stop();
        }
        return progress.isStopped();
    }

    private static TopicPartition partitionOf(ConsumerRecord<byte[], byte[]> record) {
        return new TopicPartition(record.topic(), record.partition());
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

    private int processOneBatch(Consumer<byte[], byte[]> kafkaConsumer,
                                List<ConsumerRecord<byte[], byte[]>> batch,
                                boolean commitAfterSuccess,
                                int schemaValidationFailures, int dlqCount,
                                IdentityHashMap<byte[], String> cache,
                                PollProgress progress) throws Exception {
        TopicPartition tp = partitionOf(batch.get(0));
        progress.processing(tp, batch.get(0).offset());
        Exchange exchange = callback.createExchange();

        String body;
        try {
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] processOneBatch: formatting {} records (partition {})...",
                    batch.size(), batch.get(0).partition());
            body = formatBatch(batch, cache);
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] processOneBatch: formatted OK, bodyLength={}",
                    body != null ? body.length() : 0);
        } catch (Throwable t) {
            int partition = !batch.isEmpty() ? batch.get(0).partition() : -1;
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("batch.format.failed")
                    .with("errorCode", CpiKafkaPlusErrorCode.fromThrowable(t).code())
                    .with("partition", partition)
                    .with("batchSize", batch.size()), t);
            if (dlqHelper != null) {
                LOG.info("[CPI-KAFKA-PLUS-DIAG] processOneBatch: formatBatch failed, retrying batch records individually for poison-pill isolation");
                return processRecordsIndividually(kafkaConsumer, batch, commitAfterSuccess, progress);
            }
            if (!(t instanceof Exception)) {
                throw new RuntimeException(t); // an Error is not a data problem; the poll rewinds it
            }
            progress.failed();
            callback.handleException(
                    "Could not format batch of " + batch.size() + " Kafka records from partition "
                            + batch.get(0).partition(), exchange, (Exception) t);
            resolveBatchWithoutDlq(kafkaConsumer, batch, commitAfterSuccess, progress);
            return 0;
        }

        tracingHelper.traceInbound(exchange, body);

        exchange.getIn().setBody(body);
        setBatchHeaders(exchange.getIn(), batch, schemaValidationFailures, dlqCount,
                body != null ? body.getBytes(StandardCharsets.UTF_8).length : 0);

        try {
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] processOneBatch: calling processor...");
            callback.processExchange(exchange);
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] processOneBatch: process() returned OK");
        } catch (Exception e) {
            int partition = !batch.isEmpty() ? batch.get(0).partition() : -1;
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("batch.processing.failed")
                    .with("errorCode", CpiKafkaPlusErrorCode.fromThrowable(e).code())
                    .with("partition", partition)
                    .with("batchSize", batch.size()), e);
            if (dlqHelper != null) {
                LOG.info("[CPI-KAFKA-PLUS-DIAG] processOneBatch: DLQ enabled, retrying batch records individually");
                return processRecordsIndividually(kafkaConsumer, batch, commitAfterSuccess, progress);
            }
            progress.failed();
            callback.handleException(
                    "Error processing batch of " + batch.size() + " Kafka records from partition "
                            + batch.get(0).partition(), exchange, e);
            resolveBatchWithoutDlq(kafkaConsumer, batch, commitAfterSuccess, progress);
            return 0;
        }

        // Outside the route's try block: a failing commit must not send a processed batch through
        // the individual DLQ fallback (#172).
        for (ConsumerRecord<byte[], byte[]> record : batch) {
            progress.resolved(tp, record.offset());
        }
        progress.succeeded();
        if (commitAfterSuccess) {
            commitProgress(kafkaConsumer, tp, progress);
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] processOneBatch: offsets committed for partition {}", tp);
        }
        return batch.size();
    }

    /** Batch counterpart of {@link #resolveWithoutDlq}. */
    private void resolveBatchWithoutDlq(Consumer<byte[], byte[]> kafkaConsumer,
                                        List<ConsumerRecord<byte[], byte[]>> batch,
                                        boolean commitAfterSuccess, PollProgress progress) {
        TopicPartition tp = partitionOf(batch.get(0));
        if (!endpoint.isSkipFailedMessages()) {
            progress.blocked(tp, batch.get(0).offset());
            return;
        }
        for (ConsumerRecord<byte[], byte[]> record : batch) {
            progress.resolved(tp, record.offset());
        }
        if (commitAfterSuccess) {
            commitProgress(kafkaConsumer, tp, progress);
        }
    }

    private int processRecordWithRetry(Consumer<byte[], byte[]> kafkaConsumer,
                                       ConsumerRecord<byte[], byte[]> record,
                                       boolean commitAfterSuccess,
                                       boolean batchFallback,
                                       PollProgress progress) {
        TopicPartition tp = partitionOf(record);
        progress.processing(tp, record.offset());
        String value;
        String key;
        try {
            value = deserializeValue(record.topic(), record.value());
            key = deserializeKey(record.topic(), record.key());
        } catch (Exception deserErr) {
            return handleDeserializationFailure(kafkaConsumer, record, deserErr, commitAfterSuccess, progress);
        }

        if (handleSchemaValidationFailure(kafkaConsumer, record, value, commitAfterSuccess, progress)) {
            return progress.isBlocked(tp) ? 0 : 1;
        }

        int maxRetries = (dlqHelper != null) ? endpoint.getDlqMaxRetries() : 0;
        Exception lastError = null;
        int actualRetries = 0;
        boolean permanentError = false;

        String body = null;
        if (batchFallback) {
            List<ConsumerRecord<byte[], byte[]>> singletonBatch = java.util.Collections.singletonList(record);
            try {
                body = formatBatch(singletonBatch, null);
            } catch (Exception formatErr) {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] processRecordWithRetry: formatBatch failed in fallback mode, "
                        + "falling back to raw value for offset={} partition={}: {}",
                        record.offset(), record.partition(), formatErr.getMessage());
                body = value;
            }
        }
        final String resolvedBody = batchFallback ? body : value;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            Exchange exchange = callback.createExchange();

            tracingHelper.traceInbound(exchange, resolvedBody);

            exchange.getIn().setBody(resolvedBody);
            if (batchFallback) {
                List<ConsumerRecord<byte[], byte[]>> singletonBatch = java.util.Collections.singletonList(record);
                setBatchHeaders(exchange.getIn(), singletonBatch, 0, 0,
                        resolvedBody != null ? resolvedBody.getBytes(StandardCharsets.UTF_8).length : 0);
                exchange.getIn().setHeader("CpiKafkaPlusFallbackMode", true);
                exchange.getIn().setHeader("CpiKafkaPlusKey", key);
            } else {
                setSingleRecordHeaders(exchange.getIn(), record, key,
                        value != null ? value.getBytes(StandardCharsets.UTF_8).length : 0);
            }

            try {
                callback.processExchange(exchange);
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

                if (attempt < maxRetries
                        && (stopRequested(progress) || !sleepWithBackoff(attempt, record))) {
                    // A shutdown, not a poison record: leave it unresolved for the next run
                    // instead of dead-lettering a record that may well succeed.
                    progress.stop();
                    return 0;
                }
                continue;
            }

            // Deliberately outside the route's try block: a failing commit is not a failing
            // record and must neither re-run the route nor reach the DLQ (#172).
            progress.resolved(tp, record.offset());
            progress.succeeded();
            if (commitAfterSuccess) {
                commitProgress(kafkaConsumer, tp, progress);
            }
            return 1;
        }

        progress.failed();
        return handleRetryExhausted(kafkaConsumer, record, lastError,
                actualRetries, permanentError, commitAfterSuccess, progress);
    }

    /**
     * Validates a single record against JSON Schema. If validation fails, routes to
     * DLQ (if enabled), reports to MPL, and commits the offset.
     *
     * @return true if validation failed (record should be skipped), false if OK
     */
    private boolean handleSchemaValidationFailure(Consumer<byte[], byte[]> kafkaConsumer,
                                                   ConsumerRecord<byte[], byte[]> record,
                                                   String value, boolean commitAfterSuccess,
                                                   PollProgress progress) {
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
        TopicPartition tp = partitionOf(record);
        if (dlqHelper != null) {
            try {
                dlqHelper.sendToDlq(record, new RuntimeException(validationError), 0);
            } catch (Exception dlqEx) {
                AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("dlq.send.failed")
                        .with("trigger", "schemaValidation")
                        .with("mode", "single")
                        .with("topic", record.topic())
                        .with("partition", record.partition())
                        .with("offset", record.offset())
                        .with("dlqTopic", endpoint.getDlqTopic())
                        .with("consequence", "offset not committed, record will be retried"), dlqEx);
                progress.blocked(tp, record.offset());
                return true;
            }
        }
        // Without a DLQ an invalid record is dropped, as documented for JSON Schema validation.
        progress.resolved(tp, record.offset());
        if (commitAfterSuccess) {
            commitProgress(kafkaConsumer, tp, progress);
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
    private int handleRetryExhausted(Consumer<byte[], byte[]> kafkaConsumer,
                                      ConsumerRecord<byte[], byte[]> record,
                                      Exception lastError, int actualRetries,
                                      boolean permanentError, boolean commitAfterSuccess,
                                      PollProgress progress) {
        TopicPartition tp = partitionOf(record);
        String errorType = null;
        if (endpoint.isRetryOnlyTransientErrors()) {
            errorType = permanentError ? "PERMANENT" : "TRANSIENT";
        }

        java.util.Map<String, String> context = new java.util.LinkedHashMap<>();
        context.put("topic", record.topic());
        context.put("partition", String.valueOf(record.partition()));
        context.put("offset", String.valueOf(record.offset()));
        context.put("retryAttempts", String.valueOf(actualRetries));
        context.put("errorType", errorType != null ? errorType : "UNKNOWN");
        String errorCode = CpiKafkaPlusErrorCode.fromThrowable(lastError).code();

        if (dlqHelper != null) {
            try {
                dlqHelper.sendToDlq(record, lastError, actualRetries, errorType);
                java.util.Map<String, String> dlqContext = new java.util.LinkedHashMap<>(context);
                dlqContext.put("dlqOutcome", "MOVED");
                dlqContext.put("dlqTopic", endpoint.getDlqTopic());
                Exchange traceExchange = callback.createExchange();
                tracingHelper.traceError(traceExchange, lastError, dlqContext, true);
                tracingHelper.reportFailure(traceExchange, lastError, errorCode, dlqContext, true);
                progress.resolved(tp, record.offset());
                if (commitAfterSuccess) {
                    commitProgress(kafkaConsumer, tp, progress);
                }
                return 1;
            } catch (Exception dlqEx) {
                // Without a DLQ write the record is not resolved: no commit passes it, the
                // partition is rewound and the record is retried after a backoff, until the
                // cause (missing topic, missing ACL, broker down) is removed.
                progress.blocked(tp, record.offset());
                AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("dlq.send.failed")
                        .with("trigger", "retryExhausted")
                        .with("topic", record.topic())
                        .with("partition", record.partition())
                        .with("offset", record.offset())
                        .with("dlqTopic", endpoint.getDlqTopic())
                        .with("retryAttempts", actualRetries)
                        .with("errorType", errorType != null ? errorType : "UNKNOWN")
                        .with("originalError", CpiKafkaPlusErrorCode.fromThrowable(lastError).code())
                        .with("consequence", "offset not committed, record will be retried"), dlqEx);

                java.util.Map<String, String> dlqContext = new java.util.LinkedHashMap<>(context);
                dlqContext.put("dlqOutcome", "SEND_FAILED");
                dlqContext.put("dlqTopic", endpoint.getDlqTopic());
                Exchange traceExchange = callback.createExchange();
                tracingHelper.traceError(traceExchange, lastError, dlqContext, true);
                tracingHelper.reportFailure(traceExchange, lastError, errorCode, dlqContext, true);
            }
        } else {
            Exchange traceExchange = callback.createExchange();
            tracingHelper.traceError(traceExchange, lastError, context, true);
            tracingHelper.reportFailure(traceExchange, lastError, errorCode, context, true);
            resolveWithoutDlq(kafkaConsumer, record, commitAfterSuccess, progress);
        }

        Exchange errorExchange = callback.createExchange();
        callback.handleException(
                "Error processing Kafka record at offset " + record.offset(), errorExchange, lastError);
        return 0;
    }

    /**
     * What happens to a failed record when there is no DLQ, as chosen by {@code errorHandling}:
     * Retry Failed Message (default) blocks the partition at the record so it is retried after a
     * backoff; Skip Failed Message resolves it (at-most-once), so the next commit may pass it.
     */
    private void resolveWithoutDlq(Consumer<byte[], byte[]> kafkaConsumer,
                                   ConsumerRecord<byte[], byte[]> record,
                                   boolean commitAfterSuccess, PollProgress progress) {
        TopicPartition tp = partitionOf(record);
        if (!endpoint.isSkipFailedMessages()) {
            progress.blocked(tp, record.offset());
            return;
        }
        progress.resolved(tp, record.offset());
        if (commitAfterSuccess) {
            commitProgress(kafkaConsumer, tp, progress);
        }
    }

    int processRecordsIndividually(Consumer<byte[], byte[]> kafkaConsumer,
                                   List<ConsumerRecord<byte[], byte[]>> batch,
                                   boolean commitAfterSuccess, PollProgress progress) {
        int processed = 0;
        for (ConsumerRecord<byte[], byte[]> record : batch) {
            if (stopRequested(progress) || progress.isBlocked(partitionOf(record))) {
                break;
            }
            processed += processRecordWithRetry(kafkaConsumer, record, commitAfterSuccess, true, progress);
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
     * <p>Without a DLQ the record is handled like any other failed record without a DLQ. If the
     * DLQ write fails, the partition is blocked at the record and it is retried later. The error no
     * longer escapes the poll: that skipped every remaining record of the poll as well.
     */
    private int handleDeserializationFailure(Consumer<byte[], byte[]> kafkaConsumer,
                                              ConsumerRecord<byte[], byte[]> record,
                                              Exception cause,
                                              boolean commitAfterSuccess,
                                              PollProgress progress) {
        // f2: Trace the deserialization failure for consumer/sender direction
        Exchange traceExchange = callback.createExchange();
        java.util.Map<String, String> context = new java.util.LinkedHashMap<>();
        context.put("topic", record.topic());
        context.put("partition", String.valueOf(record.partition()));
        context.put("offset", String.valueOf(record.offset()));
        context.put("failureType", "DESERIALIZATION");
        tracingHelper.traceError(traceExchange, cause, context, true);

        // f1/f4/f5: Report failure with structured fields and attachment
        // Deserialization failures map to KP_PROD_004 (serialization_failed) via the central taxonomy
        String errorCode = CpiKafkaPlusErrorCode.fromThrowable(cause).code();
        tracingHelper.reportFailure(traceExchange, cause, errorCode, context, true);
        progress.failed();

        TopicPartition tp = partitionOf(record);
        if (dlqHelper == null) {
            resolveWithoutDlq(kafkaConsumer, record, commitAfterSuccess, progress);
            callback.handleException("Could not deserialize Kafka record at offset " + record.offset(),
                    callback.createExchange(), cause);
            return 0;
        }
        LOG.warn("[CPI-KAFKA-PLUS-DIAG] poison-pill: deserialization failed at topic='{}' partition={} offset={}: {}",
                record.topic(), record.partition(), record.offset(), cause.getMessage());
        try {
            dlqHelper.sendDeserializationFailure(tp, record.offset(),
                    record.key(), record.value(),
                    record.headers(), record.timestamp(), cause);
            progress.resolved(tp, record.offset());
            if (commitAfterSuccess) {
                commitProgress(kafkaConsumer, tp, progress);
            }
            return 1;
        } catch (Exception dlqEx) {
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("dlq.send.failed")
                    .with("trigger", "deserialization")
                    .with("topic", record.topic())
                    .with("partition", record.partition())
                    .with("offset", record.offset())
                    .with("dlqTopic", endpoint.getDlqTopic())
                    .with("consequence", "offset not committed, record will be retried"), dlqEx);
            progress.blocked(tp, record.offset());
            callback.handleException("Could not deserialize Kafka record at offset " + record.offset(),
                    callback.createExchange(), cause);
            return 0;
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
            // Auto-detect is always on: values that look like XML are embedded as child
            // elements, everything else falls back to CDATA text (see BatchFormatter).
            // There is intentionally no endpoint property to toggle this.
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
        // A batch is formed per partition, so all its records share one topic. The configured topic
        // may be a comma-separated subscription, which is not the topic these records came from.
        String topic = batch.isEmpty() ? endpoint.getEffectiveTopic() : batch.get(0).topic();
        message.setHeader("SAP_Sender", topic);
        message.setHeader("CpiKafkaPlusRecordCount", batch.size());
        message.setHeader("CpiKafkaPlusPayloadSize", payloadSize);
        message.setHeader("CpiKafkaPlusTopic", topic);
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

    /**
     * Commits {@code tp} up to its first unresolved record of this poll.
     *
     * <p>A wakeup or interrupt during the commit means the consumer is stopping: processing stops,
     * the offset stays pending and is committed on revoke or by the next run. It is never treated as
     * a failure of the record that was just processed (#172).
     */
    private void commitProgress(Consumer<byte[], byte[]> kafkaConsumer, TopicPartition tp,
                                PollProgress progress) {
        long next = progress.commitOffset(tp);
        if (next < 0 || next <= progress.committedUpTo(tp)) {
            return; // nothing new since the last commit of this partition
        }
        offsetTracker.markProcessed(tp, next - 1);
        try {
            if (commitTracked(offsets -> kafkaConsumer.commitSync(offsets),
                    "commit, partition=" + tp + " nextOffset=" + next)) {
                progress.committed(tp, next);
            }
        } catch (WakeupException e) {
            LOG.info("[CPI-KAFKA-PLUS-DIAG] commit interrupted by wakeup (consumer stopping): partition={} "
                    + "nextOffset={} stays pending", tp, next);
            progress.stop();
        } catch (InterruptException e) {
            Thread.currentThread().interrupt();
            LOG.info("[CPI-KAFKA-PLUS-DIAG] commit interrupted (consumer stopping): partition={} "
                    + "nextOffset={} stays pending", tp, next);
            progress.stop();
        }
    }

    /** Marks {@code tp}'s resolved prefix as pending without committing it (consumer stopping). */
    private void markProgress(TopicPartition tp, PollProgress progress) {
        long next = progress.commitOffset(tp);
        if (next >= 0) {
            offsetTracker.markProcessed(tp, next - 1);
        }
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
        boolean committed;
        try {
            committed = commitWithRebalanceHandling(() -> action.commit(snapshot), context);
        } catch (WakeupException | InterruptException e) {
            throw e; // the consumer is stopping; the caller decides
        } catch (KafkaException | IllegalStateException e) {
            // A commit that timed out or hit a closed consumer. The offsets stay pending and go out
            // with the next commit; the records were processed and must not be processed again.
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("consumer.commit.failed")
                    .with("context", context)
                    .with("consequence", "offsets stay pending and are committed with the next commit"), e);
            return false;
        }
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
    void recommitPending(Consumer<byte[], byte[]> kafkaConsumer) {
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
        RuntimeException validationException = new RuntimeException(errorMsg);
        try {
            Exchange mplExchange = callback.createExchange();
            mplExchange.getIn().setBody(value);
            mplExchange.getIn().setHeader("CpiKafkaPlusTopic", record.topic());
            mplExchange.getIn().setHeader("CpiKafkaPlusPartition", record.partition());
            mplExchange.getIn().setHeader("CpiKafkaPlusOffset", record.offset());
            mplExchange.getIn().setHeader("SAP_Sender", record.topic());
            tracingHelper.traceInbound(mplExchange, value);
            mplExchange.setProperty(Exchange.ROUTE_STOP, Boolean.TRUE);
            mplExchange.setException(validationException);

            // f2: Call traceError for the consumer/sender direction (SENDER_OUTBOUND_FAULT)
            java.util.Map<String, String> context = new java.util.LinkedHashMap<>();
            context.put("topic", record.topic());
            context.put("partition", String.valueOf(record.partition()));
            context.put("offset", String.valueOf(record.offset()));
            context.put("validationError", validationError);
            tracingHelper.traceError(mplExchange, validationException, context, true);

            // f1/f4/f5: Report failure with structured fields and attachment
            // JSON schema validation failures have a dedicated error code
            String errorCode = CpiKafkaPlusErrorCode.fromJsonSchemaValidationFailure(validationException).code();
            tracingHelper.reportFailure(mplExchange, validationException,
                    errorCode, context, true);

            callback.processExchange(mplExchange);
        } catch (Exception e) {
            if (e == validationException) {
                // Expected: the exchange carries the validation failure precisely so that it ends
                // as a failed message. Logging it as a reporting failure was a false ERROR (#177).
                return;
            }
            // b4: Swallowed error now logged at ERROR, not DEBUG — only ERROR reaches tenant trace
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("consumer.mpl.report.failed")
                    .with("topic", record.topic())
                    .with("partition", record.partition())
                    .with("offset", record.offset())
                    .with("validationError", validationError), e);
        }
    }
}
