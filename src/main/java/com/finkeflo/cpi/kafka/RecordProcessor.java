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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

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
 * <p>No record, batch or retry is started once the poll's time limit has passed (issue #192),
 * so that the poll thread calls {@code poll()} again within {@code max.poll.interval.ms}. A record
 * whose retries the limit interrupts keeps its count of attempts and continues with a later poll.
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

        /**
         * Hands a failure to the exception handler that {@link RecordProcessor} has already
         * reported to the MPL, so it is not reported a second time (#289).
         */
        default void handleReportedException(String message, Exchange exchange, Exception e) {
            handleException(message, exchange, e);
        }

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

    /**
     * A record whose retries a poll's time limit interrupted (issue #192): it is retried by a later
     * poll from {@code nextAttempt} on instead of starting over. At most one per partition, because
     * nothing after the record is processed until it is resolved.
     */
    private static final class CarriedRetry {
        private final long offset;
        private final int nextAttempt;

        CarriedRetry(long offset, int nextAttempt) {
            this.offset = offset;
            this.nextAttempt = nextAttempt;
        }
    }

    private final CpiKafkaPlusEndpoint endpoint;
    private final AdapterTracingHelper tracingHelper;
    private final JsonSchemaValidator jsonSchemaValidator;
    private final DlqProducerHelper dlqHelper;
    private final AvroDeserializerHelper avroHelper;
    private final ConsumerCallback callback;
    private final OffsetCommitTracker offsetTracker;
    private final RecordFailurePolicy failurePolicy;
    private final LongSupplier clock;
    /** Concurrent because the rebalance callbacks can also run on the thread that closes the consumer. */
    private final Map<TopicPartition, CarriedRetry> carriedRetries = new ConcurrentHashMap<>();

    RecordProcessor(CpiKafkaPlusEndpoint endpoint,
                    AdapterTracingHelper tracingHelper,
                    JsonSchemaValidator jsonSchemaValidator,
                    DlqProducerHelper dlqHelper,
                    AvroDeserializerHelper avroHelper,
                    ConsumerCallback callback,
                    OffsetCommitTracker offsetTracker) {
        this(endpoint, tracingHelper, jsonSchemaValidator, dlqHelper, avroHelper, callback, offsetTracker,
                System::currentTimeMillis);
    }

    /** @param clock the time the poll's time limit is checked against, in milliseconds */
    RecordProcessor(CpiKafkaPlusEndpoint endpoint,
                    AdapterTracingHelper tracingHelper,
                    JsonSchemaValidator jsonSchemaValidator,
                    DlqProducerHelper dlqHelper,
                    AvroDeserializerHelper avroHelper,
                    ConsumerCallback callback,
                    OffsetCommitTracker offsetTracker,
                    LongSupplier clock) {
        this.endpoint = endpoint;
        this.tracingHelper = tracingHelper;
        this.jsonSchemaValidator = jsonSchemaValidator;
        this.dlqHelper = dlqHelper;
        this.avroHelper = avroHelper;
        this.callback = callback;
        this.offsetTracker = offsetTracker;
        this.clock = clock;
        this.failurePolicy = new RecordFailurePolicy(dlqHelper != null, endpoint::getDlqMaxRetries,
                endpoint::isRetryOnlyTransientErrors, endpoint::isSkipFailedMessages);
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
                if (stopRequested(progress) || (blockedAt != null && batch.get(0).offset() >= blockedAt)
                        || outOfTime(progress, batch.get(0))) {
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
                        RecordFailurePolicy.Action action = failurePolicy.forSchemaViolation();
                        boolean dlqFailed = false;
                        Exception dlqFailure = null;
                        if (action == RecordFailurePolicy.Action.DEAD_LETTER) {
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
                                dlqFailure = dlqEx;
                            }
                        }
                        String message = "JSON Schema validation failed at offset " + record.offset();
                        if (dlqFailed) {
                            reportDlqSendFailed(record, new RuntimeException(validationError),
                                    CpiKafkaPlusErrorCode.KP_CFG_002.code(), dlqFailure, schemaContext(record));
                            callback.handleReportedException(message, callback.createExchange(),
                                    new RuntimeException(validationError));
                        } else {
                            RuntimeException validationFailure = new RuntimeException(validationError);
                            java.util.Map<String, String> context = schemaContext(record);
                            context.put("validationError", validationError);
                            Exchange errorExchange;
                            if (action == RecordFailurePolicy.Action.DEAD_LETTER) {
                                context.put("dlqOutcome", "MOVED");
                                context.put("dlqTopic", endpoint.getDlqTopic());
                                errorExchange = newDlqMplExchange(record);
                            } else {
                                errorExchange = callback.createExchange();
                                errorExchange.getIn().setHeader("SAP_Sender", record.topic());
                            }
                            tracingHelper.traceError(errorExchange, validationFailure, context, true);
                            tracingHelper.reportFailure(errorExchange, validationFailure,
                                    CpiKafkaPlusErrorCode.KP_CFG_002.code(), context, true);
                            callback.handleReportedException(message, errorExchange, validationFailure);
                        }
                        if (dlqFailed || action == RecordFailurePolicy.Action.BLOCK) {
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
            if (outOfTime(progress, record)) {
                break;
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
            if (KeepAlive.seekBack(kafkaConsumer, e.getKey(), e.getValue())) {
                LOG.debug("[CPI-KAFKA-PLUS-DIAG] rewind: partition {} back to offset {}", e.getKey(), e.getValue());
            }
        }
    }

    private boolean stopRequested(PollProgress progress) {
        if (!progress.isStopped() && callback.isStopRequested()) {
            progress.stop();
        }
        return progress.isStopped();
    }

    /**
     * Whether the poll's time limit has passed, so that {@code next} is not started (issue #192).
     * Records, batches and retries that run long, or many of them, would otherwise keep the poll
     * thread from calling {@code poll()} within {@code max.poll.interval.ms}: Kafka then gives the
     * partitions to another member, which processes the same records again. What is not started
     * stays unresolved and is rewound, so it comes with the next poll. Logged once per poll.
     */
    private boolean outOfTime(PollProgress progress, ConsumerRecord<byte[], byte[]> next) {
        if (progress.isOutOfTime()) {
            return true;
        }
        long now = clock.getAsLong();
        if (now < progress.deadlineMs()) {
            return false;
        }
        progress.outOfTime();
        AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("consumer.poll.timeLimit")
                .with("topic", next.topic())
                .with("partition", next.partition())
                .with("offset", next.offset())
                .with("pollElapsedMs", now - progress.startedMs())
                .with("pollTimeLimitMs", progress.timeLimitMs())
                .with("consequence", "records of this poll that were not started yet come with the next poll"));
        return true;
    }

    /**
     * A route call that fails while the consumer is stopping failed because of the stop: the stop
     * interrupts the poll thread, and Camel refuses new work during a shutdown. Whatever the
     * failure says, it is not the record's fault, so the record or batch stays unresolved and is
     * delivered again after the restart, instead of being dead-lettered or skipped (#242).
     */
    private static void logStoppedDuringRoute(String what, ConsumerRecord<byte[], byte[]> first, Exception e) {
        LOG.info("[CPI-KAFKA-PLUS-DIAG] route call for the {} at offset={} partition={} failed while the consumer "
                + "is stopping; it stays uncommitted and is delivered again after the restart: {}",
                what, first.offset(), first.partition(), e.getMessage());
    }

    private static TopicPartition partitionOf(ConsumerRecord<byte[], byte[]> record) {
        return new TopicPartition(record.topic(), record.partition());
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
            RecordFailurePolicy.Action action = failurePolicy.forBatchFormatFailure(t);
            if (action == RecordFailurePolicy.Action.PROCESS_INDIVIDUALLY) {
                LOG.info("[CPI-KAFKA-PLUS-DIAG] processOneBatch: formatBatch failed, retrying batch records individually for poison-pill isolation");
                return processRecordsIndividually(kafkaConsumer, batch, commitAfterSuccess, progress);
            }
            if (action == RecordFailurePolicy.Action.ESCAPE) {
                throw new RuntimeException(t); // an Error is not a data problem; the poll rewinds it
            }
            progress.failed();
            callback.handleException(
                    "Could not format batch of " + batch.size() + " Kafka records from partition "
                            + batch.get(0).partition(), exchange, (Exception) t);
            blockOrSkip(kafkaConsumer, batch, action, commitAfterSuccess, progress);
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
            if (stopRequested(progress)) {
                logStoppedDuringRoute("batch", batch.get(0), e);
                return 0;
            }
            int partition = !batch.isEmpty() ? batch.get(0).partition() : -1;
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("batch.processing.failed")
                    .with("errorCode", CpiKafkaPlusErrorCode.fromRouteFailure(e).code())
                    .with("partition", partition)
                    .with("batchSize", batch.size()), e);
            RecordFailurePolicy.Action action = failurePolicy.forBatchRouteFailure();
            if (action == RecordFailurePolicy.Action.PROCESS_INDIVIDUALLY) {
                if (batch.size() == 1) {
                    // Running the one record again would only repeat this call (#289): the batch
                    // run is its first attempt.
                    LOG.info("[CPI-KAFKA-PLUS-DIAG] processOneBatch: DLQ enabled, batch of one record failed, "
                            + "continuing with its retries");
                    return processRecordWithRetry(kafkaConsumer, batch.get(0), commitAfterSuccess, true,
                            progress, e);
                }
                LOG.info("[CPI-KAFKA-PLUS-DIAG] processOneBatch: DLQ enabled, retrying batch records individually");
                return processRecordsIndividually(kafkaConsumer, batch, commitAfterSuccess, progress);
            }
            progress.failed();
            java.util.Map<String, String> context = new java.util.LinkedHashMap<>();
            context.put("topic", batch.get(0).topic());
            context.put("partition", String.valueOf(batch.get(0).partition()));
            context.put("firstOffset", String.valueOf(batch.get(0).offset()));
            context.put("lastOffset", String.valueOf(batch.get(batch.size() - 1).offset()));
            context.put("batchSize", String.valueOf(batch.size()));
            String message = "iFlow processing failed for batch of " + batch.size()
                    + " Kafka records from partition " + batch.get(0).partition();
            context.put("message", message);
            Exchange traceExchange = callback.createExchange();
            traceExchange.getIn().setHeader("SAP_Sender", batch.get(0).topic());
            tracingHelper.traceError(traceExchange, e, context, true);
            tracingHelper.reportFailure(traceExchange, e, CpiKafkaPlusErrorCode.fromRouteFailure(e).code(),
                    context, true);
            callback.handleReportedException(message, exchange, e);
            blockOrSkip(kafkaConsumer, batch, action, commitAfterSuccess, progress);
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

    private int processRecordWithRetry(Consumer<byte[], byte[]> kafkaConsumer,
                                       ConsumerRecord<byte[], byte[]> record,
                                       boolean commitAfterSuccess,
                                       boolean batchFallback,
                                       PollProgress progress) {
        return processRecordWithRetry(kafkaConsumer, record, commitAfterSuccess, batchFallback, progress, null);
    }

    /**
     * @param failedAttempt the failure of a batch of just this record, which counts as the record's
     *                      next attempt (#289), or {@code null}
     */
    private int processRecordWithRetry(Consumer<byte[], byte[]> kafkaConsumer,
                                       ConsumerRecord<byte[], byte[]> record,
                                       boolean commitAfterSuccess,
                                       boolean batchFallback,
                                       PollProgress progress,
                                       Exception failedAttempt) {
        TopicPartition tp = partitionOf(record);
        progress.processing(tp, record.offset());
        // A record whose retries an earlier poll ran out of time for continues where it stopped
        // (#192). One carried for another offset is stale: that record was resolved meanwhile.
        CarriedRetry carried = carriedRetries.remove(tp);
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

        int maxRetries = failurePolicy.maxRetries();
        int firstAttempt = carried != null && carried.offset == record.offset()
                ? Math.min(carried.nextAttempt, maxRetries) : 0;
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

        if (failedAttempt != null) {
            lastError = failedAttempt;
            actualRetries = firstAttempt;
            AttemptOutcome outcome = afterFailedAttempt(record, firstAttempt, maxRetries, failedAttempt, progress);
            if (outcome == AttemptOutcome.STOP) {
                return 0;
            }
            if (outcome == AttemptOutcome.PERMANENT) {
                permanentError = true;
                firstAttempt = maxRetries + 1;
            } else {
                firstAttempt++;
            }
        }

        for (int attempt = firstAttempt; attempt <= maxRetries; attempt++) {
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
                // Counted before a permanent failure breaks off, so the retry that failed
                // permanently is included (#244).
                actualRetries = attempt;

                if (stopRequested(progress)) {
                    logStoppedDuringRoute("record", record, e);
                    return 0;
                }

                AttemptOutcome outcome = afterFailedAttempt(record, attempt, maxRetries, e, progress);
                if (outcome == AttemptOutcome.STOP) {
                    return 0;
                }
                if (outcome == AttemptOutcome.PERMANENT) {
                    permanentError = true;
                    break;
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

    private enum AttemptOutcome { RETRY, PERMANENT, STOP }

    /**
     * Decides what follows the failed {@code attempt}: a permanent failure skips the retries left,
     * otherwise the wait before the next attempt, if there is one. {@link AttemptOutcome#STOP}
     * leaves the record unresolved: its retry was deferred to a later poll or the wait was
     * interrupted.
     */
    private AttemptOutcome afterFailedAttempt(ConsumerRecord<byte[], byte[]> record, int attempt,
                                              int maxRetries, Exception e, PollProgress progress) {
        if (failurePolicy.skipsRetries(e)) {
            LOG.info("[CPI-KAFKA-PLUS-DIAG] Permanent error detected, "
                    + "skipping {} remaining retries -> DLQ: offset={} partition={} error='{}'",
                    maxRetries - attempt, record.offset(), record.partition(), e.getMessage());
            return AttemptOutcome.PERMANENT;
        }
        if (attempt < maxRetries) {
            long delayMs = retryDelayMs(attempt);
            long now = clock.getAsLong();
            if (now + delayMs >= progress.deadlineMs()) {
                deferRetry(record, attempt + 1, maxRetries, now, delayMs, progress);
                return AttemptOutcome.STOP;
            }
            if (!sleepWithBackoff(attempt, record, delayMs)) {
                // Interrupted between two attempts: a shutdown, not a poison record. Leave
                // it unresolved for the next run instead of dead-lettering a record that may
                // well succeed.
                progress.stop();
                return AttemptOutcome.STOP;
            }
        }
        return AttemptOutcome.RETRY;
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
        RecordFailurePolicy.Action action = failurePolicy.forSchemaViolation();
        if (action == RecordFailurePolicy.Action.DEAD_LETTER) {
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
                reportDlqSendFailed(record, new RuntimeException(validationError),
                        CpiKafkaPlusErrorCode.KP_CFG_002.code(), dlqEx, schemaContext(record));
                return true;
            }
            progress.resolved(tp, record.offset());
            if (commitAfterSuccess) {
                commitProgress(kafkaConsumer, tp, progress);
            }
            return true;
        }
        // Without a DLQ an invalid record is dropped, as documented for JSON Schema validation.
        blockOrSkip(kafkaConsumer, record, action, commitAfterSuccess, progress);
        return true;
    }

    /**
     * @return the wait before the retry that follows the failed {@code attempt}: the retry delay,
     *         doubled after every attempt and capped at 300 seconds, or 0 without a retry delay
     */
    private long retryDelayMs(int attempt) {
        int delaySeconds = endpoint.getRetryDelaySeconds();
        if (delaySeconds <= 0) {
            return 0L;
        }
        int clampedAttempt = Math.min(attempt, 30);
        return Math.min((long) delaySeconds * (1L << clampedAttempt), MAX_RETRY_DELAY_SECONDS) * 1000L;
    }

    /**
     * The retry that follows the failed {@code nextAttempt - 1} would start after the poll's time
     * limit (issue #192). The poll thread does not wait for it: the record's partition is blocked at
     * it, rewound and paused until the retry is due, and a later poll runs that retry instead of
     * starting the record over. So the record still reaches the DLQ after the configured retries,
     * however many polls they take.
     */
    private void deferRetry(ConsumerRecord<byte[], byte[]> record, int nextAttempt, int maxRetries,
                            long now, long delayMs, PollProgress progress) {
        TopicPartition tp = partitionOf(record);
        carriedRetries.put(tp, new CarriedRetry(record.offset(), nextAttempt));
        progress.deferred(tp, record.offset(), now + delayMs);
        AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("consumer.retry.deferred")
                .with("topic", record.topic())
                .with("partition", record.partition())
                .with("offset", record.offset())
                .with("retry", nextAttempt)
                .with("maxRetries", maxRetries)
                .with("retryInMs", delayMs)
                .with("pollElapsedMs", now - progress.startedMs())
                .with("pollTimeLimitMs", progress.timeLimitMs())
                .with("consequence", "a later poll runs this retry when it is due; nothing at or after "
                        + "this offset is committed until then"));
    }

    /**
     * Sleeps with exponential backoff between retry attempts.
     *
     * @param delayMs the wait, from {@link #retryDelayMs(int)}
     * @return true to continue retrying, false if interrupted
     */
    private boolean sleepWithBackoff(int attempt, ConsumerRecord<byte[], byte[]> record, long delayMs) {
        int maxRetries = failurePolicy.maxRetries();

        if (delayMs > 0) {
            LOG.info("[CPI-KAFKA-PLUS-DIAG] Transient error, retrying in {}s "
                    + "(attempt {}/{}): offset={} partition={}",
                    delayMs / 1000, attempt + 1, maxRetries + 1,
                    record.offset(), record.partition());
            try {
                Thread.sleep(delayMs);
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
        RecordFailurePolicy.Decision decision = failurePolicy.afterRetries(permanentError);
        String errorType = decision.errorType();

        java.util.Map<String, String> context = new java.util.LinkedHashMap<>();
        context.put("topic", record.topic());
        context.put("partition", String.valueOf(record.partition()));
        context.put("offset", String.valueOf(record.offset()));
        context.put("retryAttempts", String.valueOf(actualRetries));
        context.put("errorType", errorType != null ? errorType : "UNKNOWN");
        String errorCode = CpiKafkaPlusErrorCode.fromRouteFailure(lastError).code();
        String message = "iFlow processing failed at offset " + record.offset()
                + " (partition " + record.partition() + ")";

        if (decision.action() == RecordFailurePolicy.Action.DEAD_LETTER) {
            try {
                dlqHelper.sendToDlq(record, lastError, actualRetries, errorType);
                java.util.Map<String, String> dlqContext = new java.util.LinkedHashMap<>(context);
                dlqContext.put("dlqOutcome", "MOVED");
                dlqContext.put("dlqTopic", endpoint.getDlqTopic());
                Exchange traceExchange = newDlqMplExchange(record);
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
                        .with("originalError", errorCode)
                        .with("consequence", "offset not committed, record will be retried"), dlqEx);
                reportDlqSendFailed(record, lastError, errorCode, dlqEx, context);
            }
        } else {
            Exchange traceExchange = callback.createExchange();
            traceExchange.getIn().setHeader("SAP_Sender", record.topic());
            tracingHelper.traceError(traceExchange, lastError, context, true);
            tracingHelper.reportFailure(traceExchange, lastError, errorCode, context, true);
            blockOrSkip(kafkaConsumer, record, decision.action(), commitAfterSuccess, progress);
        }

        callback.handleReportedException(message, callback.createExchange(), lastError);
        return 0;
    }

    /**
     * A fresh exchange for the MPL entry of a record on its way to the DLQ: the monitor shows the
     * source topic as sender and the DLQ topic as receiver, whether the write succeeded or not.
     */
    private Exchange newDlqMplExchange(ConsumerRecord<byte[], byte[]> record) {
        Exchange exchange = callback.createExchange();
        exchange.getIn().setHeader("SAP_Sender", record.topic());
        String dlqTopic = endpoint.getDlqTopic();
        if (dlqTopic != null && !dlqTopic.trim().isEmpty()) {
            exchange.getIn().setHeader("SAP_Receiver", dlqTopic);
        }
        return exchange;
    }

    /**
     * Reports a record whose DLQ write failed (#289). The failed write is what blocks the
     * partition, so it is the primary error, with its root cause, whatever it is; the failure that
     * sent the record to the DLQ stays in the report as the original error.
     */
    private void reportDlqSendFailed(ConsumerRecord<byte[], byte[]> record, Exception originalError,
                                     String originalErrorCode, Exception dlqEx,
                                     java.util.Map<String, String> context) {
        java.util.Map<String, String> dlqContext = new java.util.LinkedHashMap<>(context);
        dlqContext.put("originalErrorCode", originalErrorCode);
        dlqContext.put("dlqOutcome", "SEND_FAILED");
        dlqContext.put("dlqTopic", endpoint.getDlqTopic());
        Throwable root = CauseChain.root(dlqEx);
        if (root != null) {
            dlqContext.put("dlqRootCauseClass", root.getClass().getName());
            if (root.getMessage() != null) {
                dlqContext.put("dlqRootCauseMessage", root.getMessage());
            }
        }
        String hint = KafkaErrorHelper.dlqFailureHint(dlqEx);
        if (hint != null) {
            dlqContext.put("hint", hint);
        }
        dlqContext.put("consequence",
                "offset not committed, partition blocked at this record, retried after a backoff");
        Exchange traceExchange = newDlqMplExchange(record);
        tracingHelper.traceError(traceExchange, originalError, dlqContext, true, dlqEx);
        tracingHelper.reportFailure(traceExchange, originalError, CpiKafkaPlusErrorCode.KP_DLQ_001.code(),
                dlqContext, true, dlqEx);
    }

    /**
     * Carries out the policy's decision for a failed record without a DLQ write: a skip resolves it
     * (at-most-once), so the next commit may pass it; anything else blocks the partition at the
     * record, so it is retried after a backoff.
     */
    private void blockOrSkip(Consumer<byte[], byte[]> kafkaConsumer, ConsumerRecord<byte[], byte[]> record,
                             RecordFailurePolicy.Action action, boolean commitAfterSuccess,
                             PollProgress progress) {
        blockOrSkip(kafkaConsumer, java.util.Collections.singletonList(record), action, commitAfterSuccess,
                progress);
    }

    /** The same for a failed batch: a skip resolves all its records, a block stops at the first. */
    private void blockOrSkip(Consumer<byte[], byte[]> kafkaConsumer, List<ConsumerRecord<byte[], byte[]>> records,
                             RecordFailurePolicy.Action action, boolean commitAfterSuccess,
                             PollProgress progress) {
        TopicPartition tp = partitionOf(records.get(0));
        if (action != RecordFailurePolicy.Action.SKIP) {
            progress.blocked(tp, records.get(0).offset());
            return;
        }
        for (ConsumerRecord<byte[], byte[]> record : records) {
            progress.resolved(tp, record.offset());
        }
        if (commitAfterSuccess) {
            commitProgress(kafkaConsumer, tp, progress);
        }
    }

    int processRecordsIndividually(Consumer<byte[], byte[]> kafkaConsumer,
                                   List<ConsumerRecord<byte[], byte[]>> batch,
                                   boolean commitAfterSuccess, PollProgress progress) {
        int processed = 0;
        for (ConsumerRecord<byte[], byte[]> record : batch) {
            if (stopRequested(progress) || progress.isBlocked(partitionOf(record)) || outOfTime(progress, record)) {
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
     * DLQ write fails, the partition is blocked at the record and it is retried later. So is a
     * record whose deserialization failed for a transient cause, such as a Schema Registry that
     * does not answer, DLQ or not: the record is not at fault (#188). The error no longer escapes
     * the poll: that skipped every remaining record of the poll as well.
     */
    private int handleDeserializationFailure(Consumer<byte[], byte[]> kafkaConsumer,
                                              ConsumerRecord<byte[], byte[]> record,
                                              Exception cause,
                                              boolean commitAfterSuccess,
                                              PollProgress progress) {
        java.util.Map<String, String> context = new java.util.LinkedHashMap<>();
        context.put("topic", record.topic());
        context.put("partition", String.valueOf(record.partition()));
        context.put("offset", String.valueOf(record.offset()));
        context.put("failureType", "DESERIALIZATION");
        // Deserialization failures map to KP_PROD_004 (serialization_failed) via the central taxonomy
        String errorCode = CpiKafkaPlusErrorCode.fromThrowable(cause).code();
        String message = "Could not deserialize Kafka record at offset " + record.offset();
        progress.failed();

        TopicPartition tp = partitionOf(record);
        RecordFailurePolicy.Action action = failurePolicy.forDeserializationFailure(cause);
        if (action != RecordFailurePolicy.Action.DEAD_LETTER) {
            // f1/f2/f4/f5: one MPL entry with structured fields and attachment
            Exchange traceExchange = callback.createExchange();
            traceExchange.getIn().setHeader("SAP_Sender", record.topic());
            tracingHelper.traceError(traceExchange, cause, context, true);
            tracingHelper.reportFailure(traceExchange, cause, errorCode, context, true);
            blockOrSkip(kafkaConsumer, record, action, commitAfterSuccess, progress);
            callback.handleReportedException(message, callback.createExchange(), cause);
            return 0;
        }
        LOG.warn("[CPI-KAFKA-PLUS-DIAG] poison-pill: deserialization failed at topic='{}' partition={} offset={}: {}",
                record.topic(), record.partition(), record.offset(), cause.getMessage());
        try {
            dlqHelper.sendDeserializationFailure(tp, record.offset(),
                    record.key(), record.value(),
                    record.headers(), record.timestamp(), cause);
            java.util.Map<String, String> dlqContext = new java.util.LinkedHashMap<>(context);
            dlqContext.put("dlqOutcome", "MOVED");
            dlqContext.put("dlqTopic", endpoint.getDlqTopic());
            Exchange traceExchange = newDlqMplExchange(record);
            tracingHelper.traceError(traceExchange, cause, dlqContext, true);
            tracingHelper.reportFailure(traceExchange, cause, errorCode, dlqContext, true);
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
            reportDlqSendFailed(record, cause, errorCode, dlqEx, context);
            callback.handleReportedException(message, callback.createExchange(), cause);
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
        forgetCarriedRetries(revoked);
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
            forgetCarriedRetries(lost);
        }
    }

    /** The next owner of the partitions starts their records over; a retry carried here is stale. */
    private void forgetCarriedRetries(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            carriedRetries.remove(tp);
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

    private static java.util.Map<String, String> schemaContext(ConsumerRecord<byte[], byte[]> record) {
        java.util.Map<String, String> context = new java.util.LinkedHashMap<>();
        context.put("topic", record.topic());
        context.put("partition", String.valueOf(record.partition()));
        context.put("offset", String.valueOf(record.offset()));
        context.put("failureType", "JSON_SCHEMA_VALIDATION");
        return context;
    }

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
            String dlqTopic = endpoint.getDlqTopic();
            if (failurePolicy.forSchemaViolation() == RecordFailurePolicy.Action.DEAD_LETTER
                    && dlqTopic != null && !dlqTopic.trim().isEmpty()) {
                mplExchange.getIn().setHeader("SAP_Receiver", dlqTopic);
            }
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
            if (causedBy(e, validationException)) {
                // Expected: the exchange carries the validation failure precisely so that it ends
                // as a failed message. Logging it as a reporting failure was a false ERROR (#177),
                // also when the runtime rethrows it wrapped.
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

    /** True if {@code expected} is {@code thrown} or in its cause chain (bounded: chains can be cyclic). */
    private static boolean causedBy(Throwable thrown, Throwable expected) {
        Throwable t = thrown;
        for (int depth = 0; t != null && depth < 32; depth++, t = t.getCause()) {
            if (t == expected) {
                return true;
            }
        }
        return false;
    }
}
