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

import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The start-up checks of the sender ({@link CpiKafkaPlusConsumer}): a value that can never work is
 * rejected with an {@link IllegalArgumentException} that names the field as the UI shows it and the
 * value it got ({@link UiLabels}), and a value with a working fallback is logged as a
 * {@code consumer.config.*} line at ERROR. Moved out of the consumer; it logs under the consumer's
 * logger category, so log levels and trace filters set for the consumer still apply.
 *
 * <p>{@link #validate} runs after {@code endpoint.validateConfiguration()} and before
 * {@code super.doStart()}. Its checks run in a fixed order and the first failing one wins; a
 * {@code consumer.config.*} line is logged even when a later check rejects the deployment.
 * Package-private.
 */
final class SenderConfigValidator {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusConsumer.class);

    /** The characters and the length Kafka allows in a topic name. It also refuses "." and ".." alone. */
    private static final Pattern TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._-]{1,249}");

    private SenderConfigValidator() {
    }

    /**
     * Every start-up check of the sender, in order. The consumption mode comes first because the
     * drain and polling-interval checks depend on it.
     */
    static void validate(CpiKafkaPlusEndpoint endpoint) {
        // Validate the consumption mode FIRST — the drain/polling-interval checks below are
        // conditional on it.
        String mode = endpoint.getConsumptionMode();
        if (mode == null
                || !("SCHEDULED".equalsIgnoreCase(mode) || "STREAMING".equalsIgnoreCase(mode))) {
            throw new IllegalArgumentException(UiLabels.field("consumptionMode")
                    + " must be SCHEDULED or STREAMING, got: " + UiLabels.quote(mode));
        }
        boolean streaming = endpoint.isStreamingMode();

        // Consumer-specific validations. With a DLQ, Error Handling is hidden and never used, so a
        // stale value must not block the start (#174).
        if (!endpoint.isDlqEnabled()) {
            String errorHandling = endpoint.getErrorHandling();
            if (errorHandling == null
                    || !("RETRY".equalsIgnoreCase(errorHandling) || "SKIP".equalsIgnoreCase(errorHandling))) {
                throw new IllegalArgumentException(UiLabels.field("errorHandling") + " must be RETRY "
                        + "(Retry Failed Message) or SKIP (Skip Failed Message), got: "
                        + UiLabels.quote(errorHandling));
            }
        }
        if (endpoint.isDlqEnabled()) {
            String dlqTopic = endpoint.getDlqTopic();
            if (dlqTopic == null || dlqTopic.trim().isEmpty()) {
                throw new IllegalArgumentException(UiLabels.field("dlqTopic") + " must not be empty while "
                        + UiLabels.field("dlqEnabled") + " is on, got: " + UiLabels.quote(dlqTopic));
            }
        }
        validateNeverWorkingValues(endpoint);
        // Drain- and interval-related validations only apply to SCHEDULED. In STREAMING these
        // fields are documented as ignored, so a stale value (e.g. drainEnabled=true left over
        // from a SCHEDULED configuration) must not prevent the iFlow from starting.
        // The drain fields are hidden and ignored while drain is off (#174).
        if (!streaming && endpoint.isDrainEnabled()) {
            if ("AUTO".equalsIgnoreCase(endpoint.getCommitStrategy())) {
                throw new IllegalArgumentException(UiLabels.field("commitStrategy")
                        + " must be BATCH_COMPLETE while " + UiLabels.field("drainEnabled") + " is on, got: "
                        + UiLabels.quote(endpoint.getCommitStrategy())
                        + ". AUTO commit cannot guarantee at-least-once delivery during drain loops.");
            }
            if (endpoint.getMinBacklogToDrain() < 0) {
                throw new IllegalArgumentException(UiLabels.field("minBacklogToDrain")
                        + " must not be negative, got: " + endpoint.getMinBacklogToDrain());
            }
            if (endpoint.getMinBacklogToDrain() > endpoint.getMaxPollRecords()) {
                throw new IllegalArgumentException(UiLabels.field("minBacklogToDrain") + " must be at most "
                        + endpoint.getMaxPollRecords() + ", the value of " + UiLabels.field("maxPollRecords")
                        + ", got: " + endpoint.getMinBacklogToDrain()
                        + ". A higher value would disable drain after every poll.");
            }
        }
        if (endpoint.getMaxPartitionFetchSizeKb() < 1 || endpoint.getMaxPartitionFetchSizeKb() > 51200) {
            throw new IllegalArgumentException(UiLabels.field("maxPartitionFetchSizeKb")
                    + " must be between 1 and 51200 (50 MB), got: " + endpoint.getMaxPartitionFetchSizeKb());
        }
        // Used by the DLQ retries and by Retry Failed Message; without either it has no effect (#174).
        boolean retryDelayUsed = endpoint.isDlqEnabled() || !endpoint.isSkipFailedMessages();
        if (retryDelayUsed && (endpoint.getRetryDelaySeconds() < 0 || endpoint.getRetryDelaySeconds() > 300)) {
            throw new IllegalArgumentException(UiLabels.field("retryDelaySeconds")
                    + " must be between 0 and 300, got: " + endpoint.getRetryDelaySeconds());
        }
        if (!streaming) {
            validatePollingIntervalSeconds(endpoint.getPollingIntervalSeconds());
        }
    }

    /**
     * Rejects values that can never work (#183) — each only where the field is in effect (#174). Every
     * one of them used to surface only at the first poll, as an endless error loop, or not at all.
     * Values with a working fallback are logged, not rejected.
     */
    private static void validateNeverWorkingValues(CpiKafkaPlusEndpoint endpoint) {
        if (endpoint.getGroupId() == null || endpoint.getGroupId().trim().isEmpty()) {
            throw new IllegalArgumentException(UiLabels.field("groupId") + " must not be empty, got: "
                    + UiLabels.quote(endpoint.getGroupId())
                    + ". The consumer cannot join a consumer group without it.");
        }
        String commitStrategy = endpoint.getCommitStrategy();
        if (!"BATCH_COMPLETE".equalsIgnoreCase(commitStrategy) && !"AUTO".equalsIgnoreCase(commitStrategy)) {
            throw new IllegalArgumentException(UiLabels.field("commitStrategy")
                    + " must be BATCH_COMPLETE or AUTO, got: " + UiLabels.quote(commitStrategy)
                    + ". Any other value never commits an offset, so every message "
                    + "would be delivered again on every restart and rebalance.");
        }
        if (endpoint.getMaxPollRecords() < 1) {
            throw new IllegalArgumentException(UiLabels.field("maxPollRecords")
                    + " must be at least 1, got: " + endpoint.getMaxPollRecords());
        }
        if (endpoint.getFetchMinBytes() < 0) {
            throw new IllegalArgumentException(UiLabels.field("fetchMinBytes")
                    + " must not be negative, got: " + endpoint.getFetchMinBytes());
        }
        if (endpoint.getFetchMaxWaitMs() < 0) {
            throw new IllegalArgumentException(UiLabels.field("fetchMaxWaitMs")
                    + " must not be negative, got: " + endpoint.getFetchMaxWaitMs());
        }
        if (endpoint.getBatchTimeout() < 0) {
            throw new IllegalArgumentException(UiLabels.field("batchTimeout")
                    + " must not be negative, got: " + endpoint.getBatchTimeout());
        }
        String batchOutputFormat = endpoint.getBatchOutputFormat();
        boolean batchPath = endpoint.isBatchMode() && !"SPLIT_EXCHANGES".equalsIgnoreCase(batchOutputFormat);
        if (batchPath && endpoint.getBatchSize() < 1) {
            throw new IllegalArgumentException(UiLabels.field("batchSize")
                    + " must be at least 1 in batch mode, got: " + endpoint.getBatchSize());
        }
        if (endpoint.isDlqEnabled()) {
            if (endpoint.getDlqMaxRetries() < 0) {
                throw new IllegalArgumentException(UiLabels.field("dlqMaxRetries")
                        + " must not be negative, got: " + endpoint.getDlqMaxRetries());
            }
            String dlqTopic = endpoint.getDlqTopic();
            if (!dlqTopic.equals(dlqTopic.trim())) {
                // The DLQ producer sends to the name as configured, which Kafka rejects as invalid.
                throw new IllegalArgumentException(UiLabels.field("dlqTopic")
                        + " must not start or end with a space, got: " + UiLabels.quote(dlqTopic)
                        + ". Please remove the spaces around the topic name.");
            }
            if (!TOPIC_NAME.matcher(dlqTopic).matches() || ".".equals(dlqTopic) || "..".equals(dlqTopic)) {
                // Fails at the first dead letter, when the record can no longer go anywhere else (#261).
                throw new IllegalArgumentException(UiLabels.field("dlqTopic") + " must be a valid Kafka topic "
                        + "name, got: " + UiLabels.quote(dlqTopic) + ". Kafka allows 1 to 249 characters from "
                        + "a-z, A-Z, 0-9, '.', '_' and '-', but not '.' or '..' alone.");
            }
            if (CpiKafkaPlusConsumer.parseTopics(endpoint.getEffectiveTopic()).contains(dlqTopic)) {
                throw new IllegalArgumentException(UiLabels.field("dlqTopic") + " must not be a topic this "
                        + "channel consumes, got: " + UiLabels.quote(dlqTopic) + ". A failed record would be "
                        + "dead-lettered into its own source and consumed again, endlessly. Please use a "
                        + "separate DLQ topic.");
            }
        }
        if (endpoint.isSchemaRegistryEnabled() && endpoint.isAvroValueDeserialization()) {
            SchemaRegistryHttpClient.requireUsableUrl(endpoint.getSchemaRegistryUrl());
        }
        if (batchPath && !"JSON_ARRAY".equalsIgnoreCase(batchOutputFormat)
                && !"XML_LIST".equalsIgnoreCase(batchOutputFormat)) {
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("consumer.config.ineffective")
                    .with("parameter", "batchOutputFormat")
                    .with("value", batchOutputFormat)
                    .with("reason", "unknown value; batches are formatted as JSON_ARRAY"));
        }
        if (endpoint.isSchemaRegistryEnabled() && endpoint.isAvroValueDeserialization()
                && !"JSON".equalsIgnoreCase(endpoint.getAvroOutputFormat())
                && !"XML".equalsIgnoreCase(endpoint.getAvroOutputFormat())) {
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("consumer.config.ineffective")
                    .with("parameter", "avroOutputFormat")
                    .with("value", endpoint.getAvroOutputFormat())
                    .with("reason", "unknown value; Avro records are converted to JSON"));
        }
        if (endpoint.isAutoPauseEnabled()
                && (endpoint.getAutoPauseErrorThreshold() < 1 || endpoint.getAutoPauseCooldownSeconds() < 1)) {
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("consumer.config.clamped")
                    .with("autoPauseErrorThreshold", endpoint.getAutoPauseErrorThreshold())
                    .with("autoPauseCooldownSeconds", endpoint.getAutoPauseCooldownSeconds())
                    .with("reason", "values below 1 are used as 1"));
        }
        if (endpoint.isAutoPauseEnabled()
                && endpoint.getAutoPauseCooldownSeconds() > ConsumerCircuitBreaker.MAX_COOLDOWN_SECONDS) {
            // Every pause is capped at 15 minutes; a longer initial pause used to be cut silently.
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("consumer.config.clamped")
                    .with("autoPauseCooldownSeconds", endpoint.getAutoPauseCooldownSeconds())
                    .with("reason", "values above " + ConsumerCircuitBreaker.MAX_COOLDOWN_SECONDS
                            + " are used as " + ConsumerCircuitBreaker.MAX_COOLDOWN_SECONDS));
        }
        if (endpoint.getSecurityProtocol() == null || endpoint.getSecurityProtocol().isEmpty()) {
            // Works against a PLAINTEXT broker, so it is logged, not rejected (#261).
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("consumer.config.ineffective")
                    .with("parameter", "securityProtocol")
                    .with("value", "")
                    .with("reason", SecurityConfigHelper.EMPTY_PROTOCOL));
        }
    }

    /**
     * Validates that {@code pollingIntervalSeconds} is in the supported range {@code (0, 21600]}.
     * Values above 21600 (6 h) would exceed the {@code max.poll.interval.ms} hard cap of
     * 6 h 10 min and cause the consumer to leave its group between polls (issue #45).
     * The 6-h limit is chosen as a whole-hour value so polls align with predictable clock
     * times (e.g. every 6 h: 00:00, 06:00, 12:00, 18:00).
     */
    static void validatePollingIntervalSeconds(long pollingIntervalSeconds) {
        if (pollingIntervalSeconds <= 0) {
            throw new IllegalArgumentException(UiLabels.field("pollingIntervalSeconds")
                    + " must be at least 1, got: " + pollingIntervalSeconds);
        }
        if (pollingIntervalSeconds > 21600) {
            throw new IllegalArgumentException(UiLabels.field("pollingIntervalSeconds")
                    + " must be at most 21600 (6 h), got: " + pollingIntervalSeconds
                    + ". Higher values exceed the max.poll.interval.ms hard cap (6 h 10 min) "
                    + "and would cause the consumer to leave the group between polls.");
        }
    }
}
