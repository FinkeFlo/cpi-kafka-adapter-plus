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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The start-up checks of the receiver ({@link CpiKafkaPlusProducer}): a value that can never work is
 * rejected with an {@link IllegalArgumentException} that names the field as the UI shows it and the
 * value it got ({@link UiLabels}), and a setting without effect is logged as
 * {@code producer.config.ineffective} at ERROR. Moved out of the producer; it logs under the
 * producer's logger category, so log levels and trace filters set for the producer still apply.
 *
 * <p>The checks run in three phases of the producer's start, in this order, and the first failing
 * check wins. The phases are separate because start-up lines are logged between them, and a
 * rejected deployment still shows the lines logged before the rejection:
 * <ol>
 *   <li>{@link #validateBeforeStart} after {@code endpoint.validateConfiguration(...)}, before
 *       {@code super.doStart()};</li>
 *   <li>{@link #validateTransactional} after {@code super.doStart()} and the start-up line
 *       "Starting CPI Kafka Producer". The length check of the {@code transactional.id} follows
 *       in {@link TransactionalProducerPool#start}, which resolves the member suffix it depends
 *       on;</li>
 *   <li>{@link #validateRetry} last, before the background topic check.</li>
 * </ol>
 * Package-private.
 */
final class ReceiverConfigValidator {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusProducer.class);

    /**
     * Upper bound for {@code deliveryTimeoutSeconds} on a transactional channel. The adapter derives
     * {@code transaction.timeout.ms} from the delivery timeout (see
     * {@link ProducerConfigFactory#transactionTimeoutMs(int)}), and the broker rejects a producer
     * asking for more than its {@code transaction.max.timeout.ms} — 15 minutes by default — at
     * {@code initTransactions()}. Rejecting that at start-up names the parameter to change; the
     * broker's {@code InvalidTxnTimeoutException} on the first send would not.
     */
    private static final int MAX_TRANSACTIONAL_DELIVERY_TIMEOUT_SECONDS =
            ProducerConfigFactory.TRANSACTION_TIMEOUT_BROKER_MAX_MS / 1000 - 30;
    /**
     * Upper bound for {@code producerRetryTotalBudgetSeconds}. Raised from 300 s in 1.3.2: with the
     * shipped delivery timeout of 120 s a single transactional attempt already costs roughly 215 s,
     * so two attempts could not be expressed at all and the retry was effectively unavailable to
     * every channel that had not lowered its delivery timeout. The budget still has to stay below
     * the timeout of a synchronous caller — but a Kafka-to-Kafka or scheduled channel has no such
     * caller, and 300 s was an arbitrary bound for it.
     */
    private static final int MAX_RETRY_BUDGET_SECONDS = 900;

    private ReceiverConfigValidator() {
    }

    /**
     * Phase 1, before {@code super.doStart()}: the batch send mode, the delivery timeout, the producer
     * sizes, and with Avro serialization the Schema Registry URL and the subject name strategy. Logs
     * {@code producer.config.ineffective} when JSON Schema validation is switched on with a batch send
     * mode, and for an empty security protocol.
     */
    static void validateBeforeStart(CpiKafkaPlusEndpoint endpoint) {
        // Values that can never work (#183): each used to fail every single message instead.
        String batchMode = endpoint.getProducerBatchMode();
        if (!"NONE".equalsIgnoreCase(batchMode) && !"JSON_ARRAY".equalsIgnoreCase(batchMode)
                && !"XML_LIST".equalsIgnoreCase(batchMode)) {
            throw unknownBatchMode(batchMode);
        }
        if (endpoint.isJsonSchemaValidation() && !"NONE".equalsIgnoreCase(batchMode)) {
            // Once per deployment instead of a WARN per message (#263); ERROR because nothing
            // below WARN reaches the tenant trace.
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.config.ineffective")
                    .with("parameter", "jsonSchemaValidation")
                    .with("producerBatchMode", batchMode)
                    .with("reason", "the JSON Schema applies to single messages only; batches are "
                            + "sent without validation"));
        }
        if (endpoint.getDeliveryTimeoutSeconds() < 1) {
            throw new IllegalArgumentException(UiLabels.field("deliveryTimeoutSeconds") + " must be at least 1, got: "
                    + endpoint.getDeliveryTimeoutSeconds() + ". With 0 every send times out before it is sent.");
        }
        // A negative size fails the Kafka client constructor, and with a request size or buffer of 0
        // every send fails (#261).
        if (endpoint.getMaxRequestSizeKb() < 1) {
            throw new IllegalArgumentException(UiLabels.field("maxRequestSizeKb") + " must be at least 1, got: "
                    + endpoint.getMaxRequestSizeKb() + ". No message fits into a smaller request.");
        }
        if (endpoint.getProducerBatchSizeKb() < 0) {
            throw new IllegalArgumentException(UiLabels.field("producerBatchSizeKb") + " must be at least 0, got: "
                    + endpoint.getProducerBatchSizeKb() + ". 0 switches batching off.");
        }
        if (endpoint.getBufferMemoryKb() < 1) {
            throw new IllegalArgumentException(UiLabels.field("bufferMemoryKb") + " must be at least 1, got: "
                    + endpoint.getBufferMemoryKb() + ". No message fits into a smaller buffer.");
        }
        boolean avro = endpoint.isSchemaRegistryEnabled() && endpoint.isAvroValueSerialization();
        if (avro) {
            SchemaRegistryHttpClient.requireUsableUrl(endpoint.getSchemaRegistryUrl());
        }
        // null keeps today's meaning, TopicNameStrategy; case-insensitive like resolveSubject() (#261).
        String strategy = endpoint.getSubjectNameStrategy();
        if (avro && strategy != null && !AvroSerializerHelper.TOPIC_NAME_STRATEGY.equalsIgnoreCase(strategy)) {
            throw new IllegalArgumentException(UiLabels.field("subjectNameStrategy") + " must be "
                    + AvroSerializerHelper.TOPIC_NAME_STRATEGY + " while " + UiLabels.field("avroValueSerialization")
                    + " is on, got: " + UiLabels.quote(strategy) + ". The subject cannot be resolved from JSON "
                    + "input without the record name.");
        }
        if (endpoint.getSecurityProtocol() == null || endpoint.getSecurityProtocol().isEmpty()) {
            // Works against a PLAINTEXT broker, so it is logged, not rejected (#261).
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.config.ineffective")
                    .with("parameter", "securityProtocol")
                    .with("value", "")
                    .with("reason", SecurityConfigHelper.EMPTY_PROTOCOL));
        }
    }

    /**
     * Phase 2, after the "Starting CPI Kafka Producer" line: logs {@code producer.config.ineffective}
     * when transactions are switched on without a batch send mode, and with transactional batching
     * rejects a missing prefix, idempotence switched off, a delivery timeout the broker would refuse
     * and fewer than one transaction slot.
     */
    static void validateTransactional(CpiKafkaPlusEndpoint endpoint) {
        if (endpoint.isEnableTransactions() && !endpoint.isTransactionalBatching()) {
            // ERROR because nothing below WARN reaches the tenant trace; once per deployment.
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.config.ineffective")
                    .with("parameter", "enableTransactions")
                    .with("producerBatchMode", endpoint.getProducerBatchMode())
                    .with("reason", "transactions apply to batches only; single messages are sent "
                            + "by the shared, non-transactional producer"));
        }
        if (endpoint.isTransactionalBatching()) {
            if (endpoint.getTransactionalIdPrefix() == null || endpoint.getTransactionalIdPrefix().trim().isEmpty()) {
                throw new IllegalArgumentException(UiLabels.field("transactionalIdPrefix")
                        + " must not be empty while " + UiLabels.field("enableTransactions") + " is on, got: "
                        + UiLabels.quote(endpoint.getTransactionalIdPrefix()));
            }
            if (!endpoint.isEnableIdempotence()) {
                throw new IllegalArgumentException(UiLabels.field("enableIdempotence")
                        + " must be on while " + UiLabels.field("enableTransactions") + " is on, got: "
                        + endpoint.isEnableIdempotence() + ". Kafka allows transactions only with idempotence.");
            }
            int deliveryTimeoutSeconds = endpoint.getDeliveryTimeoutSeconds();
            if (deliveryTimeoutSeconds > MAX_TRANSACTIONAL_DELIVERY_TIMEOUT_SECONDS) {
                throw new IllegalArgumentException(UiLabels.field("deliveryTimeoutSeconds")
                        + " must be at most " + MAX_TRANSACTIONAL_DELIVERY_TIMEOUT_SECONDS + " while "
                        + UiLabels.field("enableTransactions") + " is on, got: " + deliveryTimeoutSeconds
                        + ". The adapter derives transaction.timeout.ms from it, and a broker rejects a "
                        + "producer asking for more than transaction.max.timeout.ms (15 minutes by default).");
            }

            int slots = endpoint.getMaxConcurrentTransactions();
            if (slots < 1) {
                throw new IllegalArgumentException(UiLabels.field("maxConcurrentTransactions")
                        + " must be at least 1 while " + UiLabels.field("enableTransactions") + " is on, got: "
                        + slots + ". With no transaction slot the adapter could not send anything.");
            }
        }
    }

    /**
     * Validates the producer outer-retry configuration and refuses a start-up that could not
     * possibly work.
     *
     * <p>The worst-case check is a hard failure rather than a warning on purpose: a retry
     * configuration whose first attempt already consumes the entire budget is a promise to
     * operations that the adapter cannot keep, and it would only be discovered during the outage it
     * was configured for.
     */
    static void validateRetry(CpiKafkaPlusEndpoint endpoint) {
        int maxAttempts = endpoint.getProducerRetryMaxAttempts();
        if (maxAttempts < 1 || maxAttempts > 5) {
            throw new IllegalArgumentException(UiLabels.field("producerRetryMaxAttempts")
                    + " must be between 1 and 5, got: " + maxAttempts + ". This is the total number of "
                    + "attempts, not the number of additional ones; 1 switches the retry off.");
        }
        if (maxAttempts == 1) {
            return;  // feature off: the other retry fields have no effect and must not block (#174)
        }
        int delaySeconds = endpoint.getProducerRetryDelaySeconds();
        if (delaySeconds < 1 || delaySeconds > 30) {
            throw new IllegalArgumentException(UiLabels.field("producerRetryDelaySeconds")
                    + " must be between 1 and 30, got: " + delaySeconds + ". A delay below one second "
                    + "cannot help against a disconnected broker node — the Kafka client has already "
                    + "backed off exponentially before giving up — and only adds load.");
        }
        int budgetSeconds = endpoint.getProducerRetryTotalBudgetSeconds();
        if (budgetSeconds < 5 || budgetSeconds > MAX_RETRY_BUDGET_SECONDS) {
            throw new IllegalArgumentException(UiLabels.field("producerRetryTotalBudgetSeconds")
                    + " must be between 5 and " + MAX_RETRY_BUDGET_SECONDS + ", got: " + budgetSeconds);
        }

        // The worst case of the path the messages actually take: transactions apply to batches only.
        boolean transactional = endpoint.isTransactionalBatching();
        boolean batchMode = !"NONE".equalsIgnoreCase(endpoint.getProducerBatchMode());

        // A silently ineffective parameter is worse than no parameter: without transactions a
        // partially sent batch cannot be repeated without duplicating the records that already
        // made it, so this path deliberately never retries (see ProducerRetryPolicy).
        // ERROR, once per deployment, because nothing below WARN reaches the tenant trace (#255).
        if (batchMode && !transactional) {
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.config.ineffective")
                    .with("parameter", "producerRetryMaxAttempts")
                    .with("value", maxAttempts)
                    .with("producerBatchMode", endpoint.getProducerBatchMode())
                    .with("reason", "a partially sent batch cannot be retried without duplicating "
                            + "records; switch on enableTransactions to retry batches"));
            return;  // no retry on this path, so its worst case must not block the deployment
        }

        if (transactional && endpoint.getMaxConcurrentTransactions() == 1) {
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.config.ineffective")
                    .with("parameter", "producerRetryMaxAttempts")
                    .with("value", maxAttempts)
                    .with("maxConcurrentTransactions", 1)
                    .with("reason", "a retry holds the single transaction slot for up to "
                            + "producerRetryTotalBudgetSeconds=" + budgetSeconds + "s and blocks every "
                            + "other send on this node meanwhile; raise maxConcurrentTransactions"));
        }

        int deliveryTimeoutSeconds = endpoint.getDeliveryTimeoutSeconds();

        long worstCase = ProducerRetryPolicy.worstCaseSeconds(maxAttempts, deliveryTimeoutSeconds,
                delaySeconds, (int) TransactionalProducerPool.TXN_PRODUCER_CLOSE_TIMEOUT.getSeconds(),
                transactional);
        if (worstCase > budgetSeconds) {
            throw new IllegalArgumentException(UiLabels.field("producerRetryTotalBudgetSeconds")
                    + " must cover the worst case of the configured retry, " + worstCase + " s, got: "
                    + budgetSeconds + ". Lower " + UiLabels.field("deliveryTimeoutSeconds") + " (currently "
                    + deliveryTimeoutSeconds + ") or " + UiLabels.field("producerRetryMaxAttempts")
                    + ", or raise the budget. The budget must stay "
                    + "below the timeout of the calling HTTP client. Note that one attempt costs "
                    + "roughly four times deliveryTimeoutSeconds on the transactional path, because "
                    + "initTransactions(), the metadata block inside send() and commitTransaction() "
                    + "are each bounded by max.block.ms = min(30s, deliveryTimeoutSeconds) on top of "
                    + "the wait for acknowledgements.");
        }
    }

    static IllegalArgumentException unknownBatchMode(String batchMode) {
        return new IllegalArgumentException(UiLabels.field("producerBatchMode")
                + " must be NONE, JSON_ARRAY or XML_LIST, got: " + UiLabels.quote(batchMode));
    }
}
