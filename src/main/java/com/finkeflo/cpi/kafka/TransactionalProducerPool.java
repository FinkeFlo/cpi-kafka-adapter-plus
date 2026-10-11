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
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The transaction slots of a receiver with transactional batching (ADR 0001) and the
 * {@code transactional.id} each slot stands for, {@code <prefix>-<topicHash>-<memberSuffix>-<slot>}.
 * Also creates and closes the throw-away producer of a transactional attempt. Moved out of
 * {@link CpiKafkaPlusProducer}; it logs under the producer's logger category, so log levels and
 * trace filters set for the producer still apply.
 */
final class TransactionalProducerPool {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusProducer.class);

    static final Duration TXN_PRODUCER_CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private final java.util.concurrent.Semaphore txnSlotSemaphore;
    private final boolean[] txnSlotInUse;
    private final Supplier<String> transactionalIdPrefix;
    private final String resolvedMemberSuffix;
    /**
     * Short (8 hex-char) SHA-256 digest of the target topic name. Included in every
     * {@code transactional.id} so that two producers with the same
     * {@code transactionalIdPrefix} but different target topics never share an id —
     * which would cause them to fence each other on every call.
     */
    private final String topicHash;

    TransactionalProducerPool(Supplier<String> transactionalIdPrefix, String topicHash,
                              String resolvedMemberSuffix, int slots) {
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.topicHash = topicHash;
        this.resolvedMemberSuffix = resolvedMemberSuffix;
        txnSlotSemaphore = new java.util.concurrent.Semaphore(slots, true);
        txnSlotInUse = new boolean[slots];
    }

    static TransactionalProducerPool start(Supplier<String> transactionalIdPrefix, String topic, int slots) {
        // Resolved once at startup and reused for every transactional.id below — avoids
        // re-reading env vars per exchange and prevents a literal "null" segment in the
        // transactional.id, which would cause fencing collisions between worker nodes that both
        // fail to resolve a suffix. If neither CF_INSTANCE_INDEX nor HOSTNAME is available, fall
        // back to a random-but-unique suffix rather than hard-failing startup: this keeps the
        // adapter usable in environments without those env vars, at the cost of the
        // transactional.id no longer being stable across restarts on the same node (so a
        // crashed producer's old transactional.id will not be actively fenced by the new one —
        // it will simply expire via transaction.timeout.ms).
        String resolvedMemberSuffix = NodeIdentity.memberSuffix();
        if (resolvedMemberSuffix == null) {
            resolvedMemberSuffix = "r" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] Could not determine a stable identifier for this worker node, "
                    + "so a random one ('{}') will be used instead. This is safe, but note that it will "
                    + "change on every restart.",
                    resolvedMemberSuffix);
        }

        // A short hash of the target topic is included in every transactional.id so that two
        // producers configured with the same transactionalIdPrefix but different target topics
        // (a common copy-paste pattern) never share an id. Shared ids cause constant mutual
        // fencing: each new producer bumps the epoch and invalidates all concurrent ones.
        String topicHash = NodeIdentity.computeTopicHash(topic);

        // Validate that the resulting id fits within a safe length budget before any producer
        // is created. Prefix + hash + memberSuffix + slotId + separators must stay short enough
        // to be handled by all Kafka broker versions without truncation.
        String exampleId = transactionalIdPrefix.get() + "-" + topicHash
                + "-" + resolvedMemberSuffix + "-0";
        if (exampleId.length() > 249) {
            throw new IllegalArgumentException(
                    "The computed transactional.id '" + exampleId + "' is " + exampleId.length()
                    + " characters long. Please shorten transactionalIdPrefix to keep it under 249 characters.");
        }

        TransactionalProducerPool pool = new TransactionalProducerPool(transactionalIdPrefix, topicHash,
                resolvedMemberSuffix, slots);
        LOG.info("[CPI-KAFKA-PLUS-DIAG] Transactional batching enabled with max {} concurrent transactions. "
                + "Prefix: {}, topicHash: {}, memberSuffix: {}, example transactional.id: {}",
                slots, transactionalIdPrefix.get(), topicHash, resolvedMemberSuffix,
                exampleId);
        return pool;
    }

    void acquire() {
        try {
            txnSlotSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for a transaction slot", e);
        }
    }

    int claimFreeSlot() {
        int slotId = -1;
        synchronized (txnSlotInUse) {
            for (int i = 0; i < txnSlotInUse.length; i++) {
                if (!txnSlotInUse[i]) {
                    txnSlotInUse[i] = true;
                    slotId = i;
                    break;
                }
            }
        }
        if (slotId == -1) {
            throw new IllegalStateException("Acquired semaphore but no slot was free. This is a bug.");
        }
        return slotId;
    }

    void release(int slotId) {
        if (slotId != -1) {
            synchronized (txnSlotInUse) {
                txnSlotInUse[slotId] = false;
            }
        }
        txnSlotSemaphore.release();
    }

    String transactionalId(int slotId) {
        // resolvedMemberSuffix and topicHash are resolved once (fail-fast) in doStart() — never null here.
        return transactionalIdPrefix.get() + "-" + topicHash
                + "-" + resolvedMemberSuffix + "-" + slotId;
    }

    String topicHash() {
        return topicHash;
    }

    /**
     * Bounded close of a throw-away transactional producer. An unreachable broker must not hang
     * here indefinitely, since the transaction slot is only released once every attempt has closed
     * its producer. Without a timeout, a broker outage could permanently exhaust all txn slots.
     */
    static void closeTxnProducerQuietly(KafkaProducer<byte[], byte[]> txnProducer, int slotId) {
        if (txnProducer == null) {
            return;
        }
        try {
            txnProducer.close(TXN_PRODUCER_CLOSE_TIMEOUT);
        } catch (Exception closeEx) {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] Failed to close transactional producer for slot {}: {}",
                    slotId, closeEx.getMessage(), closeEx);
        }
    }

    /** Default implementation behind {@link CpiKafkaPlusProducer#txnProducerFactory}. */
    static KafkaProducer<byte[], byte[]> createTransactionalProducer(Properties props) {
        try {
            return BundleBackedClassLoader.withBundleClassLoader(CpiKafkaPlusProducer.class,
                    () -> new KafkaProducer<>(props,
                            new org.apache.kafka.common.serialization.ByteArraySerializer(),
                            new org.apache.kafka.common.serialization.ByteArraySerializer()));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            // The callable itself only throws unchecked exceptions; the checked signature comes
            // from the class-loader wrapper. Wrapped rather than swallowed so the cause chain — and
            // with it the retry classification — stays intact.
            throw new org.apache.kafka.common.KafkaException(
                    "Failed to create the transactional producer", e);
        }
    }
}
