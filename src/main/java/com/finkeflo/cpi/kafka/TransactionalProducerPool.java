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
 *
 * <p>A message takes a slot with {@link #acquire()} and {@link #claimFreeSlot()} and gives it back
 * with {@link #release(int)} in a {@code finally}. Package-private.
 */
final class TransactionalProducerPool {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusProducer.class);

    static final Duration TXN_PRODUCER_CLOSE_TIMEOUT = Duration.ofSeconds(5);

    /** The adapter's own budget for a {@code transactional.id}; Kafka only needs a non-empty one. */
    private static final int MAX_TRANSACTIONAL_ID_LENGTH = 249;

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

    /**
     * A pool with {@code slots} free slots. {@link #start} resolves the topic hash and the member
     * suffix first; tests pass them directly.
     */
    TransactionalProducerPool(Supplier<String> transactionalIdPrefix, String topicHash,
                              String resolvedMemberSuffix, int slots) {
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.topicHash = topicHash;
        this.resolvedMemberSuffix = resolvedMemberSuffix;
        txnSlotSemaphore = new java.util.concurrent.Semaphore(slots, true);
        txnSlotInUse = new boolean[slots];
    }

    /**
     * The pool of a starting receiver: resolves the member suffix and the topic hash once, checks
     * the length of the resulting {@code transactional.id} and logs the start-up line.
     *
     * @param transactionalIdPrefix read again for every id
     * @param memberSuffix {@link NodeIdentity#memberSuffix()} outside tests; {@code null} from it
     *        means a random suffix and a WARN
     * @throws IllegalArgumentException if the id of the last slot would be longer than 249 characters
     */
    static TransactionalProducerPool start(Supplier<String> transactionalIdPrefix, String topic,
                                           Supplier<String> memberSuffix, int slots) {
        // Resolved once at startup and reused for every transactional.id below — avoids
        // re-reading env vars per exchange and prevents a literal "null" segment in the
        // transactional.id, which would cause fencing collisions between worker nodes that both
        // fail to resolve a suffix. If neither CF_INSTANCE_INDEX nor HOSTNAME is available, fall
        // back to a random-but-unique suffix rather than hard-failing startup: this keeps the
        // adapter usable in environments without those env vars, at the cost of the
        // transactional.id no longer being stable across restarts on the same node (so a
        // crashed producer's old transactional.id will not be actively fenced by the new one —
        // it will simply expire via transaction.timeout.ms).
        String resolvedMemberSuffix = memberSuffix.get();
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

        // Validate that every id fits within the length budget before any producer is created.
        // The last slot has the longest id: from slot 10 on, its number has two digits.
        String prefix = transactionalIdPrefix.get();
        String exampleId = formatTransactionalId(prefix, topicHash, resolvedMemberSuffix, 0);
        String longestId = formatTransactionalId(prefix, topicHash, resolvedMemberSuffix, Math.max(1, slots) - 1);
        if (longestId.length() > MAX_TRANSACTIONAL_ID_LENGTH) {
            int excess = longestId.length() - MAX_TRANSACTIONAL_ID_LENGTH;
            throw new IllegalArgumentException(UiLabels.field("transactionalIdPrefix") + " must be at most "
                    + (prefix.length() - excess) + " characters long, got: " + UiLabels.quote(prefix) + " ("
                    + prefix.length() + " characters). With " + slots + " for "
                    + UiLabels.field("maxConcurrentTransactions") + ", the transactional.id of the last "
                    + "slot, '" + longestId + "', would have " + longestId.length() + " characters, and the "
                    + "adapter allows at most " + MAX_TRANSACTIONAL_ID_LENGTH + ". Please shorten the prefix by "
                    + "at least " + excess + (excess == 1 ? " character." : " characters."));
        }

        TransactionalProducerPool pool = new TransactionalProducerPool(transactionalIdPrefix, topicHash,
                resolvedMemberSuffix, slots);
        LOG.info("[CPI-KAFKA-PLUS-DIAG] Transactional batching enabled with max {} concurrent transactions. "
                + "Prefix: {}, topicHash: {}, memberSuffix: {}, example transactional.id: {}",
                slots, transactionalIdPrefix.get(), topicHash, resolvedMemberSuffix,
                exampleId);
        return pool;
    }

    /**
     * Waits for a free slot: in arrival order (fair semaphore) and without a timeout (#204).
     *
     * @throws RuntimeException if the wait is interrupted; the interrupt flag is set again and no
     *         permit was taken, so there is nothing to {@link #release(int)}
     */
    void acquire() {
        try {
            txnSlotSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for a transaction slot", e);
        }
    }

    /**
     * Marks the lowest free slot as in use and returns it. Only after {@link #acquire()}, whose
     * permit guarantees a free slot.
     *
     * @throws IllegalStateException if no slot is free, which is a bug
     */
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

    /**
     * Frees {@code slotId} and returns the permit; once per successful {@link #acquire()}. With
     * {@code slotId} -1, when the claim failed, it only returns the permit.
     */
    void release(int slotId) {
        if (slotId != -1) {
            synchronized (txnSlotInUse) {
                txnSlotInUse[slotId] = false;
            }
        }
        txnSlotSemaphore.release();
    }

    /**
     * The {@code transactional.id} of {@code slotId}, with the prefix read on every call. The
     * caller computes it once per message, so every attempt of the message uses the same id.
     */
    String transactionalId(int slotId) {
        // resolvedMemberSuffix and topicHash are resolved once (fail-fast) in start() — never null here.
        return formatTransactionalId(transactionalIdPrefix.get(), topicHash, resolvedMemberSuffix, slotId);
    }

    /** The layout of every {@code transactional.id}; the start-up length check uses it too. */
    private static String formatTransactionalId(String prefix, String topicHash, String memberSuffix,
                                                int slotId) {
        return prefix + "-" + topicHash + "-" + memberSuffix + "-" + slotId;
    }

    /** For the context of a failed transactional send. */
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
