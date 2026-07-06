/*-
 * #%L
 * SAP CPI Kafka Adapter Plus
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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Tracks the highest successfully-processed offset per {@link TopicPartition} that
 * still needs to be committed to Kafka. Acts as the source of truth for at-least-once
 * offset commits so that a commit lost during a rebalance
 * ({@code CommitFailedException} / {@code RebalanceInProgressException}) is retained
 * and re-committed on the next cycle or on partition revocation — instead of being
 * silently dropped, which previously left the committed offset behind the processed
 * position (phantom lag + duplicate re-delivery).
 *
 * <p>Offsets are stored as the <em>next</em> offset to consume ({@code recordOffset + 1}),
 * matching Kafka's {@link OffsetAndMetadata} commit semantics. Updates are monotonic:
 * a lower offset never lowers an already-recorded high-water mark.
 *
 * <p>The Kafka consumer (and its rebalance-listener callbacks) is single-threaded, so
 * no contention is expected; methods are nonetheless {@code synchronized} to keep the
 * tracker safe if the wiring ever changes.
 *
 * <p>Package-private — used by {@link RecordProcessor} and {@link CpiKafkaPlusConsumer}.
 */
final class OffsetCommitTracker {

    private final Map<TopicPartition, Long> pending = new HashMap<TopicPartition, Long>();

    /**
     * Records that {@code recordOffset} on {@code tp} was processed successfully.
     * Stores {@code recordOffset + 1} (the next offset to consume) and never lowers
     * an existing higher value.
     */
    synchronized void markProcessed(TopicPartition tp, long recordOffset) {
        long next = recordOffset + 1;
        Long current = pending.get(tp);
        if (current == null || next > current.longValue()) {
            pending.put(tp, Long.valueOf(next));
        }
    }

    /** @return a commit-ready snapshot of all pending offsets. */
    synchronized Map<TopicPartition, OffsetAndMetadata> snapshot() {
        Map<TopicPartition, OffsetAndMetadata> out =
                new HashMap<TopicPartition, OffsetAndMetadata>();
        for (Map.Entry<TopicPartition, Long> e : pending.entrySet()) {
            out.put(e.getKey(), new OffsetAndMetadata(e.getValue().longValue()));
        }
        return out;
    }

    /** @return a commit-ready snapshot restricted to {@code partitions}. */
    synchronized Map<TopicPartition, OffsetAndMetadata> snapshotFor(
            Collection<TopicPartition> partitions) {
        Map<TopicPartition, OffsetAndMetadata> out =
                new HashMap<TopicPartition, OffsetAndMetadata>();
        for (TopicPartition tp : partitions) {
            Long v = pending.get(tp);
            if (v != null) {
                out.put(tp, new OffsetAndMetadata(v.longValue()));
            }
        }
        return out;
    }

    /**
     * Marks the offsets in {@code committed} as durably committed and removes them
     * from the pending set — unless a newer (higher) offset has been recorded for a
     * partition since the snapshot was taken, in which case the newer value survives.
     */
    synchronized void confirm(Map<TopicPartition, OffsetAndMetadata> committed) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : committed.entrySet()) {
            Long cur = pending.get(e.getKey());
            if (cur != null && cur.longValue() <= e.getValue().offset()) {
                pending.remove(e.getKey());
            }
        }
    }

    /** Removes pending offsets for {@code partitions} without committing them. */
    synchronized void drop(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            pending.remove(tp);
        }
    }

    /** @return {@code true} if there are no pending offsets awaiting commit. */
    synchronized boolean isEmpty() {
        return pending.isEmpty();
    }
}
