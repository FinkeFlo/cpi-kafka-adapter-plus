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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 * What became of the records of one poll, per partition.
 *
 * <p>{@code poll()} moves the consumer position past every record it returns, and nothing moves it
 * back on its own. A record that was neither processed, dead-lettered nor deliberately skipped must
 * therefore be <em>rewound</em> to, and no commit may pass it — otherwise the next success in the
 * same partition commits over it and the record is never delivered again (issue #187).
 *
 * <p>A record is <em>resolved</em> once nothing more has to happen to it. A partition is
 * <em>blocked</em> at the first record that has to be retried; nothing at or after that offset is
 * committed, and the partition is rewound to it. Resolution may happen out of order (the JSON Schema
 * filter handles invalid records before the batches around them), so the commit position is always
 * the first unresolved polled offset, never the highest resolved one.
 *
 * <p>A poll also has a time limit, half of {@code max.poll.interval.ms}: once it has passed, no
 * further record, batch or retry is started, and what is still open comes with the next poll
 * (issue #192).
 *
 * <p>Single-threaded, like the poll loop that owns it: one instance per poll.
 */
final class PollProgress {

    private static final class PartitionState {
        private final List<Long> offsets = new ArrayList<>();
        private final Set<Long> resolved = new HashSet<>();
        private Long blockedAt;
        /** When the blocked record's next retry is due, if the poll's time ran out before it. */
        private Long deferredUntilMs;
        private long committedUpTo = -1L;

        /** First polled offset that is unresolved or blocked, or {@code null} if there is none. */
        Long firstOpenOffset() {
            for (Long offset : offsets) {
                if ((blockedAt != null && offset >= blockedAt) || !resolved.contains(offset)) {
                    return offset;
                }
            }
            return null;
        }
    }

    private final Map<TopicPartition, PartitionState> partitions = new LinkedHashMap<>();
    private final long startedMs;
    private final long timeLimitMs;
    private boolean stopped;
    private boolean outOfTime;
    private TopicPartition currentPartition;
    private long currentOffset;
    private int successes;
    private int failuresSinceLastSuccess;

    /** A poll without a time limit. */
    PollProgress(ConsumerRecords<byte[], byte[]> records) {
        this(records, 0L, Long.MAX_VALUE);
    }

    /**
     * @param startedMs   when the {@code poll()} that returned {@code records} was called; Kafka's
     *                    {@code max.poll.interval.ms} runs from there
     * @param timeLimitMs how long after {@code startedMs} a new record, batch or retry may still be
     *                    started
     */
    PollProgress(ConsumerRecords<byte[], byte[]> records, long startedMs, long timeLimitMs) {
        this.startedMs = startedMs;
        this.timeLimitMs = timeLimitMs;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            PartitionState state = partitions.get(tp);
            if (state == null) {
                state = new PartitionState();
                partitions.put(tp, state);
            }
            state.offsets.add(record.offset());
        }
    }

    /** The record needs nothing more: processed, dead-lettered or deliberately skipped. */
    void resolved(TopicPartition tp, long offset) {
        PartitionState state = partitions.get(tp);
        if (state != null) {
            state.resolved.add(offset);
        }
    }

    /**
     * The record has to be retried. Nothing at or after {@code offset} is committed, and the
     * partition is rewound to it. The first block of a partition wins.
     */
    void blocked(TopicPartition tp, long offset) {
        PartitionState state = partitions.get(tp);
        if (state != null && state.blockedAt == null) {
            state.blockedAt = offset;
        }
    }

    boolean isBlocked(TopicPartition tp) {
        return blockedOffset(tp) != null;
    }

    Long blockedOffset(TopicPartition tp) {
        PartitionState state = partitions.get(tp);
        return state != null ? state.blockedAt : null;
    }

    /**
     * The record at {@code offset} has retries left, but the poll's time ran out before the next
     * one. The partition is blocked at it like at a failed record, but it is not backed off as a
     * failure: the consumer pauses it until {@code dueMs}, when the next retry is due.
     */
    void deferred(TopicPartition tp, long offset, long dueMs) {
        PartitionState state = partitions.get(tp);
        if (state != null && state.blockedAt == null) {
            state.blockedAt = offset;
            state.deferredUntilMs = dueMs;
        }
    }

    /**
     * @return when the next retry of the record that {@code tp} is blocked at is due, or {@code null}
     *         if the partition is not blocked or blocked by a failure
     */
    Long deferredUntil(TopicPartition tp) {
        PartitionState state = partitions.get(tp);
        return state != null ? state.deferredUntilMs : null;
    }

    /** Shutdown or interrupt: nothing more is processed in this poll. */
    void stop() {
        stopped = true;
    }

    boolean isStopped() {
        return stopped;
    }

    /** @return the time from which no further record, batch or retry is started in this poll */
    long deadlineMs() {
        return timeLimitMs > Long.MAX_VALUE - startedMs ? Long.MAX_VALUE : startedMs + timeLimitMs;
    }

    long startedMs() {
        return startedMs;
    }

    long timeLimitMs() {
        return timeLimitMs;
    }

    /**
     * The poll's time is up: nothing more is started in it. Unlike {@link #stop()}, the consumer
     * keeps running and commits what was processed; what is still open is rewound and comes with
     * the next poll.
     */
    void outOfTime() {
        outOfTime = true;
    }

    boolean isOutOfTime() {
        return outOfTime;
    }

    /** Remembers the record being processed, so a Throwable escaping it can block it. */
    void processing(TopicPartition tp, long offset) {
        currentPartition = tp;
        currentOffset = offset;
    }

    /**
     * Blocks the record being processed. No-op if no record is being processed, or if it was already
     * resolved: a Throwable escaping after that point must not run the record a second time.
     */
    void blockCurrent() {
        if (currentPartition == null) {
            return;
        }
        PartitionState state = partitions.get(currentPartition);
        if (state != null && state.resolved.contains(currentOffset)) {
            return;
        }
        blocked(currentPartition, currentOffset);
    }

    /**
     * @return the offset to commit for {@code tp} (the next one to consume): the first unresolved
     *         polled offset, or the last polled offset + 1 if all are resolved; {@code -1} if not even
     *         the first polled record is resolved
     */
    long commitOffset(TopicPartition tp) {
        PartitionState state = partitions.get(tp);
        if (state == null || state.offsets.isEmpty()) {
            return -1L;
        }
        Long open = state.firstOpenOffset();
        if (open == null) {
            return state.offsets.get(state.offsets.size() - 1) + 1;
        }
        return open.equals(state.offsets.get(0)) ? -1L : open;
    }

    /** @return the commit offset last confirmed for {@code tp} in this poll, or {@code -1} */
    long committedUpTo(TopicPartition tp) {
        PartitionState state = partitions.get(tp);
        return state != null ? state.committedUpTo : -1L;
    }

    /** Records that {@code tp} was committed up to {@code offset}, so it is not committed again. */
    void committed(TopicPartition tp, long offset) {
        PartitionState state = partitions.get(tp);
        if (state != null && offset > state.committedUpTo) {
            state.committedUpTo = offset;
        }
    }

    /** @return per partition with an unresolved polled record, the offset to seek back to */
    Map<TopicPartition, Long> rewindPositions() {
        Map<TopicPartition, Long> out = new LinkedHashMap<>();
        for (Map.Entry<TopicPartition, PartitionState> e : partitions.entrySet()) {
            Long open = e.getValue().firstOpenOffset();
            if (open != null) {
                out.put(e.getKey(), open);
            }
        }
        return out;
    }

    Set<TopicPartition> partitions() {
        return partitions.keySet();
    }

    /** A route invocation succeeded (feeds auto-pause). */
    void succeeded() {
        successes++;
        failuresSinceLastSuccess = 0;
    }

    /** A record or batch failed, whatever happened to it afterwards (feeds auto-pause). */
    void failed() {
        failuresSinceLastSuccess++;
    }

    int successes() {
        return successes;
    }

    int failuresSinceLastSuccess() {
        return failuresSinceLastSuccess;
    }
}
