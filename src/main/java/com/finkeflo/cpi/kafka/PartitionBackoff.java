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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;

/**
 * Exponential backoff for partitions blocked at a record that has to be retried.
 *
 * <p>A blocked partition is rewound and retried on a later poll. Without a delay that retry would
 * run on the very next poll — every second in STREAMING mode — and write a failed message
 * processing log each time. The consumer pauses a waiting partition, so the other partitions keep
 * flowing. The delay doubles for every further failure at the same offset, up to a cap; a failure
 * at another offset, or any progress, starts over.
 *
 * <p>Single-threaded, like the poll loop that owns it.
 */
final class PartitionBackoff {

    private static final class State {
        private final long offset;
        private int attempts;
        private long notBeforeMs;

        State(long offset) {
            this.offset = offset;
        }
    }

    private final long baseDelayMs;
    private final long maxDelayMs;
    private final Map<TopicPartition, State> states = new HashMap<>();

    PartitionBackoff(long baseDelayMs, long maxDelayMs) {
        this.baseDelayMs = baseDelayMs;
        this.maxDelayMs = maxDelayMs;
    }

    /** @return the delay before {@code tp} may be retried */
    long blocked(TopicPartition tp, long offset, long nowMs) {
        State state = states.get(tp);
        if (state == null || state.offset != offset) {
            state = new State(offset);
            states.put(tp, state);
        }
        state.attempts++;
        int shift = Math.min(state.attempts - 1, 30);
        long delayMs = Math.min(baseDelayMs * (1L << shift), maxDelayMs);
        state.notBeforeMs = nowMs + delayMs;
        return delayMs;
    }

    /** @return partitions whose delay has not yet passed */
    Set<TopicPartition> waiting(long nowMs) {
        Set<TopicPartition> out = new LinkedHashSet<>();
        for (Map.Entry<TopicPartition, State> e : states.entrySet()) {
            if (nowMs < e.getValue().notBeforeMs) {
                out.add(e.getKey());
            }
        }
        return out;
    }

    /** @return partitions whose delay has passed and that may be retried now */
    Set<TopicPartition> due(long nowMs) {
        Set<TopicPartition> out = new LinkedHashSet<>();
        for (Map.Entry<TopicPartition, State> e : states.entrySet()) {
            if (nowMs >= e.getValue().notBeforeMs) {
                out.add(e.getKey());
            }
        }
        return out;
    }

    /** The partition made progress: the next failure starts at the base delay again. */
    void progressed(TopicPartition tp) {
        states.remove(tp);
    }

    /** Revoked or lost partitions: the next owner starts without a delay. */
    void forget(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            states.remove(tp);
        }
    }
}
