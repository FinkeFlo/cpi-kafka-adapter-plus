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
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 * The keep-alive poll shared by the regular keep-alive tick ({@link CpiKafkaPlusConsumer}) and the
 * auto-pause cooldown ({@link ConsumerCircuitBreaker}): pause the assignment, poll to drive the
 * group protocol, and rewind whatever the poll still returned.
 *
 * <p>A poll can return records although the assignment was paused before it: a partition that is
 * assigned during the poll is not paused yet. Without the rewind the consumer's position would
 * already be past those records and the next emit poll would never see them (N-D-1, #230).</p>
 *
 * <p>Error handling, the connection status and resuming the partitions stay with the callers, which
 * handle them differently. Package-private.</p>
 */
final class KeepAlive {

    private KeepAlive() {
    }

    /**
     * Pauses the current assignment, polls once and rewinds the returned records with
     * {@link #seekToFirstOffsets}. Exceptions propagate; the partitions stay paused.
     *
     * @return the number of records that were rewound
     */
    static int pollPausedAndRewind(Consumer<byte[], byte[]> consumer, Duration timeout) {
        return pollPausedAndRewind(consumer, timeout, records -> seekToFirstOffsets(consumer, records));
    }

    /**
     * As {@link #pollPausedAndRewind(Consumer, Duration)}, with a caller-supplied rewind for callers
     * that handle a failed seek themselves.
     */
    static int pollPausedAndRewind(Consumer<byte[], byte[]> consumer, Duration timeout,
            java.util.function.Consumer<ConsumerRecords<byte[], byte[]>> rewind) {
        consumer.pause(consumer.assignment());
        ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
        if (records == null || records.isEmpty()) {
            return 0;
        }
        rewind.accept(records);
        return records.count();
    }

    /** Seeks every partition in {@code records} back to the smallest offset it returned. */
    static void seekToFirstOffsets(Consumer<byte[], byte[]> consumer, ConsumerRecords<byte[], byte[]> records) {
        Map<TopicPartition, Long> firstOffsets = new HashMap<>();
        for (ConsumerRecord<byte[], byte[]> rec : records) {
            TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
            Long current = firstOffsets.get(tp);
            if (current == null || rec.offset() < current) {
                firstOffsets.put(tp, rec.offset());
            }
        }
        for (Map.Entry<TopicPartition, Long> e : firstOffsets.entrySet()) {
            consumer.seek(e.getKey(), e.getValue());
        }
    }
}
