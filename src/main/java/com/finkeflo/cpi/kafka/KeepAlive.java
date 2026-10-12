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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * handle them differently. Only a failed seek back is handled here, see {@link #seekBack}, which the
 * emit poll's rewind uses as well. Package-private.</p>
 */
final class KeepAlive {

    private static final Logger LOG = LoggerFactory.getLogger(KeepAlive.class);

    private KeepAlive() {
    }

    /**
     * Pauses the current assignment, polls once and rewinds the returned records with
     * {@link #seekToFirstOffsets}. Exceptions of the pause and the poll propagate, and the partitions
     * stay paused; a failed seek is logged instead, see {@link #seekBack}.
     *
     * @return the number of records that were rewound
     */
    static int pollPausedAndRewind(Consumer<byte[], byte[]> consumer, Duration timeout) {
        return pollPausedAndRewind(consumer, timeout, records -> seekToFirstOffsets(consumer, records));
    }

    /**
     * As {@link #pollPausedAndRewind(Consumer, Duration)}, with a caller-supplied rewind for callers
     * that wrap {@link #seekToFirstOffsets} themselves.
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

    /**
     * Seeks every partition in {@code records} back to the smallest offset it returned. A failed seek
     * does not keep the other partitions from being rewound (#244), see {@link #seekBack}.
     */
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
            seekBack(consumer, e.getKey(), e.getValue());
        }
    }

    /**
     * Seeks {@code tp} back to {@code offset}, for the keep-alive polls and for the emit poll's
     * rewind. A failed seek is logged instead of thrown, so the caller goes on with its other
     * partitions. A partition revoked meanwhile needs no seek: its new owner starts from the
     * committed offset, which never passes an unprocessed record. A partition that is still assigned
     * keeps its position past records that were not processed, and the next commit can pass them, so
     * that is logged at ERROR (#244). An {@link Error} is not caught: the poll's failure handling
     * classifies it.
     *
     * @return whether the seek succeeded
     */
    static boolean seekBack(Consumer<byte[], byte[]> consumer, TopicPartition tp, long offset) {
        try {
            consumer.seek(tp, offset);
            return true;
        } catch (RuntimeException seekError) {
            if (isAssigned(consumer, tp)) {
                AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("consumer.rewind.failed")
                        .with("topic", tp.topic())
                        .with("partition", tp.partition())
                        .with("offset", offset)
                        .with("consequence", "the position stays past records that were not processed; "
                                + "the next commit can pass them"), seekError);
            } else {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] rewind: partition {} is no longer assigned, so it is not sought "
                        + "back to offset {}; its new owner starts from the committed offset: {}",
                        tp, offset, seekError.getMessage());
            }
            return false;
        }
    }

    private static boolean isAssigned(Consumer<byte[], byte[]> consumer, TopicPartition tp) {
        try {
            return consumer.assignment().contains(tp);
        } catch (RuntimeException e) {
            return true; // cannot tell, so assume the worse case
        }
    }
}
