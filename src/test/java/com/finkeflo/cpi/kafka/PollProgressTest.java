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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class PollProgressTest {

    private static final TopicPartition P0 = new TopicPartition("orders", 0);
    private static final TopicPartition P1 = new TopicPartition("orders", 1);

    @Test
    public void commitCoversOnlyTheResolvedPrefix() {
        PollProgress progress = new PollProgress(records(P0, 10, 11, 12, 13));

        progress.resolved(P0, 10);
        progress.resolved(P0, 12);

        Assert.assertEquals("11 is unresolved, so nothing at or after it may be committed",
                11L, progress.commitOffset(P0));
    }

    @Test
    public void outOfOrderResolutionCommitsOnceTheGapCloses() {
        PollProgress progress = new PollProgress(records(P0, 10, 11, 12, 13));

        progress.resolved(P0, 12);
        Assert.assertEquals(-1L, progress.commitOffset(P0));

        progress.resolved(P0, 10);
        progress.resolved(P0, 11);
        Assert.assertEquals(13L, progress.commitOffset(P0));

        progress.resolved(P0, 13);
        Assert.assertEquals(14L, progress.commitOffset(P0));
    }

    @Test
    public void nothingResolvedMeansNothingToCommit() {
        PollProgress progress = new PollProgress(records(P0, 10, 11));

        Assert.assertEquals(-1L, progress.commitOffset(P0));
        Assert.assertEquals(-1L, progress.commitOffset(new TopicPartition("other", 0)));
    }

    @Test
    public void blockedPartitionRewindsToTheBlockedOffset() {
        PollProgress progress = new PollProgress(records(P0, 5, 6, 7));

        progress.resolved(P0, 5);
        progress.blocked(P0, 6);

        Assert.assertTrue(progress.isBlocked(P0));
        Assert.assertEquals(Long.valueOf(6L), progress.blockedOffset(P0));
        Assert.assertEquals(6L, progress.commitOffset(P0));
        Assert.assertEquals(Collections.singletonMap(P0, 6L), progress.rewindPositions());
    }

    @Test
    public void firstBlockOfAPartitionWins() {
        PollProgress progress = new PollProgress(records(P0, 5, 6, 7));

        progress.blocked(P0, 6);
        progress.blocked(P0, 7);

        Assert.assertEquals(Long.valueOf(6L), progress.blockedOffset(P0));
    }

    @Test
    public void unprocessedRecordsAreRewoundPerPartition() {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> byPartition = new HashMap<>();
        byPartition.put(P0, list(P0, 1, 2, 3));
        byPartition.put(P1, list(P1, 7, 8));
        PollProgress progress = new PollProgress(new ConsumerRecords<>(byPartition));

        progress.resolved(P0, 1);
        progress.resolved(P1, 7);
        progress.resolved(P1, 8);

        // P1 is complete, P0 stopped after offset 1: only P0 is rewound, to its first open offset.
        Assert.assertEquals(Collections.singletonMap(P0, 2L), progress.rewindPositions());
    }

    @Test
    public void fullyResolvedPollNeedsNoRewind() {
        PollProgress progress = new PollProgress(records(P0, 1, 2));

        progress.resolved(P0, 1);
        progress.resolved(P0, 2);

        Assert.assertTrue(progress.rewindPositions().isEmpty());
        Assert.assertFalse(progress.isBlocked(P0));
    }

    @Test
    public void blockCurrentBlocksTheRecordBeingProcessed() {
        PollProgress progress = new PollProgress(records(P0, 1, 2, 3));

        progress.resolved(P0, 1);
        progress.processing(P0, 2);
        progress.blockCurrent();

        Assert.assertEquals(Long.valueOf(2L), progress.blockedOffset(P0));
    }

    @Test
    public void blockCurrentWithoutCurrentRecordIsANoOp() {
        PollProgress progress = new PollProgress(records(P0, 1));

        progress.blockCurrent();

        Assert.assertFalse(progress.isBlocked(P0));
    }

    @Test
    public void onlyFailuresAfterTheLastSuccessCount() {
        PollProgress progress = new PollProgress(records(P0, 1));

        progress.failed();
        progress.succeeded();
        progress.failed();
        progress.failed();

        Assert.assertEquals(1, progress.successes());
        Assert.assertEquals(2, progress.failuresSinceLastSuccess());
    }

    @Test
    public void stopIsSticky() {
        PollProgress progress = new PollProgress(records(P0, 1));

        Assert.assertFalse(progress.isStopped());
        progress.stop();
        Assert.assertTrue(progress.isStopped());
    }

    private static ConsumerRecords<byte[], byte[]> records(TopicPartition tp, long... offsets) {
        return new ConsumerRecords<>(Collections.singletonMap(tp, list(tp, offsets)));
    }

    private static List<ConsumerRecord<byte[], byte[]>> list(TopicPartition tp, long... offsets) {
        List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
        for (long offset : offsets) {
            out.add(new ConsumerRecord<>(tp.topic(), tp.partition(), offset, null, new byte[0]));
        }
        return out;
    }
}
