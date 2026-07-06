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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

public class OffsetCommitTrackerTest {

    private static final TopicPartition TP0 = new TopicPartition("orders", 0);
    private static final TopicPartition TP1 = new TopicPartition("orders", 1);

    @Test
    public void markProcessedStoresOffsetPlusOne() {
        OffsetCommitTracker t = new OffsetCommitTracker();
        t.markProcessed(TP0, 5);
        Map<TopicPartition, OffsetAndMetadata> snap = t.snapshot();
        assertEquals(6L, snap.get(TP0).offset());
    }

    @Test
    public void markProcessedIsMonotonicAndIgnoresLowerOffsets() {
        OffsetCommitTracker t = new OffsetCommitTracker();
        t.markProcessed(TP0, 10);
        t.markProcessed(TP0, 3); // lower — must not lower the committed high-water mark
        assertEquals(11L, t.snapshot().get(TP0).offset());
    }

    @Test
    public void snapshotForReturnsOnlyRequestedPartitions() {
        OffsetCommitTracker t = new OffsetCommitTracker();
        t.markProcessed(TP0, 4);
        t.markProcessed(TP1, 9);
        Map<TopicPartition, OffsetAndMetadata> only0 =
                t.snapshotFor(Collections.singletonList(TP0));
        assertEquals(1, only0.size());
        assertEquals(5L, only0.get(TP0).offset());
        assertNull(only0.get(TP1));
    }

    @Test
    public void confirmRemovesCommittedEntries() {
        OffsetCommitTracker t = new OffsetCommitTracker();
        t.markProcessed(TP0, 7);
        Map<TopicPartition, OffsetAndMetadata> snap = t.snapshot();
        assertFalse(t.isEmpty());
        t.confirm(snap);
        assertTrue(t.isEmpty());
    }

    @Test
    public void confirmKeepsEntriesThatAdvancedBeyondSnapshot() {
        OffsetCommitTracker t = new OffsetCommitTracker();
        t.markProcessed(TP0, 7); // pending -> 8
        Map<TopicPartition, OffsetAndMetadata> snap = t.snapshot();
        t.markProcessed(TP0, 9); // pending advanced -> 10 after snapshot was taken
        t.confirm(snap);
        assertFalse("newer pending offset must survive confirm", t.isEmpty());
        assertEquals(10L, t.snapshot().get(TP0).offset());
    }

    @Test
    public void dropRemovesPartitionsWithoutCommitting() {
        OffsetCommitTracker t = new OffsetCommitTracker();
        t.markProcessed(TP0, 2);
        t.markProcessed(TP1, 3);
        t.drop(Collections.singletonList(TP0));
        assertNull(t.snapshot().get(TP0));
        assertEquals(4L, t.snapshot().get(TP1).offset());
    }

    @Test
    public void isEmptyOnFreshTracker() {
        assertTrue(new OffsetCommitTracker().isEmpty());
    }
}
