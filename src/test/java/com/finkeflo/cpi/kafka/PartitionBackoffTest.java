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

import java.util.Collections;

import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

public class PartitionBackoffTest {

    private static final TopicPartition P0 = new TopicPartition("orders", 0);
    private static final TopicPartition P1 = new TopicPartition("orders", 1);

    @Test
    public void delayDoublesForTheSameOffsetUpToTheCap() {
        PartitionBackoff backoff = new PartitionBackoff(1_000L, 5_000L);

        Assert.assertEquals(1_000L, backoff.blocked(P0, 7, 0L));
        Assert.assertEquals(2_000L, backoff.blocked(P0, 7, 0L));
        Assert.assertEquals(4_000L, backoff.blocked(P0, 7, 0L));
        Assert.assertEquals(5_000L, backoff.blocked(P0, 7, 0L));
        Assert.assertEquals(5_000L, backoff.blocked(P0, 7, 0L));
    }

    @Test
    public void manyAttemptsDoNotOverflowTheDelay() {
        PartitionBackoff backoff = new PartitionBackoff(1_000L, 300_000L);
        long delay = 0L;
        for (int i = 0; i < 200; i++) {
            delay = backoff.blocked(P0, 7, 0L);
        }
        Assert.assertEquals(300_000L, delay);
    }

    @Test
    public void anotherOffsetRestartsAtTheBaseDelay() {
        PartitionBackoff backoff = new PartitionBackoff(1_000L, 300_000L);
        backoff.blocked(P0, 7, 0L);
        backoff.blocked(P0, 7, 0L);

        Assert.assertEquals(1_000L, backoff.blocked(P0, 8, 0L));
    }

    @Test
    public void partitionWaitsUntilItsDelayHasPassed() {
        PartitionBackoff backoff = new PartitionBackoff(1_000L, 300_000L);
        backoff.blocked(P0, 7, 10_000L);

        Assert.assertEquals(Collections.singleton(P0), backoff.waiting(10_999L));
        Assert.assertTrue(backoff.due(10_999L).isEmpty());

        Assert.assertTrue(backoff.waiting(11_000L).isEmpty());
        Assert.assertEquals(Collections.singleton(P0), backoff.due(11_000L));
    }

    @Test
    public void partitionsBackOffIndependently() {
        PartitionBackoff backoff = new PartitionBackoff(1_000L, 300_000L);
        backoff.blocked(P0, 7, 0L);
        backoff.blocked(P0, 7, 0L);
        backoff.blocked(P1, 3, 0L);

        // P0 waits 2 s (second attempt), P1 1 s.
        Assert.assertEquals(Collections.singleton(P0), backoff.waiting(1_500L));
        Assert.assertEquals(Collections.singleton(P1), backoff.due(1_500L));
    }

    @Test
    public void progressForgetsThePartition() {
        PartitionBackoff backoff = new PartitionBackoff(1_000L, 300_000L);
        backoff.blocked(P0, 7, 0L);
        backoff.blocked(P0, 7, 0L);

        backoff.progressed(P0);

        Assert.assertTrue(backoff.waiting(0L).isEmpty());
        Assert.assertTrue(backoff.due(0L).isEmpty());
        Assert.assertEquals("a new failure starts over", 1_000L, backoff.blocked(P0, 7, 0L));
    }

    @Test
    public void revokedPartitionsAreForgotten() {
        PartitionBackoff backoff = new PartitionBackoff(1_000L, 300_000L);
        backoff.blocked(P0, 7, 0L);
        backoff.blocked(P1, 3, 0L);

        backoff.forget(Collections.singleton(P0));

        Assert.assertEquals(Collections.singleton(P1), backoff.waiting(0L));
    }
}
