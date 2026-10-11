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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KeepAliveTest {

    private static final Duration TIMEOUT = Duration.ofMillis(10);

    private final TopicPartition p0 = new TopicPartition("t", 0);
    private final TopicPartition p1 = new TopicPartition("t", 1);
    private MockConsumer<byte[], byte[]> consumer;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>("earliest");
        consumer.subscribe(Collections.singleton("t"));
        consumer.rebalance(Collections.singleton(p0));
        Map<TopicPartition, Long> beginning = new HashMap<>();
        beginning.put(p0, 0L);
        beginning.put(p1, 0L);
        consumer.updateBeginningOffsets(beginning);
    }

    @Test
    public void pausesTheAssignmentSoItsRecordsStayUnread() {
        consumer.addRecord(record(0, 0L));

        Assert.assertEquals(0, KeepAlive.pollPausedAndRewind(consumer, TIMEOUT));
        Assert.assertEquals(Collections.singleton(p0), consumer.paused());
        Assert.assertEquals(0L, consumer.position(p0));
    }

    @Test
    public void rewindsRecordsOfAPartitionAssignedDuringThePollAndLeavesItUnpaused() {
        consumer.schedulePollTask(() -> {
            consumer.rebalance(Arrays.asList(p0, p1));
            consumer.addRecord(record(1, 3L));
            consumer.addRecord(record(1, 4L));
        });
        Map<TopicPartition, Long> beginning = new HashMap<>();
        beginning.put(p1, 3L);
        consumer.updateBeginningOffsets(beginning);

        Assert.assertEquals(2, KeepAlive.pollPausedAndRewind(consumer, TIMEOUT));
        Assert.assertEquals("rewound to the first returned offset", 3L, consumer.position(p1));
        Assert.assertEquals("resuming is the caller's job; the new partition was never paused",
                Collections.singleton(p0), consumer.paused());
    }

    @Test
    public void usesTheCallersRewind() {
        consumer.schedulePollTask(() -> {
            consumer.rebalance(Arrays.asList(p0, p1));
            consumer.addRecord(record(1, 0L));
        });
        List<ConsumerRecords<byte[], byte[]>> rewound = new ArrayList<>();

        Assert.assertEquals(1, KeepAlive.pollPausedAndRewind(consumer, TIMEOUT, rewound::add));
        Assert.assertEquals(1, rewound.size());
        Assert.assertEquals("no default seek when the caller rewinds", 1L, consumer.position(p1));
    }

    @Test
    public void anEmptyPollRewindsNothing() {
        List<ConsumerRecords<byte[], byte[]>> rewound = new ArrayList<>();
        Assert.assertEquals(0, KeepAlive.pollPausedAndRewind(consumer, TIMEOUT, rewound::add));
        Assert.assertTrue(rewound.isEmpty());
    }

    @Test
    public void seekToFirstOffsetsTakesTheSmallestOffsetPerPartition() {
        consumer.rebalance(Arrays.asList(p0, p1));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> byPartition = new HashMap<>();
        byPartition.put(p0, Arrays.asList(record(0, 7L), record(0, 5L), record(0, 6L)));
        byPartition.put(p1, Collections.singletonList(record(1, 2L)));

        KeepAlive.seekToFirstOffsets(consumer, new ConsumerRecords<>(byPartition, Collections.emptyMap()));

        Assert.assertEquals(5L, consumer.position(p0));
        Assert.assertEquals(2L, consumer.position(p1));
    }

    private static ConsumerRecord<byte[], byte[]> record(int partition, long offset) {
        return new ConsumerRecord<>("t", partition, offset, null, new byte[] {1});
    }
}
