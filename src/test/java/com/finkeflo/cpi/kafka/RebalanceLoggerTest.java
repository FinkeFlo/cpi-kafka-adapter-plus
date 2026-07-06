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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

public class RebalanceLoggerTest {

    @Test
    public void formatPartitionsReturnsEmptyBracketsForNullAndEmpty() {
        assertEquals("[]", RebalanceLogger.formatPartitions(null));
        assertEquals("[]", RebalanceLogger.formatPartitions(Collections.<TopicPartition>emptyList()));
    }

    @Test
    public void formatPartitionsRendersTopicAndPartition() {
        String rendered = RebalanceLogger.formatPartitions(
                Collections.singletonList(new TopicPartition("orders", 7)));
        assertEquals("[orders-7]", rendered);
    }

    @Test
    public void formatPartitionsSortsByTopicThenPartition() {
        String rendered = RebalanceLogger.formatPartitions(Arrays.asList(
                new TopicPartition("z-topic", 1),
                new TopicPartition("a-topic", 2),
                new TopicPartition("a-topic", 0),
                new TopicPartition("z-topic", 0)));
        assertEquals("[a-topic-0, a-topic-2, z-topic-0, z-topic-1]", rendered);
    }

    @Test
    public void formatPartitionsIsStableAcrossInputOrdering() {
        String first = RebalanceLogger.formatPartitions(Arrays.asList(
                new TopicPartition("t", 2), new TopicPartition("t", 0), new TopicPartition("t", 1)));
        String second = RebalanceLogger.formatPartitions(Arrays.asList(
                new TopicPartition("t", 0), new TopicPartition("t", 1), new TopicPartition("t", 2)));
        assertEquals(first, second);
    }

    @Test
    public void implementsKafkaListenerInterfaceAndCallbacksDoNotThrow() {
        ConsumerRebalanceListener listener = new RebalanceLogger("group-A", "topic-A");
        listener.onPartitionsRevoked(Collections.singletonList(new TopicPartition("topic-A", 0)));
        listener.onPartitionsAssigned(Arrays.asList(
                new TopicPartition("topic-A", 0), new TopicPartition("topic-A", 1)));
        listener.onPartitionsLost(Collections.<TopicPartition>emptyList());
        listener.onPartitionsRevoked(null);
        listener.onPartitionsAssigned(null);
        listener.onPartitionsLost(null);
    }

    @Test
    public void onPartitionsRevokedInvokesCommitHook() {
        RecordingHook hook = new RecordingHook();
        ConsumerRebalanceListener listener = new RebalanceLogger("group-A", "topic-A", hook);
        List<TopicPartition> revoked = Collections.singletonList(new TopicPartition("topic-A", 0));

        listener.onPartitionsRevoked(revoked);

        assertEquals(revoked, hook.revoked);
        assertNull(hook.lost);
    }

    @Test
    public void onPartitionsLostInvokesCommitHook() {
        RecordingHook hook = new RecordingHook();
        ConsumerRebalanceListener listener = new RebalanceLogger("group-A", "topic-A", hook);
        List<TopicPartition> lost = Collections.singletonList(new TopicPartition("topic-A", 3));

        listener.onPartitionsLost(lost);

        assertEquals(lost, hook.lost);
        assertNull(hook.revoked);
    }

    @Test
    public void onPartitionsAssignedDoesNotInvokeCommitHook() {
        RecordingHook hook = new RecordingHook();
        ConsumerRebalanceListener listener = new RebalanceLogger("group-A", "topic-A", hook);

        listener.onPartitionsAssigned(Collections.singletonList(new TopicPartition("topic-A", 0)));

        assertNull(hook.revoked);
        assertNull(hook.lost);
    }

    @Test
    public void nullPartitionsDoNotInvokeCommitHook() {
        RecordingHook hook = new RecordingHook();
        ConsumerRebalanceListener listener = new RebalanceLogger("group-A", "topic-A", hook);

        listener.onPartitionsRevoked(null);
        listener.onPartitionsLost(null);

        assertNull(hook.revoked);
        assertNull(hook.lost);
    }

    private static final class RecordingHook implements RebalanceLogger.RebalanceCommitHook {
        private Collection<TopicPartition> revoked;
        private Collection<TopicPartition> lost;

        @Override
        public void onRevoked(Collection<TopicPartition> partitions) {
            this.revoked = partitions;
        }

        @Override
        public void onLost(Collection<TopicPartition> partitions) {
            this.lost = partitions;
        }
    }
}
