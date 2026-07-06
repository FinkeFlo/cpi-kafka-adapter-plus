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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ConsumerRebalanceListener} that emits {@code [CPI-KAFKA-PLUS-DIAG]}
 * lines whenever the Kafka group coordinator hands out, takes back, or declares
 * lost partitions for the consumer of this pod.
 *
 * <p>The CPI tenant trace log only persists messages logged at {@code ERROR}
 * level, so all rebalance events are intentionally logged at {@code error} —
 * they are operationally critical for diagnosing group churn (Issue&nbsp;#45)
 * but otherwise low volume (only fires when the group changes shape).
 *
 * <p>This class is stateless and safe to share across pods. Tests instantiate
 * it directly and invoke the callbacks; production code wires it via
 * {@link org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(java.util.Collection,
 * ConsumerRebalanceListener)}.
 */
final class RebalanceLogger implements ConsumerRebalanceListener {

    private static final Logger LOG = LoggerFactory.getLogger(RebalanceLogger.class);

    /**
     * Optional hook invoked on rebalance transitions so pending offsets can be
     * committed while partitions are still owned (revoke) or dropped when they are
     * lost. Kept separate from the logging concern and free of any
     * {@code KafkaConsumer} dependency so this listener stays unit-testable.
     */
    interface RebalanceCommitHook {
        /** Commit pending offsets for partitions being revoked (still owned here). */
        void onRevoked(Collection<TopicPartition> partitions);
        /** Drop pending offsets for partitions that were lost (cannot be committed). */
        void onLost(Collection<TopicPartition> partitions);
    }

    private final String groupId;
    private final String topic;
    private final RebalanceCommitHook commitHook;

    RebalanceLogger(String groupId, String topic) {
        this(groupId, topic, null);
    }

    RebalanceLogger(String groupId, String topic, RebalanceCommitHook commitHook) {
        this.groupId = groupId;
        this.topic = topic;
        this.commitHook = commitHook;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOG.error("[CPI-KAFKA-PLUS-DIAG] rebalance: partitions REVOKED  count={} partitions={} group='{}' topic='{}'",
                partitions != null ? partitions.size() : 0,
                formatPartitions(partitions), groupId, topic);
        if (commitHook != null && partitions != null) {
            commitHook.onRevoked(partitions);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOG.error("[CPI-KAFKA-PLUS-DIAG] rebalance: partitions ASSIGNED count={} partitions={} group='{}' topic='{}'",
                partitions != null ? partitions.size() : 0,
                formatPartitions(partitions), groupId, topic);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        LOG.error("[CPI-KAFKA-PLUS-DIAG] rebalance: partitions LOST     count={} partitions={} group='{}' topic='{}'",
                partitions != null ? partitions.size() : 0,
                formatPartitions(partitions), groupId, topic);
        if (commitHook != null && partitions != null) {
            commitHook.onLost(partitions);
        }
    }

    /**
     * Renders the partition collection in a deterministic, easy-to-grep format
     * such as {@code [topicA-0, topicA-2, topicB-1]}. Sorted by topic, then
     * partition, so two log lines for the same assignment compare equal as
     * strings — useful when correlating events across pods.
     */
    static String formatPartitions(Collection<TopicPartition> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            return "[]";
        }
        List<TopicPartition> sorted = new ArrayList<>(partitions);
        Collections.sort(sorted, (a, b) -> {
            int byTopic = a.topic().compareTo(b.topic());
            return byTopic != 0 ? byTopic : Integer.compare(a.partition(), b.partition());
        });
        StringBuilder sb = new StringBuilder(sorted.size() * 16);
        sb.append('[');
        boolean first = true;
        for (TopicPartition tp : sorted) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(tp.topic()).append('-').append(tp.partition());
            first = false;
        }
        sb.append(']');
        return sb.toString();
    }
}
