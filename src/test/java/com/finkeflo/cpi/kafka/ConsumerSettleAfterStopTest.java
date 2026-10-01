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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

/**
 * doStop() gives the poll thread a bounded time to finish and then clears the consumer's state. A
 * poll thread that outlives that wait still settles its poll; it must not fail there with a
 * NullPointerException, which would replace the real failure in the trace.
 */
public class ConsumerSettleAfterStopTest {

    @Test
    public void settlingAPollAfterStopDoesNotThrow() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:orders?bootstrapServers=localhost:9092&groupId=g&securityProtocol=PLAINTEXT");
            // Never initialized, i.e. the state doStop() leaves behind: no record processor,
            // no backoff, no Kafka consumer.
            CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, exchange -> { });

            TopicPartition tp = new TopicPartition("orders", 0);
            PollProgress progress = new PollProgress(new ConsumerRecords<>(Collections.singletonMap(tp,
                    Collections.singletonList(new ConsumerRecord<>("orders", 0, 5L, (byte[]) null, new byte[0])))));
            progress.blocked(tp, 5L);

            Method settle = CpiKafkaPlusConsumer.class.getDeclaredMethod("settlePoll", PollProgress.class);
            settle.setAccessible(true);
            try {
                settle.invoke(consumer, progress);
            } catch (InvocationTargetException e) {
                throw new AssertionError("settlePoll after stop threw " + e.getCause(), e.getCause());
            }
        }
    }
}
