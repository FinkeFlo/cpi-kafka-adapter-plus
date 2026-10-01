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

import java.lang.reflect.Method;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * A reconnect re-initializes the consumer's helpers. The DLQ producer must survive it instead of
 * being replaced: the replaced one was never closed and kept its network thread, its metadata
 * connection and its JMX registration (#171).
 */
public class ConsumerDlqHelperReuseTest {

    @After
    public void resetProbe() {
        TlsListenerProbe.clearCacheForTests();
    }

    @Test
    public void reinitializingTheHelpersKeepsOneDlqProducer() throws Exception {
        TlsListenerProbe.setProbeRunnerForTests(address -> TlsListenerProbe.Verdict.INCONCLUSIVE);
        String group = "g-dlq-reuse-" + System.nanoTime();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:orders?bootstrapServers=localhost:9092&groupId=" + group
                    + "&securityProtocol=PLAINTEXT&dlqEnabled=true&dlqTopic=orders-dlq");
            CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, exchange -> { });
            consumer.doStart();
            Method createHelpers = CpiKafkaPlusConsumer.class.getDeclaredMethod("createConsumerHelpers");
            createHelpers.setAccessible(true);
            try {
                createHelpers.invoke(consumer);
                // What a reconnect after failed polls or a fenced instance does next.
                createHelpers.invoke(consumer);

                Assert.assertEquals("one DLQ producer network thread per consumer",
                        1, dlqNetworkThreads(group));
            } finally {
                consumer.doStop();
            }
        }
    }

    private static int dlqNetworkThreads(String group) {
        String name = "kafka-producer-network-thread | cpi-kafka-plus-dlq-" + group;
        int count = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && name.equals(thread.getName())) {
                count++;
            }
        }
        return count;
    }
}
