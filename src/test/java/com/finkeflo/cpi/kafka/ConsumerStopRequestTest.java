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

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * The record processor must learn about an undeploy between two records. Without it a stop is only
 * noticed when a Kafka call throws — and with {@code commitStrategy=AUTO} there is no such call, so
 * the rest of the poll would keep running through the route while the consumer is being closed.
 */
public class ConsumerStopRequestTest {

    @Test
    public void callbackReportsTheStopToTheRecordProcessor() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:orders?bootstrapServers=localhost:9092&groupId=g&securityProtocol=PLAINTEXT");
            CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, exchange -> { });

            consumer.doStart();
            Assert.assertFalse(consumer.getConsumerCallback().isStopRequested());

            consumer.doStop();
            Assert.assertTrue(consumer.getConsumerCallback().isStopRequested());
        }
    }
}
