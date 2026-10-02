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

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * A reconnect re-initializes the consumer's helpers. The Avro deserializer must survive it, like
 * the DLQ producer since #171: the replaced one was never closed, and its schema cache — schema IDs
 * are immutable — was thrown away on every reconnect.
 */
public class ConsumerAvroHelperReuseTest {

    @Test
    public void reinitializingTheHelpersKeepsTheAvroDeserializer() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:orders?bootstrapServers=localhost:9092&groupId=g-avro-reuse"
                    + "&securityProtocol=PLAINTEXT&schemaRegistryEnabled=true"
                    + "&schemaRegistryUrl=http://localhost:1&avroValueDeserialization=true");
            CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, exchange -> { });
            consumer.doStart();
            Method createHelpers = CpiKafkaPlusConsumer.class.getDeclaredMethod("createConsumerHelpers");
            createHelpers.setAccessible(true);
            try {
                createHelpers.invoke(consumer);
                Object first = avroHelper(consumer);
                // What a reconnect after failed polls or a fenced instance does next.
                createHelpers.invoke(consumer);

                Assert.assertNotNull("precondition: the Avro deserializer was created", first);
                Assert.assertSame("one Avro deserializer per consumer", first, avroHelper(consumer));
            } finally {
                consumer.doStop();
            }
        }
    }

    private static Object avroHelper(CpiKafkaPlusConsumer consumer) throws Exception {
        Field field = CpiKafkaPlusConsumer.class.getDeclaredField("avroHelper");
        field.setAccessible(true);
        return field.get(consumer);
    }
}
