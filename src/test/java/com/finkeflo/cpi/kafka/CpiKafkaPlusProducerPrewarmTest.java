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
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Assert;
import org.junit.Test;

public class CpiKafkaPlusProducerPrewarmTest {

    @Test
    public void failedPrewarmDoesNotBlockSecondAttempt() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx);
            String topic = "orders";

            try (ScriptedPartitionsProducer kafkaProducer = new ScriptedPartitionsProducer(true)) {
                setField(producer, "kafkaProducer", kafkaProducer);

                invokePrewarm(producer, topic);
                Assert.assertFalse(prewarmedTopics(producer).contains(topic));

                invokePrewarm(producer, topic);
                Assert.assertEquals("failed prewarm must not block retry", 2,
                        kafkaProducer.partitionsForCalls());
                Assert.assertTrue(prewarmedTopics(producer).contains(topic));
            }
        }
    }

    @Test
    public void successfulPrewarmSkipsRepeatedFetch() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx);
            String topic = "orders";

            try (ScriptedPartitionsProducer kafkaProducer = new ScriptedPartitionsProducer(false)) {
                setField(producer, "kafkaProducer", kafkaProducer);

                invokePrewarm(producer, topic);
                invokePrewarm(producer, topic);

                Assert.assertEquals("successful prewarm should be cached", 1,
                        kafkaProducer.partitionsForCalls());
                Assert.assertTrue(prewarmedTopics(producer).contains(topic));
            }
        }
    }

    private static CpiKafkaPlusProducer newProducer(DefaultCamelContext ctx) throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(ctx);
        endpoint.setBootstrapServers("localhost:9092");
        endpoint.setTopic("orders");
        endpoint.setProducerBatchMode("NONE");

        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        setField(producer, "tracingHelper", new AdapterTracingHelper(endpoint));
        return producer;
    }

    private static void invokePrewarm(CpiKafkaPlusProducer producer, String topic) throws Exception {
        Method method = CpiKafkaPlusProducer.class.getDeclaredMethod("prewarmTopicMetadata",
                String.class);
        method.setAccessible(true);
        method.invoke(producer, topic);
    }

    private static Set<String> prewarmedTopics(CpiKafkaPlusProducer producer) throws Exception {
        Field field = CpiKafkaPlusProducer.class.getDeclaredField("prewarmedTopics");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Set<String> topics = (Set<String>) field.get(producer);
        return topics;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final class ScriptedPartitionsProducer extends KafkaProducer<byte[], byte[]> {
        private final AtomicInteger partitionsForCalls = new AtomicInteger();
        private final boolean failFirst;

        ScriptedPartitionsProducer(boolean failFirst) {
            super(testProducerProps(), new ByteArraySerializer(), new ByteArraySerializer());
            this.failFirst = failFirst;
        }

        @Override
        public java.util.List<PartitionInfo> partitionsFor(String topic) {
            int call = partitionsForCalls.getAndIncrement();
            if (failFirst && call == 0) {
                throw new RuntimeException("synthetic prewarm failure");
            }
            return Collections.emptyList();
        }

        int partitionsForCalls() {
            return partitionsForCalls.get();
        }
    }

    private static Properties testProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "prewarm-test");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        return props;
    }
}
