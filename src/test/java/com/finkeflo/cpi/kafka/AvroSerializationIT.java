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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

/**
 * End-to-end Avro serialization test with real Kafka and Schema Registry containers.
 */
public class AvroSerializationIT {

    private static DefaultCamelContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        KafkaTestInfrastructure.requireDockerAvailable();
        KafkaTestInfrastructure.startKafkaWithSchemaRegistry();

        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (ctx != null) {
            ctx.stop();
        }
    }

    @Test
    public void testProducerSerializesJsonToAvroAndRegistersSchema() throws Exception {
        String topic = "it-avro-roundtrip-" + System.nanoTime();
        String subject = topic + "-value";
        KafkaTestInfrastructure.createTopic(topic, 1);

        String sentJson = "{\"name\":\"Ada\",\"age\":37,\"email\":\"ada@example.com\"}";
        CpiKafkaPlusProducer producer = createAvroProducer(topic);
        try {
            producer.doStart();

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody(sentJson);
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(
                Collections.singletonList(KafkaTestInfrastructure.getSchemaRegistryUrl()), 100);
        Collection<String> subjects = registryClient.getAllSubjects();
        Assert.assertTrue("Schema Registry should contain subject " + subject, subjects.contains(subject));

        SchemaMetadata metadata = registryClient.getLatestSchemaMetadata(subject);
        Assert.assertTrue("Registered schema should include inferred name field",
                metadata.getSchema().contains("\"name\":\"name\""));
        Assert.assertTrue("Registered schema should include inferred long age field",
                metadata.getSchema().contains("\"name\":\"age\",\"type\":\"long\""));

        GenericRecord record = consumeOneAvroRecord(topic);
        Assert.assertNotNull("Expected one Avro record from Kafka", record);
        Assert.assertEquals("Ada", record.get("name").toString());
        Assert.assertEquals(Long.valueOf(37L), record.get("age"));
        Assert.assertEquals("ada@example.com", record.get("email").toString());
    }

    private CpiKafkaPlusProducer createAvroProducer(String topic) throws Exception {
        Map<String, String> params = new HashMap<String, String>();
        params.put("schemaRegistryEnabled", "true");
        params.put("schemaRegistryUrl", KafkaTestInfrastructure.getSchemaRegistryUrl());
        params.put("avroValueSerialization", "true");
        params.put("autoRegisterSchemas", "true");
        params.put("subjectNameStrategy", "TopicNameStrategy");
        String uri = KafkaTestInfrastructure.buildEndpointUri(topic, "unused-avro-group", params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
        return new CpiKafkaPlusProducer(endpoint);
    }

    private GenericRecord consumeOneAvroRecord(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaTestInfrastructure.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-avro-consumer-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", KafkaTestInfrastructure.getSchemaRegistryUrl());
        props.put("specific.avro.reader", false);

        try (KafkaConsumer<byte[], Object> consumer = new KafkaConsumer<byte[], Object>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 10000L;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], Object> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], Object> record : records) {
                    return (GenericRecord) record.value();
                }
            }
        }
        return null;
    }
}
