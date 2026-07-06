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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Verifies poison-pill recovery (KAFKA-16507 win): records that fail Avro
 * deserialization are routed to the DLQ topic with their raw bytes preserved,
 * the consumer advances past the failing offset via {@code seek(offset+1)},
 * and the main flow continues to consume valid records without reconnect storms.
 */
public class ConsumerPoisonPillIT {

    private static final String SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"Note\",\"namespace\":\"com.test\","
            + "\"fields\":[{\"name\":\"text\",\"type\":\"string\"}]}";

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
    public void testPoisonPillRoutedToDlqAndConsumerContinues() throws Exception {
        String topic = "it-poison-" + System.nanoTime();
        String dlqTopic = topic + "-dlq";
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        String registryUrl = KafkaTestInfrastructure.getSchemaRegistryUrl();

        // Order on partition 0: avro(k1)  ->  poison-pill(plain bytes)  ->  avro(k3)
        produceAvro(topic, registryUrl, "k1", schema, "first");
        producePlain(topic, "poison", "not-avro-bytes-junk".getBytes(StandardCharsets.UTF_8));
        produceAvro(topic, registryUrl, "k3", schema, "third");

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", "0");
        params.put("schemaRegistryEnabled", "true");
        params.put("schemaRegistryUrl", registryUrl);
        params.put("avroOutputFormat", "JSON");

        List<Exchange> captured = new ArrayList<Exchange>();
        Processor capturing = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                captured.add(exchange);
            }
        };

        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, "grp-poison-" + System.nanoTime(), params));
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, capturing);

        try {
            startAndPollUntil(consumer, 2);
        } finally {
            consumer.doStop();
        }

        // Main flow received 2 valid Avro records; poison-pill was skipped.
        Assert.assertEquals("Main flow should receive both valid Avro records",
                2, captured.size());

        // DLQ contains exactly the poison-pill record with raw bytes preserved.
        List<ConsumerRecord<String, String>> dlqRecords =
                KafkaTestInfrastructure.consumeAllMessages(dlqTopic, 1, 15000);
        Assert.assertEquals("Poison-pill should be routed to DLQ",
                1, dlqRecords.size());
        ConsumerRecord<String, String> dlq = dlqRecords.get(0);

        Assert.assertEquals("DLQ key must equal raw poison key", "poison", dlq.key());
        Assert.assertEquals("DLQ value must equal raw poison value bytes",
                "not-avro-bytes-junk", dlq.value());

        assertHeaderEquals(dlq, "CpiKafkaPlusDlqErrorType", "DESERIALIZATION");
        assertHeaderEquals(dlq, "CpiKafkaPlusDlqOriginalTopic", topic);
        assertHeaderEquals(dlq, "CpiKafkaPlusDlqOriginalPartition", "0");
        assertHeaderEquals(dlq, "CpiKafkaPlusDlqOriginalOffset", "1");

        Header errClass = dlq.headers().lastHeader("CpiKafkaPlusDlqErrorClass");
        Assert.assertNotNull("CpiKafkaPlusDlqErrorClass header missing", errClass);

        Header causeClass = dlq.headers().lastHeader("CpiKafkaPlusDlqCauseClass");
        Assert.assertNotNull("CpiKafkaPlusDlqCauseClass header missing", causeClass);
        Assert.assertTrue("Root cause should reference Kafka serialization layer",
                new String(causeClass.value(), StandardCharsets.UTF_8)
                        .contains("SerializationException"));
    }

    // ---- helpers ----

    private static void produceAvro(String topic, String registryUrl, String key,
                                    Schema schema, String text) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaTestInfrastructure.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", registryUrl);
        props.put("auto.register.schemas", true);

        KafkaProducer<String, GenericRecord> p = new KafkaProducer<String, GenericRecord>(props);
        try {
            GenericRecord rec = new GenericData.Record(schema);
            rec.put("text", text);
            p.send(new ProducerRecord<String, GenericRecord>(topic, 0, key, rec)).get();
        } finally {
            p.close();
        }
    }

    private static void producePlain(String topic, String key, byte[] value) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaTestInfrastructure.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());

        KafkaProducer<String, byte[]> p = new KafkaProducer<String, byte[]>(props);
        try {
            p.send(new ProducerRecord<String, byte[]>(topic, 0, key, value)).get();
        } finally {
            p.close();
        }
    }

    private static void startAndPollUntil(CpiKafkaPlusConsumer consumer, int targetExchanges)
            throws Exception {
        consumer.doStart();
        Method pollMethod = CpiKafkaPlusConsumer.class.getDeclaredMethod("poll");
        pollMethod.setAccessible(true);

        // We need multiple poll cycles: assignment, drain valid records, hit poison-pill,
        // route to DLQ + seek, then drain remaining records. 30 cycles * 500ms = 15s ceiling.
        int totalProcessed = 0;
        for (int i = 0; i < 30; i++) {
            int polled = (Integer) pollMethod.invoke(consumer);
            totalProcessed += polled;
            if (totalProcessed >= targetExchanges) {
                // One extra cycle in case there is anything else queued.
                pollMethod.invoke(consumer);
                return;
            }
            Thread.sleep(500);
        }
    }

    private static void assertHeaderEquals(ConsumerRecord<String, String> record,
                                            String headerKey, String expected) {
        Header h = record.headers().lastHeader(headerKey);
        Assert.assertNotNull("Header '" + headerKey + "' missing", h);
        Assert.assertEquals("Header '" + headerKey + "' value mismatch",
                expected, new String(h.value(), StandardCharsets.UTF_8));
    }
}
