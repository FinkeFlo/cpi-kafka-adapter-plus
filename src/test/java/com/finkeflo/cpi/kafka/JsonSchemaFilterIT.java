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
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for JSON Schema filtering in the consumer pipeline with a real Kafka broker.
 */
public class JsonSchemaFilterIT {

    private static final String JSON_SCHEMA =
            "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"]}";

    private static DefaultCamelContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        KafkaTestInfrastructure.requireDockerAvailable();
        KafkaTestInfrastructure.startKafka();

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
    public void testBatchModeFiltersInvalidRecords() throws Exception {
        String topic = "it-schema-batch-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1", "k2", "k3"),
                Arrays.asList(
                        "{\"name\":\"Alice\"}",       // valid
                        "{\"age\":30}",                // invalid — missing "name"
                        "{\"name\":\"Charlie\"}"       // valid
                ));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "true");
        params.put("batchOutputFormat", "JSON_ARRAY");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("jsonSchemaValidation", "true");
        params.put("jsonSchema", URLEncoder.encode(JSON_SCHEMA, "UTF-8"));

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-schema-batch-" + System.nanoTime(),
                params, captured);

        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        Assert.assertFalse("Should have captured at least one batch exchange", captured.isEmpty());
        String body = captured.get(0).getIn().getBody(String.class);
        // Batch should contain only the 2 valid records
        Assert.assertTrue("Body should contain Alice", body.contains("Alice"));
        Assert.assertTrue("Body should contain Charlie", body.contains("Charlie"));
        Assert.assertFalse("Body should NOT contain the invalid record raw value",
                body.contains("\"age\":30") && !body.contains("\"name\""));

        Integer validationFailures = captured.get(0).getIn().getHeader(
                "CpiKafkaPlusSchemaValidationFailures", Integer.class);
        Assert.assertNotNull("Validation failures header should be set", validationFailures);
        Assert.assertEquals("One record should have failed validation", 1, (int) validationFailures);
    }

    @Test
    public void testInvalidRecordsRoutedToDlq() throws Exception {
        String topic = "it-schema-dlq-" + System.nanoTime();
        String dlqTopic = topic + "-dlq";
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1", "k2"),
                Arrays.asList(
                        "{\"name\":\"Valid\"}",
                        "{\"invalid\":true}"           // fails schema validation
                ));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "true");
        params.put("batchOutputFormat", "JSON_ARRAY");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("jsonSchemaValidation", "true");
        params.put("jsonSchema", URLEncoder.encode(JSON_SCHEMA, "UTF-8"));
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", "0");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-schema-dlq-" + System.nanoTime(),
                params, captured);

        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        // DLQ should receive the invalid record
        List<ConsumerRecord<String, String>> dlqRecords =
                KafkaTestInfrastructure.consumeAllMessages(dlqTopic, 1, 10000);
        Assert.assertEquals("Invalid record should be routed to DLQ", 1, dlqRecords.size());
        Assert.assertTrue("DLQ record should contain original value",
                dlqRecords.get(0).value().contains("invalid"));
    }

    @Test
    public void testSchemaValidationFailuresHeader() throws Exception {
        String topic = "it-schema-header-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1", "k2", "k3", "k4"),
                Arrays.asList(
                        "{\"name\":\"OK\"}",
                        "{\"bad\":1}",                 // invalid
                        "{\"also\":\"bad\"}",          // invalid
                        "{\"name\":\"Also OK\"}"
                ));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "true");
        params.put("batchOutputFormat", "JSON_ARRAY");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("jsonSchemaValidation", "true");
        params.put("jsonSchema", URLEncoder.encode(JSON_SCHEMA, "UTF-8"));

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-schema-count-" + System.nanoTime(),
                params, captured);

        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        Assert.assertFalse(captured.isEmpty());
        Integer failures = captured.get(0).getIn().getHeader(
                "CpiKafkaPlusSchemaValidationFailures", Integer.class);
        Assert.assertNotNull(failures);
        Assert.assertEquals("Two records should fail validation", 2, (int) failures);
    }

    @Test
    public void testSingleRecordModeSkipsInvalid() throws Exception {
        String topic = "it-schema-single-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1", "k2", "k3"),
                Arrays.asList(
                        "{\"name\":\"Valid1\"}",
                        "{\"no_name\":true}",          // invalid
                        "{\"name\":\"Valid2\"}"
                ));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("jsonSchemaValidation", "true");
        params.put("jsonSchema", URLEncoder.encode(JSON_SCHEMA, "UTF-8"));

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-schema-single-" + System.nanoTime(),
                params, captured);

        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        // Only the 2 valid records should be delivered to processor
        // (invalid record is silently dropped / routed to exception handler, returning 1)
        // The processor only receives valid records
        List<String> bodies = new ArrayList<>();
        for (Exchange ex : captured) {
            bodies.add(ex.getIn().getBody(String.class));
        }
        // Valid records should be processed
        Assert.assertTrue("Should contain Valid1", bodies.contains("{\"name\":\"Valid1\"}"));
        Assert.assertTrue("Should contain Valid2", bodies.contains("{\"name\":\"Valid2\"}"));
        // Invalid record should NOT reach the processor
        for (String body : bodies) {
            Assert.assertFalse("Invalid record should not reach processor", body.contains("no_name"));
        }
    }

    @Test
    public void testInvalidRecordOffsetCommitted() throws Exception {
        String topic = "it-schema-offset-" + System.nanoTime();
        String group = "grp-schema-offset-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1"),
                Arrays.asList("{\"no_name\":true}"));  // invalid

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("jsonSchemaValidation", "true");
        params.put("jsonSchema", URLEncoder.encode(JSON_SCHEMA, "UTF-8"));

        List<Exchange> captured1 = new ArrayList<>();
        CpiKafkaPlusConsumer consumer1 = createConsumer(topic, group, params, captured1);
        try {
            startAndPoll(consumer1);
        } finally {
            consumer1.doStop();
        }

        // Second consumer with same group — invalid record should NOT be re-delivered
        List<Exchange> captured2 = new ArrayList<>();
        CpiKafkaPlusConsumer consumer2 = createConsumer(topic, group, params, captured2);
        try {
            startAndPoll(consumer2);
        } finally {
            consumer2.doStop();
        }

        Assert.assertTrue("Invalid record should not be re-delivered after offset commit",
                captured2.isEmpty());
    }

    // --- Helpers ---

    private CpiKafkaPlusConsumer createConsumer(String topic, String groupId,
                                                 Map<String, String> params,
                                                 List<Exchange> captured) throws Exception {
        String uri = KafkaTestInfrastructure.buildEndpointUri(topic, groupId, params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);

        Processor capturingProcessor = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                captured.add(exchange);
            }
        };

        return new CpiKafkaPlusConsumer(endpoint, capturingProcessor);
    }

    private void startAndPoll(CpiKafkaPlusConsumer consumer) throws Exception {
        consumer.doStart();

        Method pollMethod = CpiKafkaPlusConsumer.class.getDeclaredMethod("poll");
        pollMethod.setAccessible(true);

        for (int i = 0; i < 10; i++) {
            int polled = (Integer) pollMethod.invoke(consumer);
            if (polled > 0) {
                break;
            }
            Thread.sleep(500);
        }
    }
}
