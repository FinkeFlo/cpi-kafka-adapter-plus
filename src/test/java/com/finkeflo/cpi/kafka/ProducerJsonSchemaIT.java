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

import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for producer-side JSON Schema validation with a real Kafka broker.
 */
public class ProducerJsonSchemaIT {

    private static final String SCHEMA =
            "{\"type\":\"object\",\"required\":[\"id\"],\"properties\":{\"id\":{\"type\":\"string\"}}}";

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
    public void testValidMessageIsSent() throws Exception {
        String topic = "it-producer-schema-valid-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("jsonSchemaValidation", "true");
        params.put("jsonSchema", URLEncoder.encode(SCHEMA, "UTF-8"));

        CpiKafkaPlusProducer producer = createProducer(topic, params);
        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("{\"id\":\"abc-123\"}");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals("Exactly one message should have been sent", 1, records.size());
        Assert.assertEquals("Message body should match", "{\"id\":\"abc-123\"}", records.get(0).value());
    }

    @Test
    public void testInvalidMessageThrowsException() throws Exception {
        String topic = "it-producer-schema-invalid-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("jsonSchemaValidation", "true");
        params.put("jsonSchema", URLEncoder.encode(SCHEMA, "UTF-8"));

        CpiKafkaPlusProducer producer = createProducer(topic, params);
        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("{\"name\":\"no-id-field\"}");
            try {
                producer.process(exchange);
                Assert.fail("Should have thrown RuntimeException for invalid message");
            } catch (RuntimeException e) {
                Assert.assertTrue("Exception message should contain 'Outbound'",
                        e.getMessage().contains("Outbound"));
            }
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 5000);
        Assert.assertEquals("No message should have been sent to topic", 0, records.size());
    }

    @Test
    public void testReportErrorFlagDoesNotPreventException() throws Exception {
        String topic = "it-producer-schema-report-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("jsonSchemaValidation", "true");
        params.put("jsonSchema", URLEncoder.encode(SCHEMA, "UTF-8"));
        params.put("jsonSchemaReportError", "true");

        CpiKafkaPlusProducer producer = createProducer(topic, params);
        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("{\"name\":\"no-id-field\"}");
            try {
                producer.process(exchange);
                Assert.fail("Should have thrown RuntimeException even when jsonSchemaReportError=true");
            } catch (RuntimeException e) {
                Assert.assertTrue("Exception message should contain 'Outbound'",
                        e.getMessage().contains("Outbound"));
            }
        } finally {
            producer.doStop();
        }
    }

    // --- Helpers ---

    private CpiKafkaPlusProducer createProducer(String topic, Map<String, String> params) throws Exception {
        String uri = KafkaTestInfrastructure.buildEndpointUri(topic, "unused-group", params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
        return new CpiKafkaPlusProducer(endpoint);
    }
}
