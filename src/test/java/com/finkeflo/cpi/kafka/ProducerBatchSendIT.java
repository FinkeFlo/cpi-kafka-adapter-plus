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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for producer batch send with a real Kafka broker.
 * Tests JSON_ARRAY and XML_LIST batch modes, response headers/body,
 * key handling, round-trip symmetry, error cases, and throughput.
 */
public class ProducerBatchSendIT {

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

    // -----------------------------------------------------------------------
    //  JSON_ARRAY Batch Send
    // -----------------------------------------------------------------------

    @Test
    public void testJsonBatchSend() throws Exception {
        String topic = "it-batch-json-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        try {
            producer.doStart();

            String json = "[" +
                    "{\"key\": \"k1\", \"value\": \"msg1\"}," +
                    "{\"key\": \"k2\", \"value\": \"msg2\"}," +
                    "{\"key\": \"k3\", \"value\": \"msg3\"}" +
                    "]";

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody(json);
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 3, 10000);
        Assert.assertEquals(3, records.size());
        Assert.assertEquals("k1", records.get(0).key());
        Assert.assertEquals("msg1", records.get(0).value());
        Assert.assertEquals("k2", records.get(1).key());
        Assert.assertEquals("msg2", records.get(1).value());
        Assert.assertEquals("k3", records.get(2).key());
        Assert.assertEquals("msg3", records.get(2).value());
    }

    @Test
    public void testJsonBatchSendWithObjectValues() throws Exception {
        String topic = "it-batch-json-obj-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        try {
            producer.doStart();

            String json = "[{\"key\": \"order-1\", \"value\": {\"id\": 1, \"amount\": 99.90}}]";

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody(json);
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("order-1", records.get(0).key());
        Assert.assertTrue("Value should be JSON object",
                records.get(0).value().contains("\"id\":1"));
    }

    @Test
    public void testJsonBatchKeyFallback() throws Exception {
        String topic = "it-batch-json-keyfb-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        try {
            producer.doStart();

            // Record without key + fallback header
            String json = "[{\"value\": \"no-key-msg\"}]";

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody(json);
            exchange.getIn().setHeader("kafka.KEY", "fallback-key");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("fallback-key", records.get(0).key());
    }

    @Test
    public void testJsonBatchKeyPrecedence() throws Exception {
        String topic = "it-batch-json-keypre-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        try {
            producer.doStart();

            // Record with key + fallback header — record key wins
            String json = "[{\"key\": \"record-key\", \"value\": \"msg\"}]";

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody(json);
            exchange.getIn().setHeader("kafka.KEY", "fallback-key");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals("record-key", records.get(0).key());
    }

    // -----------------------------------------------------------------------
    //  XML_LIST Batch Send
    // -----------------------------------------------------------------------

    @Test
    public void testXmlBatchSend() throws Exception {
        String topic = "it-batch-xml-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "XML_LIST");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        try {
            producer.doStart();

            String xml = "<kafkaRecords>" +
                    "<record><key>k1</key><value>msg1</value></record>" +
                    "<record><key>k2</key><value>msg2</value></record>" +
                    "<record><key>k3</key><value>msg3</value></record>" +
                    "</kafkaRecords>";

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody(xml);
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 3, 10000);
        Assert.assertEquals(3, records.size());
        Assert.assertEquals("k1", records.get(0).key());
        Assert.assertEquals("msg1", records.get(0).value());
        Assert.assertEquals("k3", records.get(2).key());
        Assert.assertEquals("msg3", records.get(2).value());
    }

    @Test
    public void testXmlBatchSendWithCdata() throws Exception {
        String topic = "it-batch-xml-cdata-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "XML_LIST");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        try {
            producer.doStart();

            String xml = "<kafkaRecords>" +
                    "<record><key>k1</key>" +
                    "<value><![CDATA[<order><id>1</id><amount>99.90</amount></order>]]></value>" +
                    "</record></kafkaRecords>";

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody(xml);
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("<order><id>1</id><amount>99.90</amount></order>",
                records.get(0).value());
    }

    // -----------------------------------------------------------------------
    //  Response Headers & Body
    // -----------------------------------------------------------------------

    @Test
    public void testResponseHeadersAndBody() throws Exception {
        String topic = "it-batch-resp-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        Exchange exchange = new DefaultExchange(ctx);
        try {
            producer.doStart();

            String json = "[" +
                    "{\"key\": \"k1\", \"value\": \"v1\"}," +
                    "{\"key\": \"k2\", \"value\": \"v2\"}," +
                    "{\"key\": \"k3\", \"value\": \"v3\"}" +
                    "]";

            exchange.getIn().setBody(json);
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        Message out = exchange.getIn();

        // Headers
        Assert.assertEquals(topic, out.getHeader("SAP_Receiver"));
        Assert.assertEquals(topic, out.getHeader("CamelKafkaTopic"));
        Assert.assertEquals(3, out.getHeader("CpiKafkaPlusRecordCount"));
        Assert.assertEquals("JSON_ARRAY", out.getHeader("CpiKafkaPlusBatchInputFormat"));
        Assert.assertNotNull(out.getHeader("CpiKafkaPlusFirstOffset"));
        Assert.assertNotNull(out.getHeader("CpiKafkaPlusLastOffset"));
        Assert.assertNotNull(out.getHeader("CpiKafkaPlusPartitions"));

        // Body — XML summary
        String body = out.getBody(String.class);
        Assert.assertTrue("Body should be XML", body.contains("<kafkaBatchResult>"));
        Assert.assertTrue("Body should contain recordCount",
                body.contains("<recordCount>3</recordCount>"));
        Assert.assertTrue("Body should contain status OK",
                body.contains("<status>OK</status>"));
        Assert.assertTrue("Body should contain topic",
                body.contains("<topic>" + topic + "</topic>"));
        Assert.assertTrue("Body should contain durationMs",
                body.contains("<durationMs>"));
    }

    // -----------------------------------------------------------------------
    //  Round-Trip: Consumer JSON_ARRAY → Producer JSON_ARRAY
    // -----------------------------------------------------------------------

    @Test
    public void testRoundTripJsonArray() throws Exception {
        String sourceTopic = "it-batch-rt-src-" + System.nanoTime();
        String targetTopic = "it-batch-rt-tgt-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(sourceTopic, 1);
        KafkaTestInfrastructure.createTopic(targetTopic, 1);

        // Step 1: Produce 3 messages to source topic
        java.util.List<String> keys = java.util.Arrays.asList("r1", "r2", "r3");
        java.util.List<String> values = java.util.Arrays.asList("val1", "val2", "val3");
        KafkaTestInfrastructure.produceStringMessages(sourceTopic, keys, values);

        // Step 2: Consume from source topic and format as JSON_ARRAY
        List<ConsumerRecord<String, String>> sourceRecords =
                KafkaTestInfrastructure.consumeAllMessages(sourceTopic, 3, 10000);
        Assert.assertEquals(3, sourceRecords.size());

        // Simulate BatchFormatter.toJsonArray output (consumer output format)
        StringBuilder consumerOutput = new StringBuilder("[");
        for (int i = 0; i < sourceRecords.size(); i++) {
            ConsumerRecord<String, String> r = sourceRecords.get(i);
            if (i > 0) consumerOutput.append(",");
            consumerOutput.append("{\"key\":\"").append(r.key())
                    .append("\",\"value\":\"").append(r.value())
                    .append("\",\"topic\":\"").append(r.topic())
                    .append("\",\"partition\":").append(r.partition())
                    .append(",\"offset\":").append(r.offset())
                    .append(",\"timestamp\":").append(r.timestamp())
                    .append("}");
        }
        consumerOutput.append("]");

        // Step 3: Send consumer output as producer batch input (round-trip)
        Map<String, String> producerParams = new HashMap<>();
        producerParams.put("producerBatchMode", "JSON_ARRAY");
        CpiKafkaPlusProducer producer = createProducer(targetTopic, producerParams);

        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody(consumerOutput.toString());
            producer.process(exchange);

            Assert.assertEquals(3,
                    exchange.getIn().getHeader("CpiKafkaPlusRecordCount"));
        } finally {
            producer.doStop();
        }

        // Step 4: Verify target topic has the same records
        List<ConsumerRecord<String, String>> targetRecords =
                KafkaTestInfrastructure.consumeAllMessages(targetTopic, 3, 10000);
        Assert.assertEquals(3, targetRecords.size());
        Assert.assertEquals("r1", targetRecords.get(0).key());
        Assert.assertEquals("val1", targetRecords.get(0).value());
        Assert.assertEquals("r2", targetRecords.get(1).key());
        Assert.assertEquals("val2", targetRecords.get(1).value());
        Assert.assertEquals("r3", targetRecords.get(2).key());
        Assert.assertEquals("val3", targetRecords.get(2).value());
    }

    // -----------------------------------------------------------------------
    //  Error Cases
    // -----------------------------------------------------------------------

    @Test(expected = RuntimeException.class)
    public void testJsonBatchInvalidFormat() throws Exception {
        String topic = "it-batch-err-fmt-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("{\"not\": \"an array\"}");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }
    }

    @Test(expected = RuntimeException.class)
    public void testJsonBatchEmptyBody() throws Exception {
        String topic = "it-batch-err-empty-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }
    }

    @Test(expected = RuntimeException.class)
    public void testXmlBatchWrongRoot() throws Exception {
        String topic = "it-batch-err-root-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "XML_LIST");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("<orders><order><id>1</id></order></orders>");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }
    }

    // -----------------------------------------------------------------------
    //  Throughput: Batch vs Single
    // -----------------------------------------------------------------------

    @Test
    public void testBatchThroughputAdvantage() throws Exception {
        int count = 200;

        // --- Single mode ---
        String singleTopic = "it-batch-perf-single-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(singleTopic, 1);

        CpiKafkaPlusProducer singleProducer = createProducer(singleTopic, new HashMap<String, String>());
        singleProducer.doStart();

        long singleStart = System.currentTimeMillis();
        try {
            for (int i = 0; i < count; i++) {
                Exchange exchange = new DefaultExchange(ctx);
                exchange.getIn().setBody("message-" + i);
                exchange.getIn().setHeader("kafka.KEY", "key-" + i);
                singleProducer.process(exchange);
            }
        } finally {
            singleProducer.doStop();
        }
        long singleMs = System.currentTimeMillis() - singleStart;

        // --- Batch mode ---
        String batchTopic = "it-batch-perf-batch-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(batchTopic, 1);

        Map<String, String> batchParams = new HashMap<>();
        batchParams.put("producerBatchMode", "JSON_ARRAY");
        CpiKafkaPlusProducer batchProducer = createProducer(batchTopic, batchParams);
        batchProducer.doStart();

        // Build JSON array
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < count; i++) {
            if (i > 0) json.append(",");
            json.append("{\"key\":\"key-").append(i)
                .append("\",\"value\":\"message-").append(i).append("\"}");
        }
        json.append("]");

        long batchStart = System.currentTimeMillis();
        try {
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody(json.toString());
            batchProducer.process(exchange);
        } finally {
            batchProducer.doStop();
        }
        long batchMs = System.currentTimeMillis() - batchStart;

        // Verify both produced correct count
        List<ConsumerRecord<String, String>> singleRecords =
                KafkaTestInfrastructure.consumeAllMessages(singleTopic, count, 15000);
        List<ConsumerRecord<String, String>> batchRecords =
                KafkaTestInfrastructure.consumeAllMessages(batchTopic, count, 15000);

        Assert.assertEquals(count, singleRecords.size());
        Assert.assertEquals(count, batchRecords.size());

        System.out.println("[PERF] " + count + " records — Single: " + singleMs
                + "ms, Batch: " + batchMs + "ms, Speedup: "
                + String.format("%.1fx", (double) singleMs / batchMs));

        // Batch should be faster (we don't assert a specific ratio,
        // just log it — CI environments vary too much)
    }

    // -----------------------------------------------------------------------
    //  NONE mode unchanged
    // -----------------------------------------------------------------------

    @Test
    public void testNoneModeUnchanged() throws Exception {
        String topic = "it-batch-none-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        // Explicitly set NONE — should behave like no batch mode
        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "NONE");
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("single-message");
            exchange.getIn().setHeader("kafka.KEY", "my-key");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("my-key", records.get(0).key());
        Assert.assertEquals("single-message", records.get(0).value());
    }

    // -----------------------------------------------------------------------
    //  Helper
    // -----------------------------------------------------------------------

    private CpiKafkaPlusProducer createProducer(String topic, Map<String, String> params)
            throws Exception {
        String uri = KafkaTestInfrastructure.buildEndpointUri(topic, "unused-group", params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
        return new CpiKafkaPlusProducer(endpoint);
    }
}
