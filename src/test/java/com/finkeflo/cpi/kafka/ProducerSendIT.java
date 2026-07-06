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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for {@link CpiKafkaPlusProducer} send behaviour with a real Kafka broker.
 */
public class ProducerSendIT {

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

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    public void testSimpleStringSend() throws Exception {
        String topic = "it-prod-simple-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        CpiKafkaPlusProducer producer = createProducer(topic, new HashMap<String, String>());
        try {
            producer.doStart();

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("hello-kafka");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals("Should have received exactly 1 record", 1, records.size());
        Assert.assertEquals("hello-kafka", records.get(0).value());
    }

    @Test
    public void testKeyRouting() throws Exception {
        String topic = "it-prod-key-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        CpiKafkaPlusProducer producer = createProducer(topic, new HashMap<String, String>());
        try {
            producer.doStart();

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("keyed-message");
            exchange.getIn().setHeader("kafka.KEY", "my-key");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("my-key", records.get(0).key());
        Assert.assertEquals("keyed-message", records.get(0).value());
    }

    @Test
    public void testPartitionPinning() throws Exception {
        String topic = "it-prod-partition-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 3);

        CpiKafkaPlusProducer producer = createProducer(topic, new HashMap<String, String>());
        try {
            producer.doStart();

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("partition-pinned");
            exchange.getIn().setHeader("kafka.PARTITION_KEY", "2");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("partition-pinned", records.get(0).value());
        Assert.assertEquals("Message should land on partition 2", 2, records.get(0).partition());
    }

    @Test
    public void testTimestampOverride() throws Exception {
        String topic = "it-prod-timestamp-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        long fixedTimestamp = 1700000000000L;

        CpiKafkaPlusProducer producer = createProducer(topic, new HashMap<String, String>());
        try {
            producer.doStart();

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("timestamped-message");
            exchange.getIn().setHeader("kafka.OVERRIDE_TIMESTAMP", fixedTimestamp);
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("Record timestamp should match the override value",
                fixedTimestamp, records.get(0).timestamp());
    }

    @Test
    public void testTopicOverrideViaHeader() throws Exception {
        String defaultTopic = "it-prod-default-" + System.nanoTime();
        String overrideTopic = "it-prod-override-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(defaultTopic, 1);
        KafkaTestInfrastructure.createTopic(overrideTopic, 1);

        CpiKafkaPlusProducer producer = createProducer(defaultTopic, new HashMap<String, String>());
        try {
            producer.doStart();

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("override-topic-message");
            exchange.getIn().setHeader("CamelKafkaTopic", overrideTopic);
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        // Message must appear in the override topic
        List<ConsumerRecord<String, String>> overrideRecords =
                KafkaTestInfrastructure.consumeAllMessages(overrideTopic, 1, 10000);
        Assert.assertEquals("Message should arrive in override topic", 1, overrideRecords.size());
        Assert.assertEquals("override-topic-message", overrideRecords.get(0).value());

        // Default topic must remain empty
        List<ConsumerRecord<String, String>> defaultRecords =
                KafkaTestInfrastructure.consumeAllMessages(defaultTopic, 1, 3000);
        Assert.assertEquals("Default topic should remain empty", 0, defaultRecords.size());
    }

    @Test
    public void testResponseHeaders() throws Exception {
        String topic = "it-prod-response-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        CpiKafkaPlusProducer producer = createProducer(topic, new HashMap<String, String>());
        Exchange exchange = new DefaultExchange(ctx);
        try {
            producer.doStart();

            exchange.getIn().setBody("response-header-check");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        Assert.assertNotNull("CamelKafkaTopic must be set",
                exchange.getIn().getHeader("CamelKafkaTopic"));
        Assert.assertEquals("CamelKafkaTopic must match sent topic",
                topic, exchange.getIn().getHeader("CamelKafkaTopic"));
        Assert.assertNotNull("SAP_Receiver must be set",
                exchange.getIn().getHeader("SAP_Receiver"));
        Assert.assertNotNull("CamelKafkaPartition must be set",
                exchange.getIn().getHeader("CamelKafkaPartition"));
        Assert.assertNotNull("CamelKafkaOffset must be set",
                exchange.getIn().getHeader("CamelKafkaOffset"));
        Assert.assertNotNull("CamelKafkaTimestamp must be set",
                exchange.getIn().getHeader("CamelKafkaTimestamp"));
    }

    @Test
    public void testHeaderForwarding() throws Exception {
        String topic = "it-prod-header-fwd-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        CpiKafkaPlusProducer producer = createProducer(topic, new HashMap<String, String>());
        try {
            producer.doStart();

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("header-forward-body");
            exchange.getIn().setHeader("X-Custom-Header", "custom-value");
            exchange.getIn().setHeader("X-Trace-Id", "trace-abc-123");
            // These should NOT be forwarded as Kafka record headers
            exchange.getIn().setHeader("CamelBreadcrumbId", "breadcrumb-should-not-appear");
            exchange.getIn().setHeader("kafka.KEY", "key-should-not-appear");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals(1, records.size());
        ConsumerRecord<String, String> record = records.get(0);

        // Custom headers must be forwarded
        Header customHeader = record.headers().lastHeader("X-Custom-Header");
        Assert.assertNotNull("X-Custom-Header should be forwarded as Kafka record header", customHeader);
        Assert.assertEquals("custom-value",
                new String(customHeader.value(), StandardCharsets.UTF_8));

        Header traceHeader = record.headers().lastHeader("X-Trace-Id");
        Assert.assertNotNull("X-Trace-Id should be forwarded as Kafka record header", traceHeader);
        Assert.assertEquals("trace-abc-123",
                new String(traceHeader.value(), StandardCharsets.UTF_8));

        // Camel internal and kafka.* control headers must NOT be forwarded
        Assert.assertNull("CamelBreadcrumbId must not appear as Kafka record header",
                record.headers().lastHeader("CamelBreadcrumbId"));
        Assert.assertNull("kafka.KEY must not appear as Kafka record header",
                record.headers().lastHeader("kafka.KEY"));
    }

    @Test
    public void testNullBodyTombstone() throws Exception {
        String topic = "it-prod-tombstone-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        CpiKafkaPlusProducer producer = createProducer(topic, new HashMap<String, String>());
        try {
            producer.doStart();

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody(null);
            exchange.getIn().setHeader("kafka.KEY", "tombstone-key");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals("Tombstone record should arrive", 1, records.size());
        Assert.assertEquals("tombstone-key", records.get(0).key());
        Assert.assertNull("Tombstone record value must be null", records.get(0).value());
    }

    @Test
    public void testIdempotenceWithAcksOneFallback() throws Exception {
        String topic = "it-prod-idempotence-fallback-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<String, String>();
        params.put("enableIdempotence", "true");
        params.put("acks", "1");

        CpiKafkaPlusProducer producer = createProducer(topic, params);
        try {
            producer.doStart();

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("idempotence-fallback-test");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals("Message should arrive despite acks=1 + idempotence=true", 1, records.size());
        Assert.assertEquals("idempotence-fallback-test", records.get(0).value());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private CpiKafkaPlusProducer createProducer(String topic, Map<String, String> params) throws Exception {
        String uri = KafkaTestInfrastructure.buildEndpointUri(topic, "unused-group", params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
        return new CpiKafkaPlusProducer(endpoint);
    }
}
