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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.common.header.Header;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for {@link CpiKafkaPlusConsumer} poll loop with a real Kafka broker.
 */
public class ConsumerPollIT {

    private static DefaultCamelContext ctx;
    private static CpiKafkaPlusComponent component;

    @BeforeClass
    public static void setUp() throws Exception {
        KafkaTestInfrastructure.requireDockerAvailable();
        KafkaTestInfrastructure.startKafka();

        ctx = new DefaultCamelContext();
        component = new CpiKafkaPlusComponent();
        ctx.addComponent("cpi-kafka-plus", component);
        ctx.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (ctx != null) {
            ctx.stop();
        }
    }

    @Test
    public void testSingleRecordMode() throws Exception {
        String topic = "it-single-record-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("key1"),
                Arrays.asList("{\"msg\":\"hello\"}"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-single-" + System.nanoTime(), params, captured);

        try {
            startAndPoll(consumer);

            Assert.assertFalse("Should have captured at least one exchange", captured.isEmpty());
            Exchange ex = captured.get(0);
            String body = ex.getIn().getBody(String.class);
            Assert.assertEquals("{\"msg\":\"hello\"}", body);
            Assert.assertEquals(topic, ex.getIn().getHeader("CpiKafkaPlusTopic"));
            Assert.assertNotNull(ex.getIn().getHeader("CpiKafkaPlusPartition"));
            Assert.assertNotNull(ex.getIn().getHeader("CpiKafkaPlusOffset"));
            Assert.assertEquals("key1", ex.getIn().getHeader("CpiKafkaPlusKey"));
            Assert.assertNotNull(ex.getIn().getHeader("CpiKafkaPlusTimestamp"));
        } finally {
            consumer.doStop();
        }
    }

    @Test
    public void testBatchModeJsonArray() throws Exception {
        String topic = "it-batch-json-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1", "k2", "k3"),
                Arrays.asList("{\"n\":1}", "{\"n\":2}", "{\"n\":3}"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "true");
        params.put("batchOutputFormat", "JSON_ARRAY");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("batchSize", "100");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-batch-json-" + System.nanoTime(), params, captured);

        try {
            startAndPoll(consumer);

            Assert.assertFalse("Should have captured at least one exchange", captured.isEmpty());
            Exchange ex = captured.get(0);
            String body = ex.getIn().getBody(String.class);
            Assert.assertTrue("Body should be a kafkaRecords JSON object", body.startsWith("{\"kafkaRecords\""));
            Assert.assertEquals(3, (int) ex.getIn().getHeader("CpiKafkaPlusRecordCount", Integer.class));
        } finally {
            consumer.doStop();
        }
    }

    @Test
    public void testBatchModeXmlList() throws Exception {
        String topic = "it-batch-xml-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1", "k2"),
                Arrays.asList("val1", "val2"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "true");
        params.put("batchOutputFormat", "XML_LIST");
        params.put("commitStrategy", "BATCH_COMPLETE");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-batch-xml-" + System.nanoTime(), params, captured);

        try {
            startAndPoll(consumer);

            Assert.assertFalse("Should have captured at least one exchange", captured.isEmpty());
            String body = captured.get(0).getIn().getBody(String.class);
            Assert.assertTrue("Body should have kafkaRecords XML", body.contains("<kafkaRecords"));
            Assert.assertTrue("Body should have record elements", body.contains("<record>"));
        } finally {
            consumer.doStop();
        }
    }

    @Test
    public void testSplitExchangesMode() throws Exception {
        String topic = "it-split-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1", "k2", "k3"),
                Arrays.asList("v1", "v2", "v3"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "true");
        params.put("batchOutputFormat", "SPLIT_EXCHANGES");
        params.put("commitStrategy", "BATCH_COMPLETE");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-split-" + System.nanoTime(), params, captured);

        try {
            startAndPoll(consumer);

            Assert.assertEquals("Each record should be a separate exchange", 3, captured.size());
            Assert.assertEquals("v1", captured.get(0).getIn().getBody(String.class));
            Assert.assertEquals("v2", captured.get(1).getIn().getBody(String.class));
            Assert.assertEquals("v3", captured.get(2).getIn().getBody(String.class));
        } finally {
            consumer.doStop();
        }
    }

    @Test
    public void testKafkaRecordHeaders() throws Exception {
        String topic = "it-headers-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        List<List<Header>> headers = new ArrayList<>();
        headers.add(Arrays.<Header>asList(
                KafkaTestInfrastructure.header("X-Custom", "custom-value"),
                KafkaTestInfrastructure.header("X-Trace-Id", "trace-123")
        ));

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1"), Arrays.asList("body1"), headers);

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-headers-" + System.nanoTime(), params, captured);

        try {
            startAndPoll(consumer);

            Assert.assertFalse(captured.isEmpty());
            Exchange ex = captured.get(0);
            Assert.assertEquals("custom-value", ex.getIn().getHeader("kafka.header.X-Custom"));
            Assert.assertEquals("trace-123", ex.getIn().getHeader("kafka.header.X-Trace-Id"));
        } finally {
            consumer.doStop();
        }
    }

    @Test
    public void testOffsetCommitBatchComplete() throws Exception {
        String topic = "it-offset-commit-" + System.nanoTime();
        String group = "grp-offset-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1", "k2"),
                Arrays.asList("v1", "v2"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");

        // First poll — consume all messages
        List<Exchange> captured1 = new ArrayList<>();
        CpiKafkaPlusConsumer consumer1 = createConsumer(topic, group, params, captured1);
        try {
            startAndPoll(consumer1);
            Assert.assertEquals("First poll should consume 2 records", 2, captured1.size());
        } finally {
            consumer1.doStop();
        }

        // Second poll with same group — should get no duplicates
        List<Exchange> captured2 = new ArrayList<>();
        CpiKafkaPlusConsumer consumer2 = createConsumer(topic, group, params, captured2);
        try {
            startAndPoll(consumer2);
            Assert.assertTrue("Second poll should have no records (offsets committed)", captured2.isEmpty());
        } finally {
            consumer2.doStop();
        }
    }

    @Test
    public void testAutoOffsetResetEarliest() throws Exception {
        String topic = "it-earliest-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        // Produce before consumer starts
        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("pre-key"),
                Arrays.asList("pre-value"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");

        List<Exchange> captured = new ArrayList<>();
        CpiKafkaPlusConsumer consumer = createConsumer(topic, "grp-earliest-" + System.nanoTime(), params, captured);

        try {
            startAndPoll(consumer);
            Assert.assertFalse("Should consume pre-produced message with earliest offset reset",
                    captured.isEmpty());
            Assert.assertEquals("pre-value", captured.get(0).getIn().getBody(String.class));
        } finally {
            consumer.doStop();
        }
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

        // Poll multiple times to allow partition assignment
        int totalPolled = 0;
        for (int i = 0; i < 10; i++) {
            int polled = (Integer) pollMethod.invoke(consumer);
            totalPolled += polled;
            if (totalPolled > 0) {
                break;
            }
            Thread.sleep(500);
        }
    }
}
