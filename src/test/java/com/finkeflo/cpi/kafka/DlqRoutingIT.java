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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for DLQ (Dead Letter Queue) routing with a real Kafka broker.
 */
public class DlqRoutingIT {

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
    public void testFailedRecordRoutedToDlq() throws Exception {
        String topic = "it-dlq-basic-" + System.nanoTime();
        String dlqTopic = topic + "-dlq";
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1"),
                Arrays.asList("FAIL-message"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", "0");

        Processor failProcessor = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                String body = exchange.getIn().getBody(String.class);
                if (body != null && body.contains("FAIL")) {
                    throw new RuntimeException("Simulated processing failure");
                }
            }
        };

        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, "grp-dlq-" + System.nanoTime(), params));
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, failProcessor);

        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        List<ConsumerRecord<String, String>> dlqRecords =
                KafkaTestInfrastructure.consumeAllMessages(dlqTopic, 1, 10000);
        Assert.assertEquals("Failed record should appear in DLQ", 1, dlqRecords.size());
    }

    @Test
    public void testDlqPreservesOriginalData() throws Exception {
        String topic = "it-dlq-preserve-" + System.nanoTime();
        String dlqTopic = topic + "-dlq";
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        List<List<Header>> headers = new ArrayList<>();
        headers.add(Arrays.<Header>asList(KafkaTestInfrastructure.header("X-Orig", "orig-val")));

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("original-key"),
                Arrays.asList("original-value"),
                headers);

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", "0");

        Processor alwaysFail = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                throw new RuntimeException("Always fails");
            }
        };

        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, "grp-dlq-preserve-" + System.nanoTime(), params));
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, alwaysFail);

        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        List<ConsumerRecord<String, String>> dlqRecords =
                KafkaTestInfrastructure.consumeAllMessages(dlqTopic, 1, 10000);
        Assert.assertEquals(1, dlqRecords.size());
        ConsumerRecord<String, String> dlq = dlqRecords.get(0);

        Assert.assertEquals("original-key", dlq.key());
        Assert.assertEquals("original-value", dlq.value());

        // Check original header is preserved
        Header origHeader = dlq.headers().lastHeader("X-Orig");
        Assert.assertNotNull("Original header should be preserved", origHeader);
        Assert.assertEquals("orig-val", new String(origHeader.value(), StandardCharsets.UTF_8));
    }

    @Test
    public void testDlqMetadataHeaders() throws Exception {
        String topic = "it-dlq-meta-" + System.nanoTime();
        String dlqTopic = topic + "-dlq";
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1"),
                Arrays.asList("fail-data"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", "2");
        params.put("retryOnlyTransientErrors", "false");

        Processor alwaysFail = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                throw new RuntimeException("Test error msg");
            }
        };

        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, "grp-dlq-meta-" + System.nanoTime(), params));
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, alwaysFail);

        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        List<ConsumerRecord<String, String>> dlqRecords =
                KafkaTestInfrastructure.consumeAllMessages(dlqTopic, 1, 10000);
        Assert.assertEquals(1, dlqRecords.size());
        ConsumerRecord<String, String> dlq = dlqRecords.get(0);

        assertDlqHeader(dlq, "CpiKafkaPlusDlqError", "Test error msg");
        assertDlqHeader(dlq, "CpiKafkaPlusDlqOriginalTopic", topic);
        assertDlqHeaderPresent(dlq, "CpiKafkaPlusDlqOriginalPartition");
        assertDlqHeaderPresent(dlq, "CpiKafkaPlusDlqOriginalOffset");
        assertDlqHeaderPresent(dlq, "CpiKafkaPlusDlqTimestamp");
    }

    @Test
    public void testDlqRetryCount() throws Exception {
        String topic = "it-dlq-retry-" + System.nanoTime();
        String dlqTopic = topic + "-dlq";
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1"),
                Arrays.asList("retry-data"));

        int maxRetries = 3;
        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", String.valueOf(maxRetries));
        params.put("retryOnlyTransientErrors", "false");

        Processor alwaysFail = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                throw new RuntimeException("Retry test");
            }
        };

        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, "grp-dlq-retry-" + System.nanoTime(), params));
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, alwaysFail);

        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        List<ConsumerRecord<String, String>> dlqRecords =
                KafkaTestInfrastructure.consumeAllMessages(dlqTopic, 1, 10000);
        Assert.assertEquals(1, dlqRecords.size());
        assertDlqHeader(dlqRecords.get(0), "CpiKafkaPlusDlqRetryCount", String.valueOf(maxRetries));
    }

    @Test
    public void testBatchFailureFallbackToIndividual() throws Exception {
        String topic = "it-dlq-batch-fallback-" + System.nanoTime();
        String dlqTopic = topic + "-dlq";
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("good1", "bad1", "good2"),
                Arrays.asList("good-value-1", "FAIL-value", "good-value-2"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "true");
        params.put("batchOutputFormat", "JSON_ARRAY");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", "0");

        // Batch processor always fails (triggers individual fallback)
        // Individual processor fails only for FAIL messages
        final List<Exchange> successfulExchanges = new ArrayList<>();
        Processor batchThenIndividualProcessor = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                String body = exchange.getIn().getBody(String.class);
                if (body != null && body.startsWith("[")) {
                    // Batch mode — simulate failure to trigger individual fallback
                    throw new RuntimeException("Batch processing failed");
                }
                // Individual mode — fail only for FAIL messages
                if (body != null && body.contains("FAIL")) {
                    throw new RuntimeException("Individual processing failed for FAIL message");
                }
                successfulExchanges.add(exchange);
            }
        };

        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, "grp-dlq-batch-" + System.nanoTime(), params));
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, batchThenIndividualProcessor);

        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        // Good records should be processed successfully
        Assert.assertEquals("Two good records should succeed individually", 2, successfulExchanges.size());

        // Bad record should land in DLQ
        List<ConsumerRecord<String, String>> dlqRecords =
                KafkaTestInfrastructure.consumeAllMessages(dlqTopic, 1, 10000);
        Assert.assertEquals("One bad record should be in DLQ", 1, dlqRecords.size());
        Assert.assertEquals("FAIL-value", dlqRecords.get(0).value());
    }

    @Test
    public void testSourceOffsetCommittedAfterDlq() throws Exception {
        String topic = "it-dlq-offset-" + System.nanoTime();
        String dlqTopic = topic + "-dlq";
        String group = "grp-dlq-offset-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1"),
                Arrays.asList("fail-once"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", "0");

        Processor alwaysFail = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                throw new RuntimeException("Fails for DLQ");
            }
        };

        // First consumer — DLQ the message
        CpiKafkaPlusEndpoint endpoint1 = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, group, params));
        CpiKafkaPlusConsumer consumer1 = new CpiKafkaPlusConsumer(endpoint1, alwaysFail);
        try {
            startAndPoll(consumer1);
        } finally {
            consumer1.doStop();
        }

        // Second consumer with same group — should NOT re-deliver the DLQ'd message
        List<Exchange> captured = new ArrayList<>();
        Processor capturer = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                captured.add(exchange);
            }
        };

        CpiKafkaPlusEndpoint endpoint2 = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, group, params));
        CpiKafkaPlusConsumer consumer2 = new CpiKafkaPlusConsumer(endpoint2, capturer);
        try {
            startAndPoll(consumer2);
        } finally {
            consumer2.doStop();
        }

        Assert.assertTrue("DLQ'd message offset should be committed — no re-delivery", captured.isEmpty());
    }

    @Test
    public void testPermanentErrorSkipsRetries() throws Exception {
        String topic = "it-dlq-permanent-" + System.nanoTime();
        String dlqTopic = topic + "-dlq";
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1"),
                Arrays.asList("permanent-error-data"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", "3");
        params.put("retryOnlyTransientErrors", "true");

        final java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger(0);
        Processor permanentFail = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                attempts.incrementAndGet();
                throw new NullPointerException("Simulated permanent error");
            }
        };

        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, "grp-dlq-perm-" + System.nanoTime(), params));
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, permanentFail);

        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        Assert.assertEquals("Permanent error should only be attempted once", 1, attempts.get());

        List<ConsumerRecord<String, String>> dlqRecords =
                KafkaTestInfrastructure.consumeAllMessages(dlqTopic, 1, 10000);
        Assert.assertEquals(1, dlqRecords.size());
        assertDlqHeader(dlqRecords.get(0), "CpiKafkaPlusDlqRetryCount", "0");
        assertDlqHeader(dlqRecords.get(0), "CpiKafkaPlusDlqErrorType", "PERMANENT");
    }

    @Test
    public void testTransientErrorRetriesWithBackoff() throws Exception {
        String topic = "it-dlq-transient-" + System.nanoTime();
        String dlqTopic = topic + "-dlq";
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1"),
                Arrays.asList("transient-error-data"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", "2");
        params.put("retryOnlyTransientErrors", "true");
        params.put("retryDelaySeconds", "1");

        final java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger(0);
        Processor transientFail = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                attempts.incrementAndGet();
                throw new RuntimeException("Wrapped transient",
                        new java.net.ConnectException("Connection refused"));
            }
        };

        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, "grp-dlq-trans-" + System.nanoTime(), params));
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, transientFail);

        long start = System.currentTimeMillis();
        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }
        long elapsed = System.currentTimeMillis() - start;

        Assert.assertEquals("Transient error should be attempted 3 times (1 initial + 2 retries)",
                3, attempts.get());
        Assert.assertTrue("Backoff should take at least 3s (1s + 2s), took " + elapsed + "ms",
                elapsed >= 2500);

        List<ConsumerRecord<String, String>> dlqRecords =
                KafkaTestInfrastructure.consumeAllMessages(dlqTopic, 1, 15000);
        Assert.assertEquals(1, dlqRecords.size());
        assertDlqHeader(dlqRecords.get(0), "CpiKafkaPlusDlqRetryCount", "2");
        assertDlqHeader(dlqRecords.get(0), "CpiKafkaPlusDlqErrorType", "TRANSIENT");
    }

    @Test
    public void testTransientErrorSucceedsOnRetry() throws Exception {
        String topic = "it-dlq-recover-" + System.nanoTime();
        String dlqTopic = topic + "-dlq";
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1"),
                Arrays.asList("recover-data"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", "3");
        params.put("retryOnlyTransientErrors", "true");

        final java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger(0);
        Processor failThenSucceed = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                if (attempts.incrementAndGet() <= 1) {
                    throw new RuntimeException("Transient",
                            new java.net.ConnectException("Connection refused"));
                }
            }
        };

        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, "grp-dlq-recover-" + System.nanoTime(), params));
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, failThenSucceed);

        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        Assert.assertEquals("Should succeed on second attempt", 2, attempts.get());

        List<ConsumerRecord<String, String>> dlqRecords =
                KafkaTestInfrastructure.consumeAllMessages(dlqTopic, 0, 3000);
        Assert.assertTrue("Record should NOT be in DLQ (succeeded on retry)", dlqRecords.isEmpty());
    }

    @Test
    public void testRetryClassificationDisabled() throws Exception {
        String topic = "it-dlq-noclass-" + System.nanoTime();
        String dlqTopic = topic + "-dlq";
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.createTopic(dlqTopic, 1);

        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1"),
                Arrays.asList("noclass-data"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("dlqEnabled", "true");
        params.put("dlqTopic", dlqTopic);
        params.put("dlqMaxRetries", "2");
        params.put("retryOnlyTransientErrors", "false");

        final java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger(0);
        Processor permanentFail = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                attempts.incrementAndGet();
                throw new NullPointerException("Permanent error but classification disabled");
            }
        };

        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, "grp-dlq-noclass-" + System.nanoTime(), params));
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, permanentFail);

        try {
            startAndPoll(consumer);
        } finally {
            consumer.doStop();
        }

        Assert.assertEquals("With classification disabled, all 3 attempts should run",
                3, attempts.get());

        List<ConsumerRecord<String, String>> dlqRecords =
                KafkaTestInfrastructure.consumeAllMessages(dlqTopic, 1, 10000);
        Assert.assertEquals(1, dlqRecords.size());
        Header errorTypeHeader = dlqRecords.get(0).headers().lastHeader("CpiKafkaPlusDlqErrorType");
        Assert.assertNull("No errorType header when classification disabled", errorTypeHeader);
    }

    // --- Helpers ---

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

    private static void assertDlqHeader(ConsumerRecord<String, String> record, String key, String expectedValue) {
        Header header = record.headers().lastHeader(key);
        Assert.assertNotNull("DLQ header '" + key + "' should be present", header);
        Assert.assertEquals(expectedValue, new String(header.value(), StandardCharsets.UTF_8));
    }

    private static void assertDlqHeaderPresent(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        Assert.assertNotNull("DLQ header '" + key + "' should be present", header);
    }
}
