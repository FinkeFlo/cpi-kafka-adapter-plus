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

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for the STREAMING (greedy) consumption mode against a real Kafka broker.
 *
 * <p>Unlike {@link ConsumerPollIT}, these tests do NOT drive {@code poll()} manually — they start
 * the consumer through the real Camel {@code ScheduledPollConsumer} scheduler (with greedy
 * scheduling enabled) so the end-to-end streaming behaviour is exercised exactly as it runs in
 * CPI: records produced over time are consumed continuously with sub-second latency, independent
 * of the (deliberately long) {@code pollingIntervalSeconds}.</p>
 */
public class StreamingConsumptionIT {

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

    /**
     * Records produced AFTER the consumer has joined the group are picked up within a couple of
     * seconds even though {@code pollingIntervalSeconds=30}. Greedy scheduling + the fixed 1s idle
     * heartbeat decouple consumption latency from the polling interval.
     */
    @Test
    public void testStreamingConsumesContinuouslyWithLowLatency() throws Exception {
        String topic = "it-streaming-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("consumptionMode", "STREAMING");
        params.put("pollingIntervalSeconds", "30"); // long on purpose — must be ignored in STREAMING
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("batchTimeout", "1000"); // snappy polls

        List<Exchange> captured = new CopyOnWriteArrayList<>();
        String uri = KafkaTestInfrastructure.buildEndpointUri(topic, "grp-streaming-" + System.nanoTime(), params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);

        Assert.assertTrue("Endpoint must report STREAMING mode", endpoint.isStreamingMode());

        Processor capturing = exchange -> captured.add(exchange);
        CpiKafkaPlusConsumer consumer = (CpiKafkaPlusConsumer) endpoint.createConsumer(capturing);
        Assert.assertTrue("STREAMING must configure greedy scheduling", consumer.isGreedy());

        try {
            // Start the real scheduler (initialDelay=0). Give it a moment to create the Kafka
            // consumer and join the group so 'latest'/'earliest' race conditions cannot swallow
            // the probe records below.
            consumer.start();
            Thread.sleep(3000);

            long produceStart = System.currentTimeMillis();
            for (int i = 0; i < 5; i++) {
                KafkaTestInfrastructure.produceStringMessages(topic,
                        Arrays.asList("k" + i),
                        Arrays.asList("{\"n\":" + i + "}"));
                Thread.sleep(700);
            }

            // All 5 records must be consumed well within the 30s polling interval — proving that
            // greedy streaming, not the scheduled interval, drives consumption.
            await().atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .until(() -> captured.size() >= 5);

            long elapsed = System.currentTimeMillis() - produceStart;
            Assert.assertTrue(
                    "All 5 records must be consumed far faster than the 30s poll interval "
                            + "(greedy streaming); took " + elapsed + "ms",
                    elapsed < 20000);
            Assert.assertEquals("Should consume exactly the 5 streamed records", 5, captured.size());
        } finally {
            consumer.stop();
        }
    }

    /**
     * Sanity check that STREAMING also works together with batch mode (they are orthogonal:
     * greedy controls poll cadence, batch mode controls how records are grouped per exchange).
     */
    @Test
    public void testStreamingWithBatchMode() throws Exception {
        String topic = "it-streaming-batch-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        Map<String, String> params = new HashMap<>();
        params.put("consumptionMode", "STREAMING");
        params.put("batchMode", "true");
        params.put("batchOutputFormat", "JSON_ARRAY");
        params.put("batchSize", "100");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("batchTimeout", "1000");

        List<Exchange> captured = new CopyOnWriteArrayList<>();
        String uri = KafkaTestInfrastructure.buildEndpointUri(topic, "grp-streaming-batch-" + System.nanoTime(), params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);

        Processor capturing = exchange -> captured.add(exchange);
        CpiKafkaPlusConsumer consumer = (CpiKafkaPlusConsumer) endpoint.createConsumer(capturing);
        Assert.assertTrue("STREAMING must configure greedy scheduling", consumer.isGreedy());

        try {
            consumer.start();
            Thread.sleep(3000);

            KafkaTestInfrastructure.produceStringMessages(topic,
                    Arrays.asList("k1", "k2", "k3"),
                    Arrays.asList("{\"n\":1}", "{\"n\":2}", "{\"n\":3}"));

            await().atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .until(() -> !captured.isEmpty());

            int totalRecords = 0;
            for (Exchange ex : captured) {
                Integer count = ex.getIn().getHeader("CpiKafkaPlusRecordCount", Integer.class);
                totalRecords += (count != null ? count : 0);
            }
            Assert.assertEquals("Streaming+batch must deliver all 3 records", 3, totalRecords);
        } finally {
            consumer.stop();
        }
    }
}
