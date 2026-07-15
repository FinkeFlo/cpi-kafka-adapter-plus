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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * End-to-end regression test for the consumer auto-pause lifecycle with a real Kafka broker.
 */
public class AutoPauseIT {

    private static final int ERROR_THRESHOLD = 2;
    private static final int COOLDOWN_SECONDS = 2;
    private static final int MESSAGE_COUNT = 6;

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
    public void testAutoPausePausesAfterProcessingErrorsThenResumes() throws Exception {
        String topic = "it-auto-pause-" + System.nanoTime();
        String group = "grp-auto-pause-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k0", "k1", "k2", "k3", "k4", "k5"),
                Arrays.asList("v0", "v1", "v2", "v3", "v4", "v5"));

        Map<String, String> params = new HashMap<String, String>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        params.put("maxPollRecords", "1");
        params.put("batchTimeout", "250");
        params.put("pollingIntervalSeconds", "1");
        params.put("autoPauseEnabled", "true");
        params.put("autoPauseErrorThreshold", String.valueOf(ERROR_THRESHOLD));
        params.put("autoPauseCooldownSeconds", String.valueOf(COOLDOWN_SECONDS));

        final AtomicInteger attempts = new AtomicInteger();
        final List<String> successfulBodies = new ArrayList<String>();
        final List<Long> successfulOffsets = new ArrayList<Long>();
        TrackingProcessor processor = new TrackingProcessor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                int attempt = attempts.incrementAndGet();
                if (attempt <= ERROR_THRESHOLD) {
                    throw new DownstreamProcessingError("intentional auto-pause failure " + attempt);
                }
                successfulBodies.add(exchange.getIn().getBody(String.class));
                successfulOffsets.add(exchange.getIn().getHeader("CpiKafkaPlusOffset", Long.class));
            }

            @Override
            public int getAttempts() {
                return attempts.get();
            }

            @Override
            public List<String> getSuccessfulBodies() {
                return successfulBodies;
            }
        };

        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                KafkaTestInfrastructure.buildEndpointUri(topic, group, params));
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, processor);

        try {
            Method pollMethod = startConsumerAndGetPollMethod(consumer);

            waitUntilAttemptsAtLeast(pollMethod, consumer, ERROR_THRESHOLD, 15000L);
            Assert.assertEquals("The threshold failures should not have been committed as successes",
                    0, successfulBodies.size());

            assertNoProcessingDuringCooldown(pollMethod, consumer, attempts.get());

            waitUntilSuccessCount(pollMethod, consumer, MESSAGE_COUNT - ERROR_THRESHOLD, 20000L);
        } finally {
            consumer.doStop();
        }

        Assert.assertEquals(Arrays.asList("v2", "v3", "v4", "v5"), successfulBodies);
        Assert.assertEquals(Arrays.asList(Long.valueOf(2L), Long.valueOf(3L),
                Long.valueOf(4L), Long.valueOf(5L)), successfulOffsets);

        List<ConsumerRecord<String, String>> duplicates =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 3000);
        Assert.assertTrue("A fresh group helper reads all records, sanity check topic still exists",
                duplicates.size() > 0);
        Assert.assertTrue("Same consumer group should not re-read records after successful commits",
                consumeWithSameGroup(topic, group).isEmpty());
    }

    private static Method startConsumerAndGetPollMethod(CpiKafkaPlusConsumer consumer) throws Exception {
        consumer.doStart();
        Method pollMethod = CpiKafkaPlusConsumer.class.getDeclaredMethod("poll");
        pollMethod.setAccessible(true);
        return pollMethod;
    }

    private static void waitUntilAttemptsAtLeast(Method pollMethod, CpiKafkaPlusConsumer consumer,
                                                 int expectedAttempts, long timeoutMs) throws Exception {
        await().atMost(Duration.ofMillis(timeoutMs))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    invokePollAllowingIntentionalFailures(pollMethod, consumer);
                    return getAttempts(consumer) >= expectedAttempts;
                });
    }

    private static void waitUntilSuccessCount(Method pollMethod, CpiKafkaPlusConsumer consumer,
                                             int expectedSuccesses, long timeoutMs) throws Exception {
        await().atMost(Duration.ofMillis(timeoutMs))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    invokePollAllowingIntentionalFailures(pollMethod, consumer);
                    return getSuccessfulBodies(consumer).size() >= expectedSuccesses;
                });
    }

    private static void assertNoProcessingDuringCooldown(Method pollMethod, CpiKafkaPlusConsumer consumer,
                                                         int attemptsAfterPause) throws Exception {
        AtomicInteger pausedPolls = new AtomicInteger();
        await().during(Duration.ofMillis(800))
                .atMost(Duration.ofMillis(1200))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    int processed = invokePollAllowingIntentionalFailures(pollMethod, consumer);
                    pausedPolls.incrementAndGet();
                    Assert.assertEquals("Auto-paused poll must not process records", 0, processed);
                    Assert.assertEquals("No downstream processor calls expected during cooldown",
                            attemptsAfterPause, getAttempts(consumer));
                });
        Assert.assertTrue("Cooldown observation should include at least one paused poll",
                pausedPolls.get() > 0);
    }

    private static int invokePollAllowingIntentionalFailures(Method pollMethod,
                                                            CpiKafkaPlusConsumer consumer) throws Exception {
        try {
            return (Integer) pollMethod.invoke(consumer);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (containsDownstreamProcessingError(cause)) {
                return 0;
            }
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private static boolean containsDownstreamProcessingError(Throwable t) {
        Throwable current = t;
        while (current != null) {
            if (current instanceof DownstreamProcessingError) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static int getAttempts(CpiKafkaPlusConsumer consumer) {
        Processor processor = consumer.getProcessor();
        return ((TrackingProcessor) processor).getAttempts();
    }

    private static List<String> getSuccessfulBodies(CpiKafkaPlusConsumer consumer) {
        Processor processor = consumer.getProcessor();
        return ((TrackingProcessor) processor).getSuccessfulBodies();
    }

    private static List<ConsumerRecord<String, String>> consumeWithSameGroup(
            String topic, String group) {
        java.util.Properties props = new java.util.Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaTestInfrastructure.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> result = new ArrayList<ConsumerRecord<String, String>>();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        try {
            kafkaConsumer.subscribe(java.util.Collections.singletonList(topic));
            try {
                await().atMost(Duration.ofSeconds(5))
                        .pollInterval(Duration.ofMillis(250))
                        .until(() -> {
                            ConsumerRecords<String, String> records =
                                    kafkaConsumer.poll(java.time.Duration.ofMillis(250L));
                            for (ConsumerRecord<String, String> record : records) {
                                result.add(record);
                            }
                            return !result.isEmpty();
                        });
            } catch (ConditionTimeoutException ignored) {
                // Expected when the group has already committed past the processed records.
            }
        } finally {
            kafkaConsumer.close();
        }
        return result;
    }

    private interface TrackingProcessor extends Processor {
        int getAttempts();
        List<String> getSuccessfulBodies();
    }

    private static final class DownstreamProcessingError extends Error {
        private static final long serialVersionUID = 1L;

        DownstreamProcessingError(String message) {
            super(message);
        }
    }
}
