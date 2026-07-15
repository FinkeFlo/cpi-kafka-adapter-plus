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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration test: verifies that keep-alive polls drive the group protocol
 * without consuming records or committing offsets.
 */
public class KeepAlivePollIT {

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
    public void keepAlivePollDoesNotConsumeOrCommit() throws Exception {
        String topic = "it-keepalive-" + System.nanoTime();
        String group = "grp-keepalive-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);
        KafkaTestInfrastructure.produceStringMessages(topic,
                Arrays.asList("k1", "k2"),
                Arrays.asList("v1", "v2"));

        Map<String, String> params = new HashMap<>();
        params.put("batchMode", "false");
        params.put("commitStrategy", "BATCH_COMPLETE");
        // Langes Emit-Intervall: nach dem ersten Emit sind weitere poll()s Keep-Alive.
        params.put("pollingIntervalSeconds", "3600");

        Method pollMethod = CpiKafkaPlusConsumer.class.getDeclaredMethod("poll");
        pollMethod.setAccessible(true);

        List<Exchange> captured1 = new ArrayList<>();
        CpiKafkaPlusConsumer consumer1 = createConsumer(topic, group, params, captured1);
        try {
            consumer1.doStart();

            await().atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> {
                        pollMethod.invoke(consumer1);
                        return captured1.size() >= 2;
                    });
            Assert.assertEquals("erster Emit muss beide Records konsumieren", 2, captured1.size());

            // Zwei weitere Records NACH dem Emit; die naechsten poll()s sind
            // Keep-Alive (Intervall nicht abgelaufen) und duerfen sie NICHT konsumieren.
            KafkaTestInfrastructure.produceStringMessages(topic,
                    Arrays.asList("k3", "k4"),
                    Arrays.asList("v3", "v4"));
            AtomicInteger keepAlivePolls = new AtomicInteger();
            await().pollDelay(Duration.ZERO)
                    .atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(200))
                    .until(() -> {
                        int polled = ((Integer) pollMethod.invoke(consumer1)).intValue();
                        return polled == 0 && keepAlivePolls.incrementAndGet() >= 6;
                    });
            Assert.assertEquals("Keep-Alive-Poll darf keine neuen Records konsumieren",
                    2, captured1.size());
        } finally {
            consumer1.doStop();
        }

        // Frischer Consumer in derselben Group muss k3/k4 noch sehen — Beweis,
        // dass die Keep-Alive-Polls weder committed noch die Position bewegt haben.
        List<Exchange> captured2 = new ArrayList<>();
        CpiKafkaPlusConsumer consumer2 = createConsumer(topic, group, params, captured2);
        try {
            consumer2.doStart();
            await().atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> {
                        pollMethod.invoke(consumer2);
                        return captured2.size() >= 2;
                    });
            Assert.assertEquals("neuer Consumer muss genau die 2 un-konsumierten Records sehen",
                    2, captured2.size());
        } finally {
            consumer2.doStop();
        }
    }

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
}
