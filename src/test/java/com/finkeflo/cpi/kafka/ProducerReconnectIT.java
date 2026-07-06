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

import java.util.Collections;
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
 * Integration tests for {@link CpiKafkaPlusProducer} lazy initialization and reconnect behaviour.
 */
public class ProducerReconnectIT {

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
    // Test 1 — Lazy init succeeds on first send
    // -----------------------------------------------------------------------

    /**
     * Verifies that the producer performs lazy init: {@code doStart()} does not connect to Kafka,
     * but the first {@code process()} call successfully initialises the client and delivers
     * the message to the broker.
     */
    @Test
    public void testLazyInitSucceedsOnFirstSend() throws Exception {
        String topic = "it-prod-lazy-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        CpiKafkaPlusProducer producer = createProducer(topic,
                Collections.<String, String>emptyMap());
        try {
            // doStart() must not throw — lazy init defers Kafka connection
            producer.doStart();

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("lazy-init-message");
            // First process() triggers ensureInitialized() and sends the record
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 1, 10000);
        Assert.assertEquals("Exactly one message should have arrived", 1, records.size());
        Assert.assertEquals("Message body must match", "lazy-init-message", records.get(0).value());
    }

    // -----------------------------------------------------------------------
    // Test 2 — doStart() with an unreachable broker does not crash
    // -----------------------------------------------------------------------

    /**
     * Verifies that {@code doStart()} is safe even when the configured broker is unreachable.
     * The producer uses lazy init, so the connection is only attempted on the first
     * {@code process()} call, which should throw either an {@link IllegalStateException}
     * (init failed internally) or a {@link RuntimeException} wrapping a send failure.
     */
    @Test
    public void testInitFailsWithBadBrokerDoesNotCrashStart() throws Exception {
        // Create endpoint directly — no CamelContext registry, unreachable broker
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setBootstrapServers("localhost:19999");
        endpoint.setSecurityProtocol("PLAINTEXT");
        endpoint.setTopic("unreachable-topic");
        // Short timeout so the test doesn't block for 2 minutes on the unreachable broker
        endpoint.setDeliveryTimeoutSeconds(5);

        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        try {
            // doStart() must not throw — lazy init means the bad broker is not contacted yet
            producer.doStart();

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("should-not-arrive");
            try {
                producer.process(exchange);
                Assert.fail("process() should have thrown because the broker is unreachable");
            } catch (IllegalStateException e) {
                // KafkaProducer creation failed — ensureInitialized() left kafkaProducer null
                Assert.assertTrue("Exception must mention 'not initialized'",
                        e.getMessage().contains("not initialized"));
            } catch (RuntimeException e) {
                // KafkaProducer was created but send timed out / broker refused
                Assert.assertTrue("Exception must mention the topic or send failure",
                        e.getMessage().contains("Failed to send")
                                || e.getMessage().contains("unreachable-topic"));
            }
        } finally {
            producer.doStop();
        }
    }

    // -----------------------------------------------------------------------
    // Test 3 — Multiple consecutive sends all succeed
    // -----------------------------------------------------------------------

    /**
     * Verifies that the producer stays initialised across multiple consecutive sends:
     * 10 messages sent in a loop all arrive on the broker and are consumed exactly once.
     */
    @Test
    public void testMultipleSendsSucceed() throws Exception {
        String topic = "it-prod-multi-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        CpiKafkaPlusProducer producer = createProducer(topic,
                Collections.<String, String>emptyMap());
        try {
            producer.doStart();

            for (int i = 0; i < 10; i++) {
                Exchange exchange = new DefaultExchange(ctx);
                exchange.getIn().setBody("message-" + i);
                producer.process(exchange);
            }
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, 10, 15000);
        Assert.assertEquals("All 10 messages must have arrived", 10, records.size());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private CpiKafkaPlusProducer createProducer(String topic, Map<String, String> params)
            throws Exception {
        String uri = KafkaTestInfrastructure.buildEndpointUri(topic, "unused-group", params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
        return new CpiKafkaPlusProducer(endpoint);
    }
}
