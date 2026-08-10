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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Reproduction of a reported production incident: a transactional batch send to a topic that does
 * not exist on the broker, which blocked for the full {@code max.block.ms} and then reported
 * nothing but {@code Topic ... not present in metadata}.
 *
 * <p>Uses a dedicated broker with {@code auto.create.topics.enable=false} — that is the condition
 * on Confluent Cloud, and without it the broker silently creates the topic and the incident cannot
 * be reproduced at all.
 *
 * <p>Each test prints a {@code [REPRO]} line with the measured wall-clock duration and the actual
 * exception message, so the same suite can be run against the pre-fix and post-fix code and the
 * numbers compared directly.
 */
public class MissingTopicFailFastIT {

    /** Mirrors the failing endpoint from the incident log. */
    private static final int DELIVERY_TIMEOUT_SECONDS = 120;

    private static ConfluentKafkaContainer kafka;
    private static DefaultCamelContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new IllegalStateException("Docker is not available — this reproduction needs a broker.");
        }
        kafka = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.5"))
                .withNetwork(Network.newNetwork())
                .withNetworkAliases("kafka")
                .withListener("kafka:19092")
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName("kafka"))
                // The decisive setting: without it the broker auto-creates the topic on the first
                // metadata request and the "topic not present in metadata" error never occurs.
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        kafka.start();

        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
    }

    @AfterClass
    public static void tearDown() {
        if (ctx != null) {
            try {
                ctx.stop();
            } catch (Exception ignored) {
                // best effort
            }
        }
        if (kafka != null) {
            kafka.stop();
        }
    }

    /**
     * The incident itself: transactional batch send to a non-existent topic. Post-fix this must
     * fail within seconds and name the root cause; pre-fix it blocks for the full
     * {@code max.block.ms} and reports only a metadata timeout.
     */
    @Test
    public void testTransactionalSendToMissingTopic() throws Exception {
        // A fixed, non-random name on purpose: the incident was a topic that simply was not there,
        // and a stable name keeps the [REPRO] output comparable across runs.
        String topic = "repro-topic-never-created";
        assertTopicAbsent(topic);

        CpiKafkaPlusProducer producer = createProducer(topic, txnParams("repro-missing-topic"));
        long start = System.currentTimeMillis();
        Exception thrown = null;
        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("[{\"key\": \"k1\", \"value\": \"msg1\"}]");
            producer.process(exchange);
        } catch (Exception e) {
            thrown = e;
        } finally {
            try {
                producer.doStop();
            } catch (Exception ignored) {
                // best effort
            }
        }
        long elapsedMs = System.currentTimeMillis() - start;

        report("txn-send-to-missing-topic", elapsedMs, thrown);

        Assert.assertNotNull("Send to a non-existent topic must fail", thrown);
        Assert.assertTrue(
                "Fail-fast expected: the send must abort in well under the 60 s metadata timeout, "
                + "but took " + elapsedMs + " ms",
                elapsedMs < 20_000);
        Assert.assertTrue(
                "The error must name the root cause (missing topic), but was: " + chainToString(thrown),
                chainToString(thrown).contains("does not exist"));
    }

    /**
     * Guard against the fail-fast check breaking the happy path: an existing topic must still be
     * sent to successfully, and the AdminClient probe must not reject it.
     */
    @Test
    public void testTransactionalSendToExistingTopicStillWorks() throws Exception {
        String topic = "repro-existing-topic-" + System.nanoTime();
        createTopic(topic);

        CpiKafkaPlusProducer producer = createProducer(topic, txnParams("repro-existing-topic"));
        long start = System.currentTimeMillis();
        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("[{\"key\": \"k1\", \"value\": \"msg1\"}]");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }
        report("txn-send-to-existing-topic", System.currentTimeMillis() - start, null);
    }

    /**
     * Measures the per-send cost on the happy path across several sequential sends to the same
     * existing topic. If the topic-existence check were cached (per producer or per topic), only
     * the first send would carry its cost; without a cache every send pays for a fresh AdminClient
     * bootstrap. Prints every single duration so first-call and steady-state can be told apart.
     */
    @Test
    public void testHappyPathPerSendOverhead() throws Exception {
        String topic = "repro-overhead-" + System.nanoTime();
        createTopic(topic);

        int sends = 10;
        CpiKafkaPlusProducer producer = createProducer(topic, txnParams("repro-overhead"));
        long[] durations = new long[sends];
        try {
            producer.doStart();
            for (int i = 0; i < sends; i++) {
                Exchange exchange = new DefaultExchange(ctx);
                exchange.getIn().setBody("[{\"key\": \"k" + i + "\", \"value\": \"msg" + i + "\"}]");
                long t0 = System.nanoTime();
                producer.process(exchange);
                durations[i] = (System.nanoTime() - t0) / 1_000_000L;
            }
        } finally {
            producer.doStop();
        }

        StringBuilder sb = new StringBuilder();
        long sumTail = 0;
        for (int i = 0; i < sends; i++) {
            sb.append(i == 0 ? "" : ", ").append(durations[i]);
            if (i > 0) {
                sumTail += durations[i];
            }
        }
        System.out.println("[REPRO] happy-path-per-send: first=" + durations[0] + " ms"
                + " | avg(sends 2.." + sends + ")=" + (sumTail / (sends - 1)) + " ms"
                + " | all=[" + sb + "]");
    }

    /**
     * Same per-send measurement, but over SASL/PLAIN. On a PLAINTEXT connection an extra AdminClient
     * bootstrap costs almost nothing; with SASL every new connection additionally pays an
     * authentication handshake, which is what the production endpoint (SASL_SSL against Confluent
     * Cloud) actually does on every send.
     */
    @Test
    public void testHappyPathPerSendOverheadWithSasl() throws Exception {
        KafkaTestInfrastructure.startKafkaWithSasl();
        final CredentialHelper.UserCredentials cred = new CredentialHelper.UserCredentials(
                KafkaTestInfrastructure.SASL_USERNAME, KafkaTestInfrastructure.SASL_PASSWORD);
        CredentialHelper.setCredentialResolver(new CredentialHelper.CredentialResolver() {
            public CredentialHelper.UserCredentials resolveUserCredential(String alias) {
                return cred;
            }
        });
        try {
            String topic = "repro-overhead-sasl-" + System.nanoTime();
            KafkaTestInfrastructure.createSaslTopic(topic, 1);

            Map<String, String> params = txnParams("repro-overhead-sasl");
            String uri = KafkaTestInfrastructure.buildSaslEndpointUri(topic, "unused-group", params);
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
            CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);

            int sends = 10;
            long[] durations = new long[sends];
            try {
                producer.doStart();
                for (int i = 0; i < sends; i++) {
                    Exchange exchange = new DefaultExchange(ctx);
                    exchange.getIn().setBody("[{\"key\": \"k" + i + "\", \"value\": \"msg" + i + "\"}]");
                    long t0 = System.nanoTime();
                    producer.process(exchange);
                    durations[i] = (System.nanoTime() - t0) / 1_000_000L;
                }
            } finally {
                producer.doStop();
            }

            StringBuilder sb = new StringBuilder();
            long sumTail = 0;
            for (int i = 0; i < sends; i++) {
                sb.append(i == 0 ? "" : ", ").append(durations[i]);
                if (i > 0) {
                    sumTail += durations[i];
                }
            }
            System.out.println("[REPRO] happy-path-per-send-SASL: first=" + durations[0] + " ms"
                    + " | avg(sends 2.." + sends + ")=" + (sumTail / (sends - 1)) + " ms"
                    + " | all=[" + sb + "]");
        } finally {
            CredentialHelper.setCredentialResolver(null);
        }
    }

    /**
     * A rejected authentication is definitive: the same credentials will be rejected again, so the
     * send must fail immediately and say so. Without this, wrong credentials looked exactly like a
     * missing topic — {@code Topic ... not present in metadata} — because the producer only ever
     * reported the metadata timeout that followed.
     */
    @Test
    public void testWrongSaslCredentialsFailFastWithAnAuthenticationError() throws Exception {
        KafkaTestInfrastructure.startKafkaWithSasl();
        final CredentialHelper.UserCredentials wrong = new CredentialHelper.UserCredentials(
                KafkaTestInfrastructure.SASL_USERNAME, "definitely-not-the-password");
        CredentialHelper.setCredentialResolver(new CredentialHelper.CredentialResolver() {
            public CredentialHelper.UserCredentials resolveUserCredential(String alias) {
                return wrong;
            }
        });

        long start = System.currentTimeMillis();
        Exception thrown = null;
        try {
            String topic = "repro-bad-credentials-" + System.nanoTime();
            KafkaTestInfrastructure.createSaslTopic(topic, 1);

            String uri = KafkaTestInfrastructure.buildSaslEndpointUri(
                    topic, "unused-group", txnParams("repro-bad-credentials"));
            CpiKafkaPlusProducer producer =
                    new CpiKafkaPlusProducer((CpiKafkaPlusEndpoint) ctx.getEndpoint(uri));
            try {
                producer.doStart();
                Exchange exchange = new DefaultExchange(ctx);
                exchange.getIn().setBody("[{\"key\": \"k1\", \"value\": \"never-delivered\"}]");
                producer.process(exchange);
            } finally {
                try {
                    producer.doStop();
                } catch (Exception ignored) {
                    // best effort
                }
            }
        } catch (Exception e) {
            thrown = e;
        } finally {
            CredentialHelper.setCredentialResolver(null);
        }
        long elapsedMs = System.currentTimeMillis() - start;
        report("send-with-wrong-sasl-credentials", elapsedMs, thrown);

        Assert.assertNotNull("A send with wrong credentials must fail", thrown);
        Assert.assertTrue("Fail-fast expected on a rejected authentication, but took "
                + elapsedMs + " ms", elapsedMs < 20_000);
        String chain = chainToString(thrown);
        Assert.assertTrue("The error must name the connection problem, but was: " + chain,
                chain.contains("Cannot connect to Kafka broker"));
        Assert.assertTrue("The error must name the authentication failure, but was: " + chain,
                chain.contains("Authentication"));
    }

    /**
     * The non-transactional single-send path must fail fast on a missing topic too. Before the
     * check was applied to both paths this took the full {@code max.block.ms} and reported only a
     * metadata timeout.
     */
    @Test
    public void testNonTransactionalSendToMissingTopic() throws Exception {
        String topic = "repro-missing-nontxn-" + System.nanoTime();
        assertTopicAbsent(topic);

        Map<String, String> params = new HashMap<>();
        params.put("deliveryTimeoutSeconds", String.valueOf(DELIVERY_TIMEOUT_SECONDS));
        CpiKafkaPlusProducer producer = createProducer(topic, params);

        long start = System.currentTimeMillis();
        Exception thrown = null;
        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("plain-single-message");
            producer.process(exchange);
        } catch (Exception e) {
            thrown = e;
        } finally {
            try {
                producer.doStop();
            } catch (Exception ignored) {
                // best effort
            }
        }
        long elapsedMs = System.currentTimeMillis() - start;
        report("nontxn-send-to-missing-topic", elapsedMs, thrown);

        Assert.assertNotNull("Send to a non-existent topic must fail", thrown);
        Assert.assertTrue("Fail-fast expected on the single-send path too, but took " + elapsedMs + " ms",
                elapsedMs < 20_000);
        Assert.assertTrue("The error must name the root cause, but was: " + chainToString(thrown),
                chainToString(thrown).contains("does not exist"));
    }

    /**
     * A topic created only <em>after</em> the first failed send must be picked up without a
     * redeployment — i.e. negative results must never be cached.
     */
    @Test
    public void testMissingResultIsNotCachedSoTopicCanBeCreatedLater() throws Exception {
        String topic = "repro-created-later-" + System.nanoTime();
        assertTopicAbsent(topic);

        CpiKafkaPlusProducer producer = createProducer(topic, txnParams("repro-created-later"));
        try {
            producer.doStart();

            Exchange first = new DefaultExchange(ctx);
            first.getIn().setBody("[{\"key\": \"k1\", \"value\": \"before-topic-exists\"}]");
            try {
                producer.process(first);
                Assert.fail("Expected the send to fail while the topic is still missing");
            } catch (Exception expected) {
                Assert.assertTrue("Expected a missing-topic error, but was: " + chainToString(expected),
                        chainToString(expected).contains("does not exist"));
            }

            // Ops creates the topic — no redeployment of the IFlow.
            createTopic(topic);

            Exchange second = new DefaultExchange(ctx);
            second.getIn().setBody("[{\"key\": \"k2\", \"value\": \"after-topic-exists\"}]");
            producer.process(second);
        } finally {
            producer.doStop();
        }
        System.out.println("[REPRO] negative-result-not-cached: send succeeded after topic was created");
    }

    /**
     * A missing topic must be visible in the deployment log, but must never stop the route from
     * starting — otherwise a broker outage during a node move would leave the IFlow in an error
     * state needing manual intervention.
     */
    @Test
    public void testStartupDoesNotFailOnMissingTopic() throws Exception {
        String topic = "repro-startup-missing-" + System.nanoTime();
        assertTopicAbsent(topic);

        CpiKafkaPlusProducer producer = createProducer(topic, txnParams("repro-startup-missing"));
        producer.doStart();
        producer.doStop();
        System.out.println("[REPRO] startup-with-missing-topic: route started (warning only)");
    }

    /**
     * The same must hold when the broker cannot be reached at all at startup: an unreachable
     * bootstrap address must not block the route, and must not stall deployment for long.
     */
    @Test
    public void testStartupDoesNotFailOnUnreachableBroker() throws Exception {
        Map<String, String> params = txnParams("repro-startup-unreachable");
        // Port 1 is reserved and not listening — stands in for an unreachable broker.
        StringBuilder uri = new StringBuilder("cpi-kafka-plus:some-topic")
                .append("?bootstrapServers=127.0.0.1:1&securityProtocol=PLAINTEXT");
        for (Map.Entry<String, String> e : params.entrySet()) {
            uri.append("&").append(e.getKey()).append("=").append(e.getValue());
        }
        CpiKafkaPlusProducer producer =
                new CpiKafkaPlusProducer((CpiKafkaPlusEndpoint) ctx.getEndpoint(uri.toString()));

        long start = System.currentTimeMillis();
        producer.doStart();
        producer.doStop();
        long elapsedMs = System.currentTimeMillis() - start;
        System.out.println("[REPRO] startup-with-unreachable-broker: route started in " + elapsedMs + " ms");
        // The startup probe runs off the deployment thread, so an unreachable broker must not add
        // its timeout to route startup — otherwise every Kafka endpoint in the IFlow would pay it.
        Assert.assertTrue("Startup must not wait for the topic probe, but took " + elapsedMs + " ms",
                elapsedMs < 2_000);
    }

    /**
     * Pure configuration check for the {@code max.block.ms} half of the fix: reports the effective
     * value, which is what bounds how long {@code send()}/{@code initTransactions()} block when the
     * AdminClient probe cannot give a definitive answer.
     */
    @Test
    public void testEffectiveMaxBlockMs() {
        Map<String, String> params = new HashMap<>();
        params.put("deliveryTimeoutSeconds", String.valueOf(DELIVERY_TIMEOUT_SECONDS));
        CpiKafkaPlusEndpoint endpoint = endpoint("some-topic", params);

        Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);
        Object maxBlockMs = props.get("max.block.ms");
        System.out.println("[REPRO] max.block.ms (deliveryTimeoutSeconds=" + DELIVERY_TIMEOUT_SECONDS + ") = "
                + maxBlockMs + (maxBlockMs == null ? "  -> Kafka client default: 60000" : "")
                + " | request.timeout.ms = " + props.get("request.timeout.ms")
                + " | delivery.timeout.ms = " + props.get("delivery.timeout.ms"));

        // Must stay at or below the Kafka client default of 60 s. Tying it to delivery.timeout.ms
        // would raise it to 120 s and double the time a send to a missing topic needs to fail in
        // every case the AdminClient probe cannot decide.
        Assert.assertNotNull("max.block.ms must be set explicitly", maxBlockMs);
        Assert.assertTrue("max.block.ms must not exceed the 60 s client default, but was " + maxBlockMs,
                ((Number) maxBlockMs).longValue() <= 60_000L);
    }

    // -----------------------------------------------------------------------
    //  Helpers
    // -----------------------------------------------------------------------

    private static Map<String, String> txnParams(String txnIdPrefix) {
        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        params.put("enableTransactions", "true");
        params.put("enableIdempotence", "true");
        params.put("transactionalIdPrefix", txnIdPrefix);
        params.put("maxConcurrentTransactions", "5");
        params.put("deliveryTimeoutSeconds", String.valueOf(DELIVERY_TIMEOUT_SECONDS));
        return params;
    }

    private CpiKafkaPlusProducer createProducer(String topic, Map<String, String> params) throws Exception {
        return new CpiKafkaPlusProducer(endpoint(topic, params));
    }

    private static CpiKafkaPlusEndpoint endpoint(String topic, Map<String, String> params) {
        StringBuilder uri = new StringBuilder("cpi-kafka-plus:").append(topic)
                .append("?bootstrapServers=").append(kafka.getBootstrapServers())
                .append("&securityProtocol=PLAINTEXT");
        for (Map.Entry<String, String> e : params.entrySet()) {
            uri.append("&").append(e.getKey()).append("=").append(e.getValue());
        }
        return (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri.toString());
    }

    private static Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 20000);
        return props;
    }

    private static void createTopic(String topic) throws Exception {
        try (AdminClient admin = AdminClient.create(adminProps())) {
            admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }

    /** Precondition guard: the reproduction is meaningless if the topic happens to exist. */
    private static void assertTopicAbsent(String topic) throws Exception {
        try (AdminClient admin = AdminClient.create(adminProps())) {
            Assert.assertFalse("Precondition: topic '" + topic + "' must not exist on the broker",
                    admin.listTopics().names().get().contains(topic));
        }
    }

    private static void report(String scenario, long elapsedMs, Exception thrown) {
        System.out.println("[REPRO] " + scenario + ": elapsed=" + elapsedMs + " ms"
                + " | outcome=" + (thrown == null ? "SUCCESS" : thrown.getClass().getSimpleName())
                + (thrown == null ? "" : " | message=" + chainToString(thrown)));
    }

    private static String chainToString(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null && sb.length() < 2000; c = c.getCause()) {
            if (sb.length() > 0) {
                sb.append(" <- ");
            }
            sb.append(c.getClass().getSimpleName()).append(": ").append(c.getMessage());
            if (c.getCause() == c) {
                break;
            }
        }
        return sb.toString();
    }
}
