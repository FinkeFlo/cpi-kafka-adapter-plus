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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
import org.testcontainers.utility.MountableFile;

/**
 * Reproduces a reported connection failure whose cause was a security-protocol mismatch: a producer
 * configured with {@code SASL_PLAINTEXT} against a broker that only accepts TLS on that listener,
 * as Confluent Cloud does.
 *
 * <p>Kafka's own symptom is indistinguishable from a missing topic — {@code Topic ... not present
 * in metadata after N ms} — because a client without TLS never gets far enough for the broker to
 * answer at all, so no handshake or authentication error is ever raised. The topic itself exists,
 * which this test asserts up front over a correctly configured client.
 *
 * <p>This test guards the diagnosis: the adapter's topic probe hits the same wall, and its cause
 * plus the security-protocol hint must appear in the exception the IFlow sees. Without that, the
 * operator has nothing to go on.
 *
 * <p>Only the broker needs certificate material here: a {@code SASL_PLAINTEXT} client performs no
 * TLS at all, so no client-side truststore is involved. That is what makes this configuration
 * reproducible even though the adapter's SSL path is bound to the CPI keystore.
 */
public class ProtocolMismatchIT {

    private static final String SASL_USER = "testuser";
    private static final String SASL_PASSWORD = "test-secret";
    private static final String STORE_PASSWORD = "changeit";
    /** Keeps the reproduction quick: max.block.ms is derived from this. */
    private static final int DELIVERY_TIMEOUT_SECONDS = 10;

    private static File secretsDir;
    private static ConfluentKafkaContainer kafka;
    private static DefaultCamelContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new IllegalStateException("Docker is not available — this reproduction needs a broker.");
        }
        secretsDir = generateBrokerCertificates();

        kafka = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.5"))
                .withNetwork(Network.newNetwork())
                .withNetworkAliases("kafka")
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName("kafka"))
                .withCopyFileToContainer(
                        MountableFile.forHostPath(secretsDir.getAbsolutePath()), "/etc/kafka/secrets")
                // The external listener speaks SASL_SSL only — exactly like Confluent Cloud on 9092.
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                        "BROKER:PLAINTEXT,PLAINTEXT:SASL_SSL,CONTROLLER:PLAINTEXT")
                .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"admin\" password=\"admin-secret\" "
                        + "user_admin=\"admin-secret\" "
                        + "user_" + SASL_USER + "=\"" + SASL_PASSWORD + "\";")
                // Note: the KAFKA_SSL_KEYSTORE_FILENAME / _CREDENTIALS convention of the Confluent
                // image is not translated by cp-kafka 7.9.5 under KRaft — it ends up verbatim in
                // kafka.properties, the broker starts without a certificate and every handshake
                // fails with a bare handshake_failure. Set the real Kafka properties instead.
                .withEnv("KAFKA_SSL_KEYSTORE_LOCATION", "/etc/kafka/secrets/server.jks")
                .withEnv("KAFKA_SSL_KEYSTORE_PASSWORD", STORE_PASSWORD)
                .withEnv("KAFKA_SSL_KEY_PASSWORD", STORE_PASSWORD)
                .withEnv("KAFKA_SSL_KEYSTORE_TYPE", "JKS")
                .withEnv("KAFKA_SSL_CLIENT_AUTH", "none")
                .withEnv("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "")
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
     * The reproduction: same broker, same topic, same credentials — only the security protocol is
     * wrong. A correctly configured client (SASL_SSL, test-owned truststore) creates the topic and
     * proves the broker is healthy first, so the failure below cannot be blamed on the cluster.
     *
     * <p>The send must still fail — a client without TLS cannot talk to this listener — but the
     * exception must name the protocol mismatch instead of only reporting a metadata timeout.
     */
    @Test
    public void testSaslPlaintextAgainstTlsOnlyBrokerReportsTheProtocolMismatch() throws Exception {
        String topic = "topic_0";
        createTopicOverTls(topic);
        Assert.assertTrue("Precondition: the topic must exist on the broker",
                listTopicsOverTls().contains(topic));

        Map<String, String> params = new HashMap<>();
        params.put("deliveryTimeoutSeconds", String.valueOf(DELIVERY_TIMEOUT_SECONDS));
        params.put("credentialAlias", "mismatch-test-credential");
        // The misconfiguration under test: SASL_PLAINTEXT against a TLS-only listener.
        String uri = "cpi-kafka-plus:" + topic
                + "?bootstrapServers=" + plainBootstrapServers()
                + "&securityProtocol=SASL_PLAINTEXT"
                + "&saslMechanism=PLAIN"
                + "&deliveryTimeoutSeconds=" + DELIVERY_TIMEOUT_SECONDS
                + "&credentialAlias=mismatch-test-credential";

        final CredentialHelper.UserCredentials cred =
                new CredentialHelper.UserCredentials(SASL_USER, SASL_PASSWORD);
        CredentialHelper.setCredentialResolver(new CredentialHelper.CredentialResolver() {
            public CredentialHelper.UserCredentials resolveUserCredential(String alias) {
                return cred;
            }
        });

        long start = System.currentTimeMillis();
        Exception thrown = null;
        try {
            CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(
                    (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri));
            try {
                producer.doStart();
                Exchange exchange = new DefaultExchange(ctx);
                exchange.getIn().setBody("{\"testId\":\"KAFKA-PLUS-001\"}");
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

        String chain = chainToString(thrown);
        System.out.println("[REPRO] sasl-plaintext-against-tls-broker: elapsed=" + elapsedMs + " ms"
                + " | outcome=" + (thrown == null ? "SUCCESS" : thrown.getClass().getSimpleName())
                + " | message=" + chain);

        Assert.assertNotNull("A SASL_PLAINTEXT client must not succeed against a TLS-only listener",
                thrown);
        Assert.assertFalse("The reproduction is only meaningful while the topic really exists",
                chain.contains("does not exist"));

        // The whole point of the fix: the message must say why, not just that it timed out.
        // The listener probe now recognises this broker's TLS alert, so the mismatch is caught
        // before any Kafka client is built - earlier, and with a more specific message, than the
        // pre-send reachability check that used to report it.
        String message = thrown.getMessage();
        Assert.assertTrue("The failure must name the configured protocol, but was: " + message,
                message.contains("SASL_PLAINTEXT"));
        Assert.assertTrue("The failure must state that the broker requires TLS, but was: "
                + message, message.contains("requires TLS"));
        Assert.assertTrue("The hint must point at the protocol that would work, but was: " + message,
                message.contains("SASL_SSL"));
    }

    // -----------------------------------------------------------------------
    //  Helpers
    // -----------------------------------------------------------------------

    /** Bootstrap address of the container's external listener. */
    private static String plainBootstrapServers() {
        return kafka.getHost() + ":" + kafka.getMappedPort(9092);
    }

    /** Client config that is configured correctly — used only to prove the broker works. */
    private static Properties tlsClientProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, plainBootstrapServers());
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"" + SASL_USER + "\" password=\"" + SASL_PASSWORD + "\";");
        props.put("ssl.truststore.location", new File(secretsDir, "truststore.jks").getAbsolutePath());
        props.put("ssl.truststore.password", STORE_PASSWORD);
        // The certificate is self-signed for a container host name, so skip hostname verification.
        props.put("ssl.endpoint.identification.algorithm", "");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 40000);
        return props;
    }

    private static void createTopicOverTls(String topic) throws Exception {
        try (AdminClient admin = AdminClient.create(tlsClientProps())) {
            if (!admin.listTopics().names().get().contains(topic)) {
                admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                        .all().get();
            }
        }
    }

    private static java.util.Set<String> listTopicsOverTls() throws Exception {
        try (AdminClient admin = AdminClient.create(tlsClientProps())) {
            return admin.listTopics().names().get();
        }
    }

    /**
     * Creates a self-signed broker keystore plus a matching truststore for the control client, in
     * the layout the Confluent image expects under {@code /etc/kafka/secrets}.
     */
    private static File generateBrokerCertificates() throws Exception {
        File dir = Files.createTempDirectory("cpi-kafka-plus-tls").toFile();
        run(dir, "keytool", "-genkeypair", "-alias", "kafka", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "2", "-dname", "CN=localhost, OU=test, O=test, L=test, ST=test, C=DE",
                "-ext", "SAN=dns:localhost,dns:kafka,ip:127.0.0.1",
                "-keystore", "server.jks", "-storepass", STORE_PASSWORD,
                "-keypass", STORE_PASSWORD, "-storetype", "JKS");
        run(dir, "keytool", "-exportcert", "-alias", "kafka", "-keystore", "server.jks",
                "-storepass", STORE_PASSWORD, "-rfc", "-file", "kafka.crt");
        run(dir, "keytool", "-importcert", "-alias", "kafka", "-keystore", "truststore.jks",
                "-storepass", STORE_PASSWORD, "-file", "kafka.crt", "-noprompt");
        write(new File(dir, "keystore_creds"), STORE_PASSWORD);
        write(new File(dir, "key_creds"), STORE_PASSWORD);
        write(new File(dir, "truststore_creds"), STORE_PASSWORD);
        // Files.createTempDirectory creates the directory with mode 0700. The Confluent image runs
        // the broker as a non-root user, which then cannot traverse /etc/kafka/secrets and silently
        // starts without a certificate — the TLS handshake fails with a bare handshake_failure.
        makeWorldReadable(dir);
        for (File f : dir.listFiles()) {
            makeWorldReadable(f);
        }
        return dir;
    }

    private static void makeWorldReadable(File file) {
        file.setReadable(true, false);
        if (file.isDirectory()) {
            file.setExecutable(true, false);
        }
    }

    private static void run(File workDir, String... command) throws Exception {
        Process p = new ProcessBuilder(command).directory(workDir).redirectErrorStream(true).start();
        String output = new String(readAll(p.getInputStream()), StandardCharsets.UTF_8);
        int exit = p.waitFor();
        if (exit != 0) {
            throw new IllegalStateException("Command failed (" + exit + "): "
                    + String.join(" ", command) + "\n" + output);
        }
    }

    private static byte[] readAll(java.io.InputStream in) throws Exception {
        java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
        byte[] chunk = new byte[4096];
        int read;
        while ((read = in.read(chunk)) != -1) {
            buffer.write(chunk, 0, read);
        }
        return buffer.toByteArray();
    }

    private static void write(File file, String content) throws Exception {
        try (OutputStreamWriter w = new OutputStreamWriter(
                new FileOutputStream(file), StandardCharsets.UTF_8)) {
            w.write(content);
        }
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
