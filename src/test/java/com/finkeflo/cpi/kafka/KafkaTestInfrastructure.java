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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared test infrastructure for Testcontainers-based integration tests.
 * Provides lazy-started Kafka and Schema Registry containers.
 */
public final class KafkaTestInfrastructure {

    private static final String CONFLUENT_VERSION = "7.9.5";
    public static final String SASL_USERNAME = "testuser";
    public static final String SASL_PASSWORD = "test-secret";
    public static final String SASL_CREDENTIAL_ALIAS = "sasl-test-credential";

    private static ConfluentKafkaContainer kafkaContainer;
    private static ConfluentKafkaContainer saslKafkaContainer;
    private static GenericContainer<?> schemaRegistryContainer;
    private static Network network;
    private static Network saslNetwork;

    private KafkaTestInfrastructure() {}

    /**
     * Requires Docker to be available and aborts (fails) the test if it is not.
     * Integration tests must never be silently skipped — a missing Docker daemon
     * is treated as a hard error so CI cannot pass without running them.
     */
    public static void requireDockerAvailable() {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new IllegalStateException(
                    "Docker is not available — integration tests require a running Docker daemon. "
                    + "Aborting instead of skipping.");
        }
    }

    /**
     * Start Kafka container (lazy, idempotent).
     */
    public static synchronized void startKafka() {
        if (kafkaContainer != null && kafkaContainer.isRunning()) {
            return;
        }
        network = Network.newNetwork();
        kafkaContainer = new ConfluentKafkaContainer(
                DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION))
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withListener("kafka:19092")
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName("kafka"));
        kafkaContainer.start();
    }

    /**
     * Start a separate Kafka container that requires SASL/PLAIN on the external listener.
     * The existing PLAINTEXT container remains untouched for the other integration tests.
     */
    public static synchronized void startKafkaWithSasl() {
        if (saslKafkaContainer != null && saslKafkaContainer.isRunning()) {
            return;
        }
        saslNetwork = Network.newNetwork();
        saslKafkaContainer = new ConfluentKafkaContainer(
                DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION))
                .withNetwork(saslNetwork)
                .withNetworkAliases("sasl-kafka")
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName("sasl-kafka"))
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                        "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT")
                .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"admin\" password=\"admin-secret\" "
                        + "user_admin=\"admin-secret\" "
                        + "user_" + SASL_USERNAME + "=\"" + SASL_PASSWORD + "\";");
        saslKafkaContainer.start();
    }

    /**
     * Start Kafka + Schema Registry containers (lazy, idempotent).
     */
    @SuppressWarnings("resource")
    public static synchronized void startKafkaWithSchemaRegistry() {
        startKafka();
        if (schemaRegistryContainer != null && schemaRegistryContainer.isRunning()) {
            return;
        }
        schemaRegistryContainer = new GenericContainer<>(
                DockerImageName.parse("confluentinc/cp-schema-registry:" + CONFLUENT_VERSION))
                .withNetwork(network)
                .dependsOn(kafkaContainer)
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:19092")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200)
                        .withStartupTimeout(java.time.Duration.ofSeconds(120)));
        schemaRegistryContainer.start();
    }

    public static String getBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }

    public static String getSaslBootstrapServers() {
        return saslKafkaContainer.getBootstrapServers();
    }

    public static String getSchemaRegistryUrl() {
        return "http://" + schemaRegistryContainer.getHost() + ":"
                + schemaRegistryContainer.getMappedPort(8081);
    }

    /**
     * Create a topic with the given number of partitions.
     */
    public static void createTopic(String topic, int partitions) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);
        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topic, partitions, (short) 1);
            admin.createTopics(Collections.singletonList(newTopic)).all().get();
        }
    }

    /**
     * Create a topic on the SASL-enabled broker.
     */
    public static void createSaslTopic(String topic, int partitions)
            throws ExecutionException, InterruptedException {
        Properties props = buildSaslClientProperties(getSaslBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);
        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topic, partitions, (short) 1);
            admin.createTopics(Collections.singletonList(newTopic)).all().get();
        }
    }

    /**
     * Produce string messages to the given topic.
     * Keys and values lists must have the same length. Null keys are allowed.
     */
    public static void produceStringMessages(String topic, List<String> keys, List<String> values) {
        produceStringMessages(topic, keys, values, Collections.<List<Header>>emptyList());
    }

    /**
     * Produce string messages with optional headers.
     */
    public static void produceStringMessages(String topic, List<String> keys, List<String> values,
                                              List<List<Header>> headersList) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < values.size(); i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, keys.get(i), values.get(i));
                if (!headersList.isEmpty() && i < headersList.size()) {
                    for (Header header : headersList.get(i)) {
                        record.headers().add(header);
                    }
                }
                producer.send(record).get();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to produce messages", e);
        }
    }

    /**
     * Produce string messages asynchronously (no per-message blocking).
     * Much faster for large volumes (72,000+ messages). Calls flush() at end.
     */
    public static void produceStringMessagesAsync(String topic, List<String> keys, List<String> values) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < values.size(); i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, keys.get(i), values.get(i));
                producer.send(record);
            }
            producer.flush();
        } catch (Exception e) {
            throw new RuntimeException("Failed to produce messages async", e);
        }
    }

    /**
     * Consume all messages from a topic with a timeout.
     * Uses a unique consumer group per invocation to read from the beginning.
     */
    public static List<ConsumerRecord<String, String>> consumeAllMessages(
            String topic, int expectedCount, long timeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (result.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    result.add(record);
                }
            }
        }
        return result;
    }

    /**
     * Consume all messages from the SASL-enabled broker.
     */
    public static List<ConsumerRecord<String, String>> consumeAllSaslMessages(
            String topic, int expectedCount, long timeoutMs) {
        Properties props = buildSaslClientProperties(getSaslBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-sasl-consumer-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (result.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    result.add(record);
                }
            }
        }
        return result;
    }

    /**
     * Build a standard Camel endpoint URI for integration tests.
     */
    public static String buildEndpointUri(String topic, String groupId, Map<String, String> extraParams) {
        StringBuilder sb = new StringBuilder();
        sb.append("cpi-kafka-plus:").append(topic);
        sb.append("?bootstrapServers=").append(getBootstrapServers());
        sb.append("&groupId=").append(groupId);
        sb.append("&securityProtocol=PLAINTEXT");
        sb.append("&autoOffsetReset=earliest");
        for (Map.Entry<String, String> entry : extraParams.entrySet()) {
            sb.append("&").append(entry.getKey()).append("=").append(entry.getValue());
        }
        return sb.toString();
    }

    /**
     * Build a Camel endpoint URI for the SASL-enabled Kafka integration tests.
     */
    public static String buildSaslEndpointUri(String topic, String groupId, Map<String, String> extraParams) {
        StringBuilder sb = new StringBuilder();
        sb.append("cpi-kafka-plus:").append(topic);
        sb.append("?bootstrapServers=").append(getSaslBootstrapServers());
        sb.append("&groupId=").append(groupId);
        sb.append("&securityProtocol=SASL_PLAINTEXT");
        sb.append("&saslMechanism=PLAIN");
        sb.append("&credentialAlias=").append(SASL_CREDENTIAL_ALIAS);
        sb.append("&autoOffsetReset=earliest");
        for (Map.Entry<String, String> entry : extraParams.entrySet()) {
            sb.append("&").append(entry.getKey()).append("=").append(entry.getValue());
        }
        return sb.toString();
    }

    /**
     * Build a standard Camel endpoint URI with default params.
     */
    public static String buildEndpointUri(String topic, String groupId) {
        return buildEndpointUri(topic, groupId, Collections.<String, String>emptyMap());
    }

    /**
     * Create a RecordHeader helper.
     */
    public static Header header(String key, String value) {
        return new RecordHeader(key, value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static Properties buildSaslClientProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"" + SASL_USERNAME + "\" "
                + "password=\"" + SASL_PASSWORD + "\";");
        return props;
    }
}
