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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Regression tests for Avro schema evolution and Schema Registry subject naming.
 */
public class SchemaEvolutionIT {

    private static final String NAMESPACE = "com.finkeflo.cpi.kafka.generated";

    private static DefaultCamelContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        KafkaTestInfrastructure.requireDockerAvailable();
        KafkaTestInfrastructure.startKafkaWithSchemaRegistry();

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
    public void testIncompatibleEvolutionIsRejectedAndAdapterSurfacesFailure() throws Exception {
        String topic = "it-schema-incompat-" + System.nanoTime();
        String subject = topic + "-value";
        String recordName = sanitizeAvroName(topic);
        KafkaTestInfrastructure.createTopic(topic, 1);

        SchemaRegistryClient registryClient = createRegistryClient();
        registryClient.updateCompatibility(subject, "backward");

        produceWithAdapter(topic, "{\"name\":\"Ada\",\"age\":37,\"email\":\"ada@example.com\"}");
        SchemaMetadata v1 = registryClient.getLatestSchemaMetadata(subject);
        Assert.assertEquals("Initial adapter-produced schema should be version 1", 1, v1.getVersion());
        Assert.assertTrue("v1 should infer age as long", v1.getSchema().contains("\"name\":\"age\",\"type\":\"long\""));

        Schema incompatible = buildPersonSchema(recordName, "string");
        Assert.assertFalse("Changing required field age from long to string must be incompatible",
                registryClient.testCompatibility(subject, incompatible));
        Throwable registrationFailure = registerExpectingFailure(registryClient, subject, incompatible);
        Assert.assertTrue("Expected Schema Registry compatibility error but got: "
                        + describeCauseChain(registrationFailure),
                hasCauseNamed(registrationFailure, "RestClientException")
                        && causeChainContains(registrationFailure, "incompatible"));

        CpiKafkaPlusProducer producer = createAvroProducer(topic);
        try {
            Throwable adapterFailure = sendExpectingFailure(producer,
                    "{\"name\":\"Ada\",\"age\":\"thirty-seven\",\"email\":\"ada@example.com\"}");
            Assert.assertTrue("Adapter must surface the incompatible JSON/schema failure but got: "
                            + describeCauseChain(adapterFailure),
                    causeChainContains(adapterFailure, "avro serialization failed")
                            || causeChainContains(adapterFailure, "expected long")
                            || causeChainContains(adapterFailure, "value_string"));
        } finally {
            producer.doStop();
        }

        List<Integer> versions = registryClient.getAllVersions(subject);
        Assert.assertEquals("Rejected incompatible schema must not create a second version",
                Collections.singletonList(Integer.valueOf(1)), versions);
    }

    @Test
    public void testSameSchemaIsAcceptedAndKeepsSingleVersion() throws Exception {
        String topic = "it-schema-compatible-" + System.nanoTime();
        String subject = topic + "-value";
        KafkaTestInfrastructure.createTopic(topic, 1);

        SchemaRegistryClient registryClient = createRegistryClient();
        registryClient.updateCompatibility(subject, "backward");

        produceWithAdapter(topic, "{\"name\":\"Ada\",\"age\":37,\"email\":\"ada@example.com\"}");
        SchemaMetadata first = registryClient.getLatestSchemaMetadata(subject);

        // JSON inference currently creates required fields without defaults, so this test uses
        // the same schema as the compatible case and verifies Schema Registry reuses one version.
        produceWithAdapter(topic, "{\"name\":\"Grace\",\"age\":39,\"email\":\"grace@example.com\"}");
        SchemaMetadata second = registryClient.getLatestSchemaMetadata(subject);

        Assert.assertEquals("Same inferred schema should reuse the schema id", first.getId(), second.getId());
        Assert.assertEquals("Same inferred schema should stay on version 1", 1, second.getVersion());
        Assert.assertEquals("Only one compatible schema version should exist",
                Collections.singletonList(Integer.valueOf(1)), registryClient.getAllVersions(subject));
    }

    @Test
    public void testSubjectNameStrategyChangesSubjectName() throws Exception {
        String topic = "it-schema-subject-" + System.nanoTime();
        String recordName = sanitizeAvroName(topic);
        String fullName = NAMESPACE + "." + recordName;
        String topicNameSubject = topic + "-value";
        String topicRecordNameSubject = topic + "-" + fullName;

        SchemaRegistryClient registryClient = createRegistryClient();
        Schema schema = buildPersonSchema(recordName, "long");

        serializeWithStrategy(topic, schema, "io.confluent.kafka.serializers.subject.TopicNameStrategy");
        serializeWithStrategy(topic, schema, "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy");

        Collection<String> subjects = registryClient.getAllSubjects();
        Assert.assertTrue("TopicNameStrategy should register subject " + topicNameSubject,
                subjects.contains(topicNameSubject));
        Assert.assertTrue("TopicRecordNameStrategy should register subject " + topicRecordNameSubject,
                subjects.contains(topicRecordNameSubject));
        Assert.assertFalse("Non-default subject strategy must use a different subject name",
                topicNameSubject.equals(topicRecordNameSubject));
    }

    private CpiKafkaPlusProducer createAvroProducer(String topic) throws Exception {
        Map<String, String> params = new HashMap<String, String>();
        params.put("schemaRegistryEnabled", "true");
        params.put("schemaRegistryUrl", KafkaTestInfrastructure.getSchemaRegistryUrl());
        params.put("avroValueSerialization", "true");
        params.put("autoRegisterSchemas", "true");
        params.put("subjectNameStrategy", "TopicNameStrategy");
        params.put("deliveryTimeoutSeconds", "5");
        String uri = KafkaTestInfrastructure.buildEndpointUri(topic, "unused-schema-evolution-group", params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
        return new CpiKafkaPlusProducer(endpoint);
    }

    private void produceWithAdapter(String topic, String jsonBody) throws Exception {
        CpiKafkaPlusProducer producer = createAvroProducer(topic);
        try {
            producer.doStart();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody(jsonBody);
            producer.process(exchange);
        } finally {
            producer.doStop();
        }
    }

    private Throwable sendExpectingFailure(final CpiKafkaPlusProducer producer, final String body)
            throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Void> future = executor.submit(new Callable<Void>() {
            public Void call() throws Exception {
                producer.doStart();
                Exchange exchange = new DefaultExchange(ctx);
                exchange.getIn().setBody(body);
                producer.process(exchange);
                return null;
            }
        });
        try {
            future.get(20000, TimeUnit.MILLISECONDS);
            Assert.fail("Expected incompatible Avro send to fail, but it succeeded");
            return null;
        } catch (TimeoutException e) {
            producer.doStop();
            future.cancel(true);
            Assert.fail("Incompatible Avro failure did not complete within the bounded wait");
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        } finally {
            executor.shutdownNow();
        }
    }

    private Throwable registerExpectingFailure(final SchemaRegistryClient registryClient,
                                               final String subject,
                                               final Schema schema) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Void> future = executor.submit(new Callable<Void>() {
            public Void call() throws Exception {
                registryClient.register(subject, schema);
                return null;
            }
        });
        try {
            future.get(20000, TimeUnit.MILLISECONDS);
            Assert.fail("Expected incompatible schema registration to fail, but it succeeded");
            return null;
        } catch (TimeoutException e) {
            future.cancel(true);
            Assert.fail("Incompatible schema registration did not complete within the bounded wait");
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        } finally {
            executor.shutdownNow();
        }
    }

    private void serializeWithStrategy(String topic, Schema schema, String strategyClassName) {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        Map<String, Object> config = new HashMap<String, Object>();
        config.put("schema.registry.url", KafkaTestInfrastructure.getSchemaRegistryUrl());
        config.put("auto.register.schemas", Boolean.TRUE);
        config.put("value.subject.name.strategy", strategyClassName);
        serializer.configure(config, false);
        try {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("name", "Ada");
            record.put("age", Long.valueOf(37L));
            record.put("email", "ada@example.com");
            byte[] bytes = serializer.serialize(topic, record);
            Assert.assertEquals("Confluent Avro wire format should start with magic byte", 0, bytes[0]);
            Assert.assertTrue("Schema id should be embedded in wire format",
                    ByteBuffer.wrap(bytes, 1, 4).getInt() > 0);
        } finally {
            serializer.close();
        }
    }

    private SchemaRegistryClient createRegistryClient() {
        return new CachedSchemaRegistryClient(
                Collections.singletonList(KafkaTestInfrastructure.getSchemaRegistryUrl()), 100);
    }

    private Schema buildPersonSchema(String recordName, String ageType) {
        String schemaJson = "{"
                + "\"type\":\"record\","
                + "\"name\":\"" + recordName + "\","
                + "\"namespace\":\"" + NAMESPACE + "\","
                + "\"fields\":["
                + "{\"name\":\"name\",\"type\":\"string\"},"
                + "{\"name\":\"age\",\"type\":\"" + ageType + "\"},"
                + "{\"name\":\"email\",\"type\":\"string\"}"
                + "]} ";
        return new Schema.Parser().parse(schemaJson);
    }

    private String sanitizeAvroName(String name) {
        String sanitized = name.replaceAll("[^A-Za-z0-9_]", "_");
        if (sanitized.length() == 0 || Character.isDigit(sanitized.charAt(0))) {
            sanitized = "_" + sanitized;
        }
        return sanitized;
    }

    private boolean hasCauseNamed(Throwable failure, String simpleClassName) {
        Throwable current = failure;
        while (current != null) {
            if (simpleClassName.equals(current.getClass().getSimpleName())) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean causeChainContains(Throwable failure, String needle) {
        String lowerNeedle = needle.toLowerCase();
        Throwable current = failure;
        while (current != null) {
            String className = current.getClass().getName().toLowerCase();
            String message = current.getMessage() == null ? "" : current.getMessage().toLowerCase();
            if (className.contains(lowerNeedle) || message.contains(lowerNeedle)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private String describeCauseChain(Throwable failure) {
        StringBuilder sb = new StringBuilder();
        Throwable current = failure;
        while (current != null) {
            if (sb.length() > 0) {
                sb.append(" -> ");
            }
            sb.append(current.getClass().getSimpleName()).append(": ").append(current.getMessage());
            current = current.getCause();
        }
        return sb.toString();
    }
}
