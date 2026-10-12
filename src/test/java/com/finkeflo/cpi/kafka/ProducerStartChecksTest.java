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

import java.util.Properties;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Deployment-time checks of the receiver: reject values that can never work (#183), and only where the
 * field is in effect.
 */
public class ProducerStartChecksTest {

    private DefaultCamelContext ctx;

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        ctx.close();
    }

    @Test
    public void emptyBootstrapServersAreRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setBootstrapServers(" ");
        assertRejected(e, "bootstrapServers", "Bootstrap Servers");
    }

    @Test
    public void malformedJsonSchemaIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setJsonSchemaValidation(true);
        e.setJsonSchema("{\"type\": \"object\",");
        assertRejected(e, "jsonSchema", "JSON Schema");
    }

    @Test
    public void malformedJsonSchemaIsIgnoredWhileValidationIsOff() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setJsonSchemaValidation(false);
        e.setJsonSchema("{\"type\": \"object\",");
        assertStarts(e);
    }

    @Test
    public void malformedJsonSchemaIsIgnoredInBatchMode() throws Exception {
        // A batch is sent without validation, so its schema must not block the start (#263).
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("JSON_ARRAY");
        e.setJsonSchemaValidation(true);
        e.setJsonSchema("{\"type\": \"object\",");
        assertStarts(e);
    }

    @Test
    public void missingJsonSchemaIsIgnoredInBatchMode() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("XML_LIST");
        e.setJsonSchemaValidation(true);
        e.setJsonSchema(null);
        assertStarts(e);
    }

    @Test
    public void unknownProducerBatchModeIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("CSV");
        assertRejected(e, "producerBatchMode", "Batch Send Mode");
    }

    @Test
    public void producerBatchModeIsCaseInsensitiveAsAtRuntime() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("json_array");
        assertStarts(e);
    }

    @Test
    public void deliveryTimeoutBelowOneSecondIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setDeliveryTimeoutSeconds(0);
        assertRejected(e, "deliveryTimeoutSeconds", "Delivery Timeout (Seconds)");
    }

    @Test
    public void unsupportedSubjectNameStrategyIsRejectedForAvroSerialization() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setSchemaRegistryEnabled(true);
        e.setSchemaRegistryUrl("http://localhost:8081");
        e.setAvroValueSerialization(true);
        e.setSubjectNameStrategy("RecordNameStrategy");
        assertRejected(e, "subjectNameStrategy", "Subject Name Strategy");
    }

    @Test
    public void subjectNameStrategyIsIgnoredWithoutAvroSerialization() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setSchemaRegistryEnabled(false);
        e.setSubjectNameStrategy("RecordNameStrategy");
        assertStarts(e);
    }

    @Test
    public void subjectNameStrategyIsIgnoredWhileAvroSerializationIsOff() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setSchemaRegistryEnabled(true);
        e.setSchemaRegistryUrl("http://localhost:8081");
        e.setAvroValueSerialization(false);
        e.setSubjectNameStrategy("RecordNameStrategy");
        assertStarts(e);
    }

    @Test
    public void noSubjectNameStrategyMeansTopicNameStrategy() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setSchemaRegistryEnabled(true);
        e.setSchemaRegistryUrl("http://localhost:8081");
        e.setAvroValueSerialization(true);
        e.setSubjectNameStrategy(null);
        assertStarts(e);
    }

    @Test
    public void sizesThatCanNeverWorkAreRejected() throws Exception {
        // A negative size fails the Kafka client constructor; with a request size or a buffer of 0
        // every send fails (#261).
        CpiKafkaPlusEndpoint request = freshEndpoint();
        request.setMaxRequestSizeKb(0);
        assertRejected(request, "maxRequestSizeKb", "Max Request Size (KB)");
        CpiKafkaPlusEndpoint batch = freshEndpoint();
        batch.setProducerBatchSizeKb(-1);
        assertRejected(batch, "producerBatchSizeKb", "Producer Batch Size (KB)");
        CpiKafkaPlusEndpoint buffer = freshEndpoint();
        buffer.setBufferMemoryKb(0);
        assertRejected(buffer, "bufferMemoryKb", "Buffer Memory (KB)");
    }

    @Test
    public void theSmallestSizesThatWorkStartAndMakeAProducerConfig() throws Exception {
        CpiKafkaPlusEndpoint e = freshEndpoint();
        e.setMaxRequestSizeKb(1);
        e.setProducerBatchSizeKb(0);   // batching off
        e.setBufferMemoryKb(1);
        assertStarts(e);
        new ProducerConfig(producerProperties(e));
    }

    @Test
    public void bootstrapServersKafkaCannotReadAreRejected() throws Exception {
        // Each deployed and failed at the first send, with "Invalid url in bootstrap.servers" or
        // "Invalid port in bootstrap.servers" (#261).
        for (String value : new String[] {"broker1", "b1:9092,broker2", "b1:9092;b2:9092", "b1:65536",
                "b1:99999999999"}) {
            CpiKafkaPlusEndpoint e = freshEndpoint();
            e.setBootstrapServers(value);
            assertRejected(e, "bootstrapServers", "Bootstrap Servers");
        }
    }

    @Test
    public void bootstrapServersKafkaCanReadStillDeployAndMakeAProducerConfig() throws Exception {
        for (String value : new String[] {"b1:9092, b2:9092", "SASL_SSL://b1:9092", "b1:9092,", "b1:9092,,b2:9092"}) {
            CpiKafkaPlusEndpoint e = freshEndpoint();
            e.setBootstrapServers(value);
            assertStarts(e);
            // kafka-clients 4.3.1 refuses an empty entry, so the setter has dropped it (#261).
            new ProducerConfig(producerProperties(e));
        }
    }

    @Test
    public void aSchemaRegistryUrlWithoutHttpIsRejectedWithAvroSerialization() throws Exception {
        // Each deployed and failed at the first registry call (#261).
        for (String value : new String[] {"sr.example.com:8081", "localhost:8081", "sr.example.com",
                "ftp://sr.example.com", "file:///tmp/schemas"}) {
            assertRejected(avroEndpoint(value, true), "schemaRegistryUrl", "Schema Registry URL");
        }
    }

    @Test
    public void anHttpSchemaRegistryUrlInAnyCaseStarts() throws Exception {
        for (String value : new String[] {"HTTPS://sr:8081", "http://sr:8081/",
                "https://user:pw@sr:8081/registry"}) {
            assertStarts(avroEndpoint(value, true));
        }
    }

    @Test
    public void theSchemaRegistryUrlIsIgnoredWhileAvroSerializationIsOff() throws Exception {
        assertStarts(avroEndpoint("sr.example.com:8081", false));
    }

    @Test
    public void aRejectedSchemaRegistryUrlDoesNotShowItsCredentials() throws Exception {
        CpiKafkaPlusEndpoint e = avroEndpoint("sr-user:sr-secret@sr.example.com:8081", true);
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(e);
        try {
            producer.doStart();
            Assert.fail("expected the deployment to be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("got: '***@sr.example.com:8081'"));
            Assert.assertFalse(expected.getMessage(), expected.getMessage().contains("sr-secret"));
            Assert.assertFalse(expected.getMessage(), expected.getMessage().contains("sr-user"));
        } finally {
            producer.doStop();
        }
    }

    @Test
    public void receiverWithoutGroupIdStarts() throws Exception {
        // groupId is a sender field; the endpoint() URI has none on purpose.
        assertStarts(endpoint());
    }

    private CpiKafkaPlusEndpoint endpoint() throws Exception {
        return (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:orders?bootstrapServers=localhost:9999&securityProtocol=PLAINTEXT");
    }

    private int endpointCount;

    /** An endpoint no other step has changed: Camel caches endpoints by URI. */
    private CpiKafkaPlusEndpoint freshEndpoint() throws Exception {
        return (CpiKafkaPlusEndpoint) ctx.getEndpoint("cpi-kafka-plus:orders" + (++endpointCount)
                + "?bootstrapServers=localhost:9999&securityProtocol=PLAINTEXT");
    }

    private CpiKafkaPlusEndpoint avroEndpoint(String schemaRegistryUrl, boolean avroValueSerialization)
            throws Exception {
        CpiKafkaPlusEndpoint e = freshEndpoint();
        e.setSchemaRegistryEnabled(true);
        e.setSchemaRegistryUrl(schemaRegistryUrl);
        e.setAvroValueSerialization(avroValueSerialization);
        return e;
    }

    /** What the producer is built from, ready for Kafka's own validation in {@link ProducerConfig}. */
    private static Properties producerProperties(CpiKafkaPlusEndpoint e) {
        Properties props = ProducerConfigFactory.buildProducerProperties(e);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return props;
    }

    /** The message names both the UI label and the parameter, so it is findable from either side. */
    private static void assertRejected(CpiKafkaPlusEndpoint endpoint, String field, String uiLabel)
            throws Exception {
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        try {
            producer.doStart();
            Assert.fail("expected the deployment to be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage(), expected.getMessage().contains(field));
            Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("'" + uiLabel + "'"));
        } finally {
            try {
                producer.doStop();
            } catch (Exception ignored) {
                // start failed, so stop may too — not what is under test
            }
        }
    }

    private static void assertStarts(CpiKafkaPlusEndpoint endpoint) throws Exception {
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        try {
            producer.doStart();
        } finally {
            producer.doStop();
        }
    }
}
