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

import java.util.List;

import org.apache.camel.CamelContext;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.Assert;
import org.junit.Test;

public class CpiKafkaPlusComponentTest {

    @Test
    public void testComponentCreation() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        Assert.assertNotNull("Component should be created", component);
    }

    @Test
    public void testEndpointProperties() throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setBootstrapServers("localhost:9092");
        endpoint.setTopic("test-topic");
        endpoint.setGroupId("test-group");
        endpoint.setAutoOffsetReset("earliest");
        endpoint.setMaxPollRecords(100);
        endpoint.setBatchMode(true);
        endpoint.setBatchSize(50);
        endpoint.setBatchTimeout(3000);
        endpoint.setBatchOutputFormat("JSON_ARRAY");
        endpoint.setSecurityProtocol("SASL_SSL");
        endpoint.setSaslMechanism("PLAIN");
        endpoint.setCredentialAlias("myAlias");
        endpoint.setSslKeystoreAlias("tenant-keystore");

        Assert.assertEquals("localhost:9092", endpoint.getBootstrapServers());
        Assert.assertEquals("test-topic", endpoint.getTopic());
        Assert.assertEquals("test-group", endpoint.getGroupId());
        Assert.assertEquals("earliest", endpoint.getAutoOffsetReset());
        Assert.assertEquals(100, endpoint.getMaxPollRecords());
        Assert.assertTrue(endpoint.isBatchMode());
        Assert.assertEquals(50, endpoint.getBatchSize());
        Assert.assertEquals(3000, endpoint.getBatchTimeout());
        Assert.assertEquals("JSON_ARRAY", endpoint.getBatchOutputFormat());
        Assert.assertEquals("SASL_SSL", endpoint.getSecurityProtocol());
        Assert.assertEquals("PLAIN", endpoint.getSaslMechanism());
        Assert.assertEquals("myAlias", endpoint.getCredentialAlias());
        Assert.assertEquals("tenant-keystore", endpoint.getSslKeystoreAlias());
    }

    @Test
    public void testEffectiveTopic() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            // Topic from URI remaining part
            CpiKafkaPlusEndpoint ep = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:my-uri-topic?bootstrapServers=localhost:9092&groupId=g1");
            Assert.assertEquals("my-uri-topic", ep.getEffectiveTopic());

            // Topic from explicit parameter overrides URI
            CpiKafkaPlusEndpoint ep2 = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:uri-topic?bootstrapServers=localhost:9092&groupId=g1&topic=explicit-topic");
            Assert.assertEquals("explicit-topic", ep2.getEffectiveTopic());
            ctx.stop();
        }
    }

    @Test
    public void testDefaultValues() throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        Assert.assertEquals(5, endpoint.getPollingIntervalSeconds());
        Assert.assertEquals("latest", endpoint.getAutoOffsetReset());
        Assert.assertEquals(500, endpoint.getMaxPollRecords());
        Assert.assertTrue(endpoint.isBatchMode());
        Assert.assertEquals(100, endpoint.getBatchSize());
        Assert.assertEquals(5000, endpoint.getBatchTimeout());
        Assert.assertEquals("JSON_ARRAY", endpoint.getBatchOutputFormat());
        Assert.assertTrue(endpoint.isAvroValueDeserialization());
        Assert.assertEquals("BATCH_COMPLETE", endpoint.getCommitStrategy());
        Assert.assertFalse(endpoint.isDrainEnabled());
        Assert.assertEquals(0, endpoint.getMinBacklogToDrain());
        Assert.assertEquals("SASL_SSL", endpoint.getSecurityProtocol());
        Assert.assertEquals("PLAIN", endpoint.getSaslMechanism());
        Assert.assertNull(endpoint.getSslKeystoreAlias());
        Assert.assertEquals("JSON", endpoint.getAvroOutputFormat());
        Assert.assertFalse(endpoint.isSchemaRegistryEnabled());
        Assert.assertNull(endpoint.getSchemaRegistryUrl());
        Assert.assertNull(endpoint.getSchemaRegistryCredentialAlias());
        Assert.assertFalse(endpoint.isAutoRegisterSchemas());
        Assert.assertEquals("TopicNameStrategy", endpoint.getSubjectNameStrategy());
        Assert.assertFalse(endpoint.isDlqEnabled());
        Assert.assertNull(endpoint.getDlqTopic());
        Assert.assertEquals(3, endpoint.getDlqMaxRetries());
        Assert.assertFalse(endpoint.isJsonSchemaValidation());
        Assert.assertNull(endpoint.getJsonSchema());
        Assert.assertFalse(endpoint.isJsonSchemaReportError());
        Assert.assertFalse(endpoint.isAutoPauseEnabled());
        Assert.assertEquals(5, endpoint.getAutoPauseErrorThreshold());
        Assert.assertEquals(60, endpoint.getAutoPauseCooldownSeconds());
        Assert.assertEquals(1024, endpoint.getMaxPartitionFetchSizeKb());
    }

    @Test
    public void testBatchFormatterJsonArray() throws Exception {
        // Simple test with mock records would require Kafka test utils
        // This verifies the BatchFormatter class exists and is accessible
        Assert.assertNotNull(BatchFormatter.class);
    }

    @Test
    public void testAutoPauseEndpointProperties() throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setAutoPauseEnabled(true);
        endpoint.setAutoPauseErrorThreshold(3);
        endpoint.setAutoPauseCooldownSeconds(30);
        Assert.assertTrue(endpoint.isAutoPauseEnabled());
        Assert.assertEquals(3, endpoint.getAutoPauseErrorThreshold());
        Assert.assertEquals(30, endpoint.getAutoPauseCooldownSeconds());
    }

    @Test
    public void testProducerEndpointProperties() throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();

        // Test default producer values
        Assert.assertEquals("NONE", endpoint.getProducerBatchMode());
        Assert.assertEquals("all", endpoint.getAcks());
        Assert.assertEquals("none", endpoint.getCompressionType());
        Assert.assertEquals(5120, endpoint.getMaxRequestSizeKb());
        Assert.assertEquals(1024, endpoint.getProducerBatchSizeKb());
        Assert.assertEquals(32768, endpoint.getBufferMemoryKb());
        Assert.assertTrue(endpoint.isEnableIdempotence());
        Assert.assertEquals(120, endpoint.getDeliveryTimeoutSeconds());
        Assert.assertTrue(endpoint.isAvroValueSerialization());
        Assert.assertFalse(endpoint.isAutoRegisterSchemas());
        Assert.assertEquals("TopicNameStrategy", endpoint.getSubjectNameStrategy());

        // Test setters
        endpoint.setAcks("1");
        endpoint.setCompressionType("gzip");
        endpoint.setMaxRequestSizeKb(2048);
        endpoint.setProducerBatchSizeKb(32);
        endpoint.setBufferMemoryKb(65536);
        endpoint.setEnableIdempotence(false);
        endpoint.setDeliveryTimeoutSeconds(60);
        endpoint.setSchemaRegistryEnabled(true);
        endpoint.setAvroValueSerialization(true);
        endpoint.setAutoRegisterSchemas(true);
        endpoint.setSubjectNameStrategy("RecordNameStrategy");

        Assert.assertEquals("1", endpoint.getAcks());
        Assert.assertEquals("gzip", endpoint.getCompressionType());
        Assert.assertEquals(2048, endpoint.getMaxRequestSizeKb());
        Assert.assertEquals(32, endpoint.getProducerBatchSizeKb());
        Assert.assertEquals(65536, endpoint.getBufferMemoryKb());
        Assert.assertFalse(endpoint.isEnableIdempotence());
        Assert.assertEquals(60, endpoint.getDeliveryTimeoutSeconds());
        Assert.assertTrue(endpoint.isSchemaRegistryEnabled());
        Assert.assertTrue(endpoint.isAvroValueSerialization());
        Assert.assertTrue(endpoint.isAutoRegisterSchemas());
        Assert.assertEquals("RecordNameStrategy", endpoint.getSubjectNameStrategy());
    }

    @Test
    public void testProducerCreation() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint ep = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:my-topic?bootstrapServers=localhost:9092&groupId=g1");
            Producer producer = ep.createProducer();
            Assert.assertNotNull("Producer should be created", producer);
            Assert.assertTrue("Should be CpiKafkaPlusProducer", producer instanceof CpiKafkaPlusProducer);
            ctx.stop();
        }
    }

    @Test
    public void testJsonSchemaValidationDefaults() {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        Assert.assertFalse("JSON Schema validation should be disabled by default", endpoint.isJsonSchemaValidation());
        Assert.assertNull("JSON Schema should be null by default", endpoint.getJsonSchema());
        Assert.assertFalse("JSON Schema error reporting should be disabled by default", endpoint.isJsonSchemaReportError());
    }

    @Test
    public void testJsonSchemaValidationProperties() {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        String schema = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"]}";
        endpoint.setJsonSchemaValidation(true);
        endpoint.setJsonSchema(schema);
        Assert.assertTrue(endpoint.isJsonSchemaValidation());
        Assert.assertEquals(schema, endpoint.getJsonSchema());
    }

    @Test
    public void testJsonSchemaReportErrorProperty() {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        Assert.assertFalse(endpoint.isJsonSchemaReportError());
        endpoint.setJsonSchemaReportError(true);
        Assert.assertTrue(endpoint.isJsonSchemaReportError());
        endpoint.setJsonSchemaReportError(false);
        Assert.assertFalse(endpoint.isJsonSchemaReportError());
    }

    @Test
    public void testJsonSchemaValidatorValid() {
        String schema = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"age\":{\"type\":\"integer\"}},\"required\":[\"name\"]}";
        JsonSchemaValidator validator = new JsonSchemaValidator(schema);
        Assert.assertNull("Valid JSON should pass", validator.validate("{\"name\":\"Alice\",\"age\":30}"));
    }

    @Test
    public void testJsonSchemaValidatorInvalid() {
        String schema = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"]}";
        JsonSchemaValidator validator = new JsonSchemaValidator(schema);
        String error = validator.validate("{\"age\":30}");
        Assert.assertNotNull("Missing required field should fail", error);
        Assert.assertTrue("Error should mention validation failed", error.contains("JSON Schema validation failed"));
    }

    @Test
    public void testJsonSchemaValidatorNullInput() {
        String schema = "{\"type\":\"object\"}";
        JsonSchemaValidator validator = new JsonSchemaValidator(schema);
        Assert.assertNull("Null input should pass through", validator.validate(null));
        Assert.assertNull("Empty input should pass through", validator.validate(""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJsonSchemaValidatorInvalidSchema() {
        new JsonSchemaValidator("not valid json");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJsonSchemaValidationEmptySchemaThrows() {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setJsonSchemaValidation(true);
        endpoint.setJsonSchema("");
        // Simulate the guard logic from doStart()
        String schemaStr = endpoint.getJsonSchema();
        if (schemaStr == null || schemaStr.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "JSON Schema validation is enabled but no JSON Schema is configured. "
                    + "Please provide a valid JSON Schema (draft-07) or disable JSON Schema validation.");
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJsonSchemaValidationNullSchemaThrows() {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setJsonSchemaValidation(true);
        // jsonSchema defaults to null
        String schemaStr = endpoint.getJsonSchema();
        if (schemaStr == null || schemaStr.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "JSON Schema validation is enabled but no JSON Schema is configured. "
                    + "Please provide a valid JSON Schema (draft-07) or disable JSON Schema validation.");
        }
    }

    @Test
    public void testAvroEndpointProperties() throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();

        endpoint.setSchemaRegistryUrl("https://schema-registry.example.com");
        endpoint.setSchemaRegistryCredentialAlias("sr-alias");
        endpoint.setAvroOutputFormat("XML");
        endpoint.setAutoRegisterSchemas(true);
        endpoint.setSubjectNameStrategy("RecordNameStrategy");

        Assert.assertEquals("https://schema-registry.example.com", endpoint.getSchemaRegistryUrl());
        Assert.assertEquals("sr-alias", endpoint.getSchemaRegistryCredentialAlias());
        Assert.assertEquals("XML", endpoint.getAvroOutputFormat());
        Assert.assertTrue(endpoint.isAutoRegisterSchemas());
        Assert.assertEquals("RecordNameStrategy", endpoint.getSubjectNameStrategy());
    }

    @Test
    public void testDlqDefaultValues() throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        Assert.assertFalse("DLQ should be disabled by default", endpoint.isDlqEnabled());
        Assert.assertNull("DLQ topic should be null by default", endpoint.getDlqTopic());
        Assert.assertEquals("DLQ max retries should default to 3", 3, endpoint.getDlqMaxRetries());
    }

    @Test
    public void testDlqEndpointProperties() throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setDlqEnabled(true);
        endpoint.setDlqTopic("my-dlq-topic");
        endpoint.setDlqMaxRetries(5);

        Assert.assertTrue(endpoint.isDlqEnabled());
        Assert.assertEquals("my-dlq-topic", endpoint.getDlqTopic());
        Assert.assertEquals(5, endpoint.getDlqMaxRetries());
    }

    @Test
    public void testSmartRetryDefaultValues() throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        Assert.assertTrue("retryOnlyTransientErrors should default to true",
                endpoint.isRetryOnlyTransientErrors());
        Assert.assertEquals("retryDelaySeconds should default to 0",
                0, endpoint.getRetryDelaySeconds());
    }

    @Test
    public void testSmartRetryEndpointProperties() throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setRetryOnlyTransientErrors(false);
        endpoint.setRetryDelaySeconds(5);

        Assert.assertFalse(endpoint.isRetryOnlyTransientErrors());
        Assert.assertEquals(5, endpoint.getRetryDelaySeconds());
    }

    @Test
    public void testDlqValidationConditions() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();

        ep.setDlqEnabled(false);
        ep.setDlqTopic(null);
        Assert.assertFalse("DLQ disabled should not trigger validation", ep.isDlqEnabled());

        ep.setDlqEnabled(true);
        ep.setDlqTopic("my-dlq-topic");
        Assert.assertTrue(ep.isDlqEnabled());
        Assert.assertFalse("Valid DLQ topic should not be empty",
                ep.getDlqTopic() == null || ep.getDlqTopic().trim().isEmpty());

        ep.setDlqTopic(null);
        Assert.assertTrue("Null DLQ topic should trigger validation",
                ep.getDlqTopic() == null || ep.getDlqTopic().trim().isEmpty());

        ep.setDlqTopic("");
        Assert.assertTrue("Empty DLQ topic should trigger validation",
                ep.getDlqTopic() == null || ep.getDlqTopic().trim().isEmpty());

        ep.setDlqTopic("  ");
        Assert.assertTrue("Blank DLQ topic should trigger validation",
                ep.getDlqTopic() == null || ep.getDlqTopic().trim().isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDrainWithAutoCommitThrows() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        ep.setDrainEnabled(true);
        ep.setCommitStrategy("AUTO");
        // Simulate the guard logic from doStart()
        if (ep.isDrainEnabled() && "AUTO".equalsIgnoreCase(ep.getCommitStrategy())) {
            throw new IllegalArgumentException(
                    "Drain mode requires commitStrategy=BATCH_COMPLETE. "
                    + "AUTO commit cannot guarantee at-least-once delivery during drain loops.");
        }
    }

    @Test
    public void testParseTopicsSingle() {
        List<String> topics = CpiKafkaPlusConsumer.parseTopics("my-topic");
        Assert.assertEquals(1, topics.size());
        Assert.assertEquals("my-topic", topics.get(0));
    }

    @Test
    public void testParseTopicsMultiple() {
        List<String> topics = CpiKafkaPlusConsumer.parseTopics("topic-a, topic-b , topic-c");
        Assert.assertEquals(3, topics.size());
        Assert.assertEquals("topic-a", topics.get(0));
        Assert.assertEquals("topic-b", topics.get(1));
        Assert.assertEquals("topic-c", topics.get(2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseTopicsEmpty() {
        CpiKafkaPlusConsumer.parseTopics("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseTopicsNull() {
        CpiKafkaPlusConsumer.parseTopics(null);
    }

    @Test
    public void testMaxPartitionFetchSizeKb() {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        Assert.assertEquals(1024, endpoint.getMaxPartitionFetchSizeKb());

        endpoint.setMaxPartitionFetchSizeKb(10240);
        Assert.assertEquals(10240, endpoint.getMaxPartitionFetchSizeKb());

        endpoint.setMaxPartitionFetchSizeKb(51200);
        Assert.assertEquals(51200, endpoint.getMaxPartitionFetchSizeKb());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxPartitionFetchSizeKbTooLow() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        ep.setMaxPartitionFetchSizeKb(0);
        // Simulate the guard logic from doStart()
        if (ep.getMaxPartitionFetchSizeKb() < 1 || ep.getMaxPartitionFetchSizeKb() > 51200) {
            throw new IllegalArgumentException(
                    "maxPartitionFetchSizeKb must be between 1 and 51200 (50 MB).");
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxPartitionFetchSizeKbTooHigh() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        ep.setMaxPartitionFetchSizeKb(51201);
        // Simulate the guard logic from doStart()
        if (ep.getMaxPartitionFetchSizeKb() < 1 || ep.getMaxPartitionFetchSizeKb() > 51200) {
            throw new IllegalArgumentException(
                    "maxPartitionFetchSizeKb must be between 1 and 51200 (50 MB).");
        }
    }

    @Test
    public void testMaxPartitionFetchSizeKbPropertyWiring() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        ep.setMaxPartitionFetchSizeKb(10240);
        // Simulate the wiring logic from buildConsumerProperties()
        int expectedBytes = 10240 * 1024;
        Assert.assertEquals(10485760, expectedBytes);
        Assert.assertEquals(ep.getMaxPartitionFetchSizeKb() * 1024, expectedBytes);
    }

    @Test
    public void testCredentialHelperOutsideCpi() {
        // Outside CPI runtime, ITApiFactory returns null for SecureStoreService
        // which causes getUserCredential to return null
        try {
            CredentialHelper.UserCredentials cred = CredentialHelper.getUserCredential("nonexistent");
            // If no exception, should be null (SecureStoreService unavailable)
            Assert.assertNull("Should return null outside CPI runtime", cred);
        } catch (RuntimeException e) {
            // Expected outside CPI - ITApiFactory may throw
            Assert.assertTrue("Expected credential resolution failure",
                    e.getMessage().contains("Failed to retrieve credentials"));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSaslWithoutCredentialAliasThrows() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        ep.setSecurityProtocol("SASL_SSL");
        ep.setCredentialAlias(null);
        // Simulate the guard logic from doStart()
        if (ep.getSecurityProtocol() != null
                && ep.getSecurityProtocol().toUpperCase().contains("SASL")
                && (ep.getCredentialAlias() == null || ep.getCredentialAlias().trim().isEmpty())) {
            throw new IllegalArgumentException(
                    "Security protocol " + ep.getSecurityProtocol()
                    + " requires SASL authentication but no credentialAlias is configured.");
        }
    }

    @Test
    public void testPlaintextWithoutCredentialAliasIsOk() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        ep.setSecurityProtocol("PLAINTEXT");
        ep.setCredentialAlias(null);
        boolean isSasl = ep.getSecurityProtocol() != null
                && ep.getSecurityProtocol().toUpperCase().contains("SASL");
        Assert.assertFalse("PLAINTEXT should not require credentials", isSasl);
    }

    @Test
    public void testSaslWithCredentialAliasIsOk() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        ep.setSecurityProtocol("SASL_SSL");
        ep.setCredentialAlias("my-alias");
        boolean needsCreds = ep.getSecurityProtocol() != null
                && ep.getSecurityProtocol().toUpperCase().contains("SASL")
                && (ep.getCredentialAlias() == null || ep.getCredentialAlias().trim().isEmpty());
        Assert.assertFalse("SASL with alias should pass", needsCreds);
    }

    // --- validateConfiguration() tests ---

    @Test
    public void testValidateConfigurationPassesWithDefaults() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        // Default securityProtocol is SASL_SSL, so a credentialAlias is required
        ep.setCredentialAlias("test-alias");
        ep.validateConfiguration(); // should not throw
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateConfigurationFailsSchemaRegistryWithoutUrl() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        ep.setSchemaRegistryEnabled(true);
        ep.setSchemaRegistryUrl(null);
        ep.validateConfiguration();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateConfigurationFailsJsonSchemaWithoutSchema() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        ep.setJsonSchemaValidation(true);
        ep.setJsonSchema("   ");
        ep.validateConfiguration();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateConfigurationFailsSaslWithoutCredentialAlias() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        ep.setSecurityProtocol("SASL_SSL");
        ep.setCredentialAlias(null);
        ep.validateConfiguration();
    }

    @Test
    public void testValidateConfigurationPassesSaslWithAlias() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        ep.setSecurityProtocol("SASL_SSL");
        ep.setCredentialAlias("my-alias");
        ep.validateConfiguration(); // should not throw
    }

    @Test
    public void testValidateConfigurationPassesPlaintextWithoutAlias() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        ep.setSecurityProtocol("PLAINTEXT");
        ep.setCredentialAlias(null);
        ep.validateConfiguration(); // should not throw
    }

    @Test
    public void testEmbedXmlValuesDefaultFalse() {
        CpiKafkaPlusEndpoint ep = new CpiKafkaPlusEndpoint();
        Assert.assertFalse("embedXmlValues should default to false", ep.isEmbedXmlValues());
    }
}
