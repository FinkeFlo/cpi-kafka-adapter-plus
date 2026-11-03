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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

/**
 * The sender reads values whose schema ID the Schema Registry has as a JSON Schema, and optionally
 * validates them against it. All values are invented.
 */
public class RegistryJsonSchemaTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TopicPartition TP = new TopicPartition("orders", 0);
    private static final String JSON = "{\"id\":\"a\"}";
    private static final String REQUIRES_NAME = "{\"type\":\"object\",\"required\":[\"name\"]}";
    private static final String AVRO_SCHEMA = "{\"type\":\"record\",\"name\":\"Order\","
            + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
    private static final String VALIDATE = "&registrySchemaValidation=true";

    private final List<DefaultCamelContext> contexts = new ArrayList<>();
    private final List<Exchange> delivered = new ArrayList<>();
    private final java.util.Map<Integer, MockResponse> schemas = new java.util.concurrent.ConcurrentHashMap<>();
    private MockWebServer registry;

    @Before
    public void startRegistry() throws Exception {
        CredentialHelper.setCredentialResolver(null);
        registry = new MockWebServer();
        registry.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                String path = request.getPath();
                if (path != null && path.startsWith("/schemas/ids/")) {
                    MockResponse response = schemas.get(Integer.valueOf(path.substring("/schemas/ids/".length())));
                    if (response != null) {
                        return response;
                    }
                }
                return new MockResponse().setResponseCode(404)
                        .setBody("{\"error_code\":40403,\"message\":\"Schema not found\"}");
            }
        });
        registry.start();
    }

    @After
    public void stop() throws Exception {
        CredentialHelper.setCredentialResolver(null);
        registry.shutdown();
        for (DefaultCamelContext ctx : contexts) {
            try {
                ctx.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
    }

    // --- Reading ---

    @Test
    public void aJsonSchemaValueIsTheJsonAfterTheHeader() throws Exception {
        registered(42, "JSON", REQUIRES_NAME);

        Message message = deliverOne(endpoint(""), framed(42, JSON));

        Assert.assertEquals(JSON, message.getBody(String.class));
        Assert.assertEquals(42, message.getHeader("CpiKafkaPlusSchemaId"));
        Assert.assertEquals("JSON", message.getHeader("CpiKafkaPlusSchemaType"));
    }

    @Test
    public void anAvroValueIsStillConvertedAndCarriesItsType() throws Exception {
        registered(7, null, AVRO_SCHEMA);

        Message message = deliverOne(endpoint(""), avro(7, "a"));

        Assert.assertEquals(JSON, message.getBody(String.class).replace(" ", ""));
        Assert.assertEquals(7, message.getHeader("CpiKafkaPlusSchemaId"));
        Assert.assertEquals("AVRO", message.getHeader("CpiKafkaPlusSchemaType"));
    }

    @Test
    public void aBatchMixesAvroAndJsonAndFetchesEachSchemaOnce() throws Exception {
        registered(7, "AVRO", AVRO_SCHEMA);
        registered(42, "JSON", REQUIRES_NAME);
        ConsumerRecords<byte[], byte[]> records = records(record(0, avro(7, "a")),
                record(1, framed(42, "{\"id\":\"b\"}")), record(2, framed(42, "[1,2]")));

        processor(endpoint("&batchMode=true&batchOutputFormat=JSON_ARRAY"), null, null)
                .processBatchRecords(null, records, false, new PollProgress(records));

        Assert.assertEquals(1, delivered.size());
        String body = delivered.get(0).getIn().getBody(String.class);
        Assert.assertTrue(body, body.replace(" ", "").contains("\"value\":{\"id\":\"b\"}"));
        Assert.assertTrue(body, body.replace(" ", "").contains("\"value\":[1,2]"));
        Assert.assertTrue(body, body.replace(" ", "").contains("\"value\":{\"id\":\"a\"}"));
        Assert.assertEquals("one request per schema ID", 2, registry.getRequestCount());
    }

    @Test
    public void anXmlListBatchWritesTheJsonAsText() throws Exception {
        registered(42, "JSON", REQUIRES_NAME);
        ConsumerRecords<byte[], byte[]> records = records(record(0, framed(42, JSON)));

        processor(endpoint("&batchMode=true&batchOutputFormat=XML_LIST"), null, null)
                .processBatchRecords(null, records, false, new PollProgress(records));

        String body = delivered.get(0).getIn().getBody(String.class);
        Assert.assertTrue(body, body.contains(JSON));
        Assert.assertFalse(body, body.contains("base64"));
    }

    @Test
    public void avroOutputFormatXmlLeavesJsonAsJsonAndIsLoggedOnce() throws Exception {
        registered(42, "JSON", REQUIRES_NAME);
        ConsumerRecords<byte[], byte[]> records = records(record(0, framed(42, JSON)), record(1, framed(42, JSON)));

        String logged = capture(() -> processor(endpoint("&avroOutputFormat=XML"), null, null)
                .processSingleRecords(null, records, false, new PollProgress(records)));

        Assert.assertEquals(2, delivered.size());
        Assert.assertEquals(JSON, delivered.get(1).getIn().getBody(String.class));
        Assert.assertEquals(logged, 1, logged.split("consumer\\.config\\.ineffective parameter=avroOutputFormat", -1).length - 1);
        Assert.assertTrue(logged, logged.contains("schemaType=JSON"));
    }

    @Test
    public void aProtobufValueReachesTheDlq() throws Exception {
        registered(9, "PROTOBUF", "syntax = \"proto3\"; message Order { string id = 1; }");

        assertDeadLettered(endpoint(""), framed(9, new byte[] {0, 10, 1, 97}));
    }

    @Test
    public void aValueThatIsNotUtf8ReachesTheDlq() throws Exception {
        registered(42, "JSON", REQUIRES_NAME);

        assertDeadLettered(endpoint(""), framed(42, new byte[] {'"', (byte) 0xFF, '"'}));
    }

    @Test
    public void aValueThatIsNotOneJsonValueReachesTheDlq() throws Exception {
        registered(42, "JSON", REQUIRES_NAME);

        assertDeadLettered(endpoint(""), framed(42, "{\"id\":\"a\"} {}"));
    }

    @Test
    public void aTruncatedAvroValueIsUnreadableNotARegistryFailure() throws Exception {
        registered(7, "AVRO", AVRO_SCHEMA);

        try {
            new AvroDeserializerHelper(endpoint("")).decode("orders", framed(7, new byte[] {20, 'a'}));
            Assert.fail("a truncated Avro value was read");
        } catch (RegistryValueException expected) {
            Assert.assertEquals(7, expected.getSchemaId());
            Assert.assertEquals("AVRO", expected.getSchemaType());
            Assert.assertEquals(CpiKafkaPlusErrorCode.KP_SR_002, CpiKafkaPlusErrorCode.fromThrowable(expected));
        }
    }

    @Test
    public void anUnreadableValueIsReportedWithItsOwnCode() {
        Assert.assertEquals(CpiKafkaPlusErrorCode.KP_SR_002,
                CpiKafkaPlusErrorCode.fromThrowable(new RegistryValueException(42, "JSON", "x")));
        Assert.assertEquals(CpiKafkaPlusErrorCode.KP_SR_001, CpiKafkaPlusErrorCode.fromThrowable(
                new RuntimeException(new SchemaRegistryHttpClient.SchemaRegistryException("GET /schemas/ids/7", 503, ""))));
    }

    @Test
    public void aRegistryOutageBlocksThePartitionInsteadOfReachingTheDlq() throws Exception {
        schemas.put(42, new MockResponse().setResponseCode(503).setBody("{\"error_code\":50301}"));
        MockProducer<byte[], byte[]> dlq = dlqProducer();
        ConsumerRecords<byte[], byte[]> records = records(record(0, framed(42, JSON)));
        PollProgress progress = new PollProgress(records);

        processor(endpoint(""), null, dlqHelper(dlq)).processSingleRecords(null, records, false, progress);

        Assert.assertTrue(delivered.isEmpty());
        Assert.assertTrue(dlq.history().isEmpty());
        Assert.assertTrue(progress.isBlocked(TP));
    }

    // --- Validating against the registry schema ---

    @Test
    public void aValidValuePassesTheRegistrySchema() throws Exception {
        registered(42, "JSON", REQUIRES_NAME);

        Message message = deliverOne(endpoint(VALIDATE), framed(42, "{\"name\":\"a\"}"));

        Assert.assertEquals("{\"name\":\"a\"}", message.getBody(String.class));
    }

    @Test
    public void anInvalidValueReachesTheDlqWithItsHeader() throws Exception {
        registered(42, "JSON", REQUIRES_NAME);

        byte[] value = framed(42, JSON);
        MockProducer<byte[], byte[]> dlq = assertDeadLettered(endpoint(VALIDATE), value);

        Assert.assertArrayEquals("the DLQ gets the value as it was consumed", value, dlq.history().get(0).value());
    }

    @Test
    public void withoutTheOptionAnInvalidValueIsDelivered() throws Exception {
        registered(42, "JSON", REQUIRES_NAME);

        Assert.assertEquals(JSON, deliverOne(endpoint(""), framed(42, JSON)).getBody(String.class));
    }

    @Test
    public void aValueThatFailsBothSchemasIsHandledOnce() throws Exception {
        registered(42, "JSON", REQUIRES_NAME);
        JsonSchemaValidator inline = new JsonSchemaValidator("{\"required\":[\"other\"]}");
        MockProducer<byte[], byte[]> dlq = dlqProducer();
        ConsumerRecords<byte[], byte[]> records = records(record(0, framed(42, JSON)));

        processor(endpoint(VALIDATE + "&jsonSchemaValidation=true"), inline, dlqHelper(dlq))
                .processSingleRecords(null, records, false, new PollProgress(records));

        Assert.assertTrue(delivered.isEmpty());
        Assert.assertEquals(1, dlq.history().size());
        String error = new String(dlq.history().get(0).headers().lastHeader("CpiKafkaPlusDlqError").value(),
                StandardCharsets.UTF_8);
        Assert.assertTrue(error, error.contains("other") && error.contains("Registry schema 42: "));
    }

    @Test
    public void aBatchFiltersAValueThatFailsTheRegistrySchema() throws Exception {
        registered(42, "JSON", REQUIRES_NAME);
        MockProducer<byte[], byte[]> dlq = dlqProducer();
        ConsumerRecords<byte[], byte[]> records = records(record(0, framed(42, JSON)),
                record(1, framed(42, "{\"name\":\"b\"}")));

        processor(endpoint(VALIDATE + "&batchMode=true"), null, dlqHelper(dlq))
                .processBatchRecords(null, records, false, new PollProgress(records));

        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals(1, delivered.size());
        Assert.assertEquals(1, delivered.get(0).getIn().getHeader("CpiKafkaPlusRecordCount"));
        Assert.assertEquals(1, delivered.get(0).getIn().getHeader("CpiKafkaPlusSchemaValidationFailures"));
    }

    @Test
    public void aSchemaWithReferencesCannotValidateButStillReads() throws Exception {
        schemas.put(42, schemaResponse("JSON", REQUIRES_NAME,
                "[{\"name\":\"other.json\",\"subject\":\"other\",\"version\":1}]"));

        assertDeadLettered(endpoint(VALIDATE), framed(42, "{\"name\":\"a\"}"));
        delivered.clear();
        Assert.assertEquals("{\"name\":\"a\"}",
                deliverOne(endpoint(""), framed(42, "{\"name\":\"a\"}")).getBody(String.class));
    }

    @Test
    public void theDraftFollowsSchema() {
        String prefixItems = "\"prefixItems\":[{\"type\":\"string\"}]";
        Assert.assertNotNull(JsonSchemaValidator.forRegistrySchema(
                "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\"," + prefixItems + "}").validate("[1]"));
        Assert.assertNull("draft-07 has no prefixItems",
                JsonSchemaValidator.forRegistrySchema("{" + prefixItems + "}").validate("[1]"));
        Assert.assertNotNull(JsonSchemaValidator.forRegistrySchema(
                "{\"$schema\":\"https://json-schema.org/draft/2019-09/schema\",\"required\":[\"a\"]}").validate("{}"));
        Assert.assertNotNull(JsonSchemaValidator.forRegistrySchema(
                "{\"$schema\":\"http://json-schema.org/draft-04/schema#\",\"required\":[\"a\"]}").validate("{}"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void anUnknownSchemaIsRejected() {
        JsonSchemaValidator.forRegistrySchema("{\"$schema\":\"https://example.com/my-dialect\"}");
    }

    @Test
    public void aRemoteRefIsRejectedWithoutBeingFetched() throws Exception {
        MockWebServer remote = new MockWebServer();
        remote.start();
        try {
            String schema = "{\"properties\":{\"a\":{\"$ref\":\"" + remote.url("/other.json") + "\"}}}";
            try {
                JsonSchemaValidator.forRegistrySchema(schema).validate("{\"a\":1}");
                Assert.fail("a remote $ref was accepted");
            } catch (IllegalArgumentException expected) {
                // compiled eagerly, so the $ref fails here
            }
            Assert.assertEquals(0, remote.getRequestCount());
        } finally {
            remote.shutdown();
        }
    }

    @Test
    public void theUiOffersTheOptionWhileTheSchemaRegistryIsOn() throws Exception {
        File sender = new File("src/main/resources/metadata/metadata-sender-1.6.0.xml");
        NodeList references = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(sender)
                .getElementsByTagName("AttributeReference");
        for (int i = 0; i < references.getLength(); i++) {
            Element reference = (Element) references.item(i);
            if ("registrySchemaValidation".equals(text(reference, "ReferenceName"))) {
                Element condition = (Element) reference.getElementsByTagName("EditCondition").item(0);
                Assert.assertEquals("schemaRegistryEnabled", text(condition, "AttributeName"));
                Assert.assertEquals("true", text(condition, "AttributeValue"));
                return;
            }
        }
        Assert.fail("the sender 1.6 does not offer registrySchemaValidation");
    }

    // --- Helpers ---

    private void registered(int id, String type, String schema) {
        schemas.put(id, schemaResponse(type, schema, null));
    }

    private static MockResponse schemaResponse(String type, String schema, String referencesJson) {
        try {
            ObjectNode body = MAPPER.createObjectNode().put("schema", schema);
            if (type != null) {
                body.put("schemaType", type);
            }
            if (referencesJson != null) {
                body.set("references", MAPPER.readTree(referencesJson));
            }
            return new MockResponse().setHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                    .setBody(MAPPER.writeValueAsString(body));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] framed(int schemaId, String json) {
        return framed(schemaId, json.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] framed(int schemaId, byte[] payload) {
        return ByteBuffer.allocate(5 + payload.length).put((byte) 0).putInt(schemaId).put(payload).array();
    }

    private static byte[] avro(int schemaId, String id) throws Exception {
        Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", id);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
        encoder.flush();
        return framed(schemaId, out.toByteArray());
    }

    private CpiKafkaPlusEndpoint endpoint(String options) throws Exception {
        DefaultCamelContext ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
        contexts.add(ctx);
        return (CpiKafkaPlusEndpoint) ctx.getEndpoint("cpi-kafka-plus:orders?bootstrapServers=localhost:9092"
                + "&groupId=g&commitStrategy=BATCH_COMPLETE&schemaRegistryEnabled=true&schemaRegistryUrl="
                + registry.url("/") + options);
    }

    private Message deliverOne(CpiKafkaPlusEndpoint endpoint, byte[] value) throws Exception {
        ConsumerRecords<byte[], byte[]> records = records(record(0, value));
        processor(endpoint, null, null).processSingleRecords(null, records, false, new PollProgress(records));
        Assert.assertEquals(1, delivered.size());
        return delivered.get(0).getIn();
    }

    private MockProducer<byte[], byte[]> assertDeadLettered(CpiKafkaPlusEndpoint endpoint, byte[] value)
            throws Exception {
        MockProducer<byte[], byte[]> dlq = dlqProducer();
        ConsumerRecords<byte[], byte[]> records = records(record(0, value));
        PollProgress progress = new PollProgress(records);

        processor(endpoint, null, dlqHelper(dlq)).processSingleRecords(null, records, false, progress);

        Assert.assertTrue("the record never reaches the integration flow", delivered.isEmpty());
        Assert.assertEquals(1, dlq.history().size());
        Assert.assertFalse(progress.isBlocked(TP));
        return dlq;
    }

    private static MockProducer<byte[], byte[]> dlqProducer() {
        return new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static DlqProducerHelper dlqHelper(MockProducer<byte[], byte[]> dlq) {
        return new DlqProducerHelper("orders-dlq", dlq, ProducerSendGuard.of(500L, "PLAINTEXT"));
    }

    private static ConsumerRecord<byte[], byte[]> record(long offset, byte[] value) {
        return new ConsumerRecord<>("orders", 0, offset, null, value);
    }

    @SafeVarargs
    private static ConsumerRecords<byte[], byte[]> records(ConsumerRecord<byte[], byte[]>... records) {
        return new ConsumerRecords<>(Collections.singletonMap(TP, Arrays.asList(records)));
    }

    private RecordProcessor processor(CpiKafkaPlusEndpoint endpoint, JsonSchemaValidator validator,
                                      DlqProducerHelper dlq) {
        return new RecordProcessor(endpoint, new AdapterTracingHelper(endpoint), validator, dlq,
                new AvroDeserializerHelper(endpoint),
                new RecordProcessor.ConsumerCallback() {
                    @Override
                    public void processExchange(Exchange exchange) {
                        delivered.add(exchange);
                    }

                    @Override
                    public void handleException(String message, Exchange exchange, Exception e) {
                    }

                    @Override
                    public Exchange createExchange() {
                        return endpoint.createExchange();
                    }
                }, new OffsetCommitTracker());
    }

    private static String text(Element parent, String tag) {
        NodeList nodes = parent.getElementsByTagName(tag);
        return nodes.getLength() > 0 ? nodes.item(0).getTextContent().trim() : null;
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static String capture(ThrowingRunnable action) throws Exception {
        java.io.PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(captured, true, "UTF-8"));
        try {
            action.run();
        } finally {
            System.setErr(original);
        }
        return new String(captured.toByteArray(), StandardCharsets.UTF_8);
    }
}
