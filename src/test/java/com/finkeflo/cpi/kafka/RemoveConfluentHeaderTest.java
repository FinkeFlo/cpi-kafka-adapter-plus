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
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;

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
import org.junit.Test;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * The sender option <i>Remove Confluent Header</i> (#284): a value behind the 5-byte header that
 * Confluent's JSON Schema serializer writes (magic byte {@code 0x00}, then the schema ID big-endian)
 * reaches the integration flow without it. All values are invented.
 */
public class RemoveConfluentHeaderTest {

    private static final String JSON = "{\"id\":\"a\"}";
    private static final String REMOVE = "&removeConfluentHeader=true";

    private final List<DefaultCamelContext> contexts = new ArrayList<>();
    private final List<Exchange> delivered = new ArrayList<>();

    @After
    public void closeContexts() {
        for (DefaultCamelContext ctx : contexts) {
            try {
                ctx.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
        contexts.clear();
    }

    // --- One message per record ---

    @Test
    public void theJsonAfterTheHeaderIsTheBody() throws Exception {
        Message message = deliverOne(endpoint(REMOVE), null, framed(42, JSON));

        Assert.assertEquals(JSON, message.getBody(String.class));
        Assert.assertEquals(42, message.getHeader("CpiKafkaPlusSchemaId"));
        Assert.assertEquals(JSON.length(), message.getHeader("CpiKafkaPlusPayloadSize"));
    }

    @Test
    public void theHeaderIsRemovedByPositionNotAtTheFirstBrace() throws Exception {
        // The last byte of schema ID 123 is 0x7B, a '{'.
        Message message = deliverOne(endpoint(REMOVE), null, framed(123, JSON));

        Assert.assertEquals(JSON, message.getBody(String.class));
        Assert.assertEquals(123, message.getHeader("CpiKafkaPlusSchemaId"));
    }

    @Test
    public void aSchemaIdWhoseBytesAreOneUtf8CharacterIsRemovedAsWell() throws Exception {
        // The bytes C2 80 of schema ID 49,792 decode to one character, so the header is only four
        // characters of text: cutting five characters off the body would eat into the JSON.
        Message message = deliverOne(endpoint(REMOVE), null, framed(0xC280, JSON));

        Assert.assertEquals(JSON, message.getBody(String.class));
        Assert.assertEquals(0xC280, message.getHeader("CpiKafkaPlusSchemaId"));
    }

    @Test
    public void aValueWithoutTheHeaderIsPassedOnUnchanged() throws Exception {
        Message message = deliverOne(endpoint(REMOVE), null, JSON.getBytes(StandardCharsets.UTF_8));

        Assert.assertEquals(JSON, message.getBody(String.class));
        Assert.assertNull(message.getHeader("CpiKafkaPlusSchemaId"));
    }

    @Test
    public void aValueShorterThanTheHeaderIsPassedOnUnchanged() throws Exception {
        Message message = deliverOne(endpoint(REMOVE), null, new byte[] {0, 0, 0, 42});

        Assert.assertEquals("\u0000\u0000\u0000*", message.getBody(String.class));
        Assert.assertNull(message.getHeader("CpiKafkaPlusSchemaId"));
    }

    @Test
    public void aValueOfOnlyTheHeaderIsPassedOnUnchanged() throws Exception {
        // Nothing follows the header, so there is no JSON value to hand on: keep the header.
        Message message = deliverOne(endpoint(REMOVE), null, framed(42, new byte[0]));

        Assert.assertEquals("\u0000\u0000\u0000\u0000*", message.getBody(String.class));
        Assert.assertNull(message.getHeader("CpiKafkaPlusSchemaId"));
    }

    @Test
    public void aJsonScalarAfterTheHeaderIsTheBody() throws Exception {
        Message message = deliverOne(endpoint(REMOVE), null, framed(42, "\"a\""));

        Assert.assertEquals("\"a\"", message.getBody(String.class));
        Assert.assertEquals(42, message.getHeader("CpiKafkaPlusSchemaId"));
    }

    @Test
    public void avroBehindTheHeaderIsPassedOnUnchanged() throws Exception {
        // Confluent's Avro serializer with the Schema Registry off: a record {name: "a", qty: 8}
        // in Avro binary, the string as zigzag length 1 and its byte, then the int 8 as zigzag 16.
        byte[] value = framed(42, new byte[] {0x02, 'a', 0x10});

        Message message = deliverOne(endpoint(REMOVE), null, value);

        Assert.assertEquals(new String(value, StandardCharsets.UTF_8), message.getBody(String.class));
        Assert.assertNull(message.getHeader("CpiKafkaPlusSchemaId"));
    }

    @Test
    public void protobufBehindTheHeaderIsPassedOnUnchanged() throws Exception {
        // Confluent's Protobuf serializer writes the message indexes after the header (0x00 for the
        // first message type), then the message: field 1, a string of length 1.
        byte[] value = framed(42, new byte[] {0x00, 0x0A, 0x01, 'a'});

        Message message = deliverOne(endpoint(REMOVE), null, value);

        Assert.assertEquals(new String(value, StandardCharsets.UTF_8), message.getBody(String.class));
        Assert.assertNull(message.getHeader("CpiKafkaPlusSchemaId"));
    }

    @Test
    public void textAfterTheHeaderThatIsNotOneJsonValueIsPassedOnUnchanged() throws Exception {
        for (String text : new String[] {"{\"id\":", "{\"id\":\"a\"} {\"id\":\"b\"}", "id=a"}) {
            delivered.clear();
            byte[] value = framed(42, text);

            Message message = deliverOne(endpoint(REMOVE), null, value);

            Assert.assertEquals(text, new String(value, StandardCharsets.UTF_8), message.getBody(String.class));
            Assert.assertNull(text, message.getHeader("CpiKafkaPlusSchemaId"));
        }
    }

    @Test
    public void aLongJsonStringAfterTheHeaderIsStillRemoved() throws Exception {
        // Longer than Jackson's default limit of 20,000,000 characters for one string.
        char[] chars = new char[20_000_001];
        Arrays.fill(chars, 'x');
        String json = "{\"id\":\"" + new String(chars) + "\"}";

        Message message = deliverOne(endpoint(REMOVE), null, framed(42, json));

        Assert.assertEquals(json.length(), message.getBody(String.class).length());
        Assert.assertEquals(42, message.getHeader("CpiKafkaPlusSchemaId"));
    }

    @Test
    public void aTombstoneStaysATombstone() throws Exception {
        Message message = deliverOne(endpoint(REMOVE), null, null);

        Assert.assertNull(message.getBody());
        Assert.assertNull(message.getHeader("CpiKafkaPlusSchemaId"));
    }

    @Test
    public void theKeyIsNotChanged() throws Exception {
        byte[] key = framed(7, "k");

        Message message = deliverOne(endpoint(REMOVE), key, framed(42, JSON));

        Assert.assertEquals(new String(key, StandardCharsets.UTF_8), message.getHeader("CpiKafkaPlusKey"));
    }

    @Test
    public void theHeaderStaysWhileTheOptionIsOff() throws Exception {
        byte[] value = framed(42, JSON);

        Message message = deliverOne(endpoint(""), null, value);

        Assert.assertEquals(new String(value, StandardCharsets.UTF_8), message.getBody(String.class));
        Assert.assertNull(message.getHeader("CpiKafkaPlusSchemaId"));
    }

    @Test
    public void theOptionIsIgnoredWhileTheSchemaRegistryIsOn() throws Exception {
        // The UI offers the option only while the registry is off, but a value saved before stays
        // when the registry is switched on and must have no effect then. Without Avro the value is
        // read as text.
        byte[] value = framed(42, JSON);
        CpiKafkaPlusEndpoint endpoint = endpoint(REMOVE + "&schemaRegistryEnabled=true"
                + "&avroValueDeserialization=false");

        Message message = deliverOne(endpoint, null, value);

        Assert.assertEquals(new String(value, StandardCharsets.UTF_8), message.getBody(String.class));
        Assert.assertNull(message.getHeader("CpiKafkaPlusSchemaId"));
    }

    // --- Batches ---

    @Test
    public void aJsonArrayBatchEmbedsTheJsonAsAnObject() throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint(REMOVE + "&batchOutputFormat=JSON_ARRAY");
        ConsumerRecords<byte[], byte[]> records = records(
                record(0, null, framed(42, JSON)),
                record(1, null, "{\"id\":\"b\"}".getBytes(StandardCharsets.UTF_8)));

        processor(endpoint, null, null).processBatchRecords(null, records, false, new PollProgress(records));

        Assert.assertEquals(1, delivered.size());
        Message message = delivered.get(0).getIn();
        String body = message.getBody(String.class);
        Assert.assertTrue(body, body.contains("\"value\":{\"id\":\"a\"}"));
        Assert.assertTrue(body, body.contains("\"value\":{\"id\":\"b\"}"));
        Assert.assertNull("a batch has no single schema ID", message.getHeader("CpiKafkaPlusSchemaId"));
    }

    @Test
    public void anXmlListBatchWritesTheJsonAsText() throws Exception {
        // Without the option the header's control characters make the value Base64 (#285).
        CpiKafkaPlusEndpoint endpoint = endpoint(REMOVE + "&batchOutputFormat=XML_LIST");
        ConsumerRecords<byte[], byte[]> records = records(record(0, null, framed(42, JSON)));

        processor(endpoint, null, null).processBatchRecords(null, records, false, new PollProgress(records));

        Assert.assertEquals(1, delivered.size());
        String body = delivered.get(0).getIn().getBody(String.class);
        Element value = (Element) DocumentBuilderFactory.newInstance().newDocumentBuilder()
                .parse(new InputSource(new StringReader(body))).getElementsByTagName("value").item(0);
        Assert.assertEquals(body, "text", value.getAttribute("format"));
        Assert.assertEquals(JSON, value.getTextContent());
    }

    // --- JSON Schema Validation and the DLQ ---

    @Test
    public void jsonSchemaValidationChecksTheJsonAfterTheHeader() throws Exception {
        JsonSchemaValidator validator = new JsonSchemaValidator(
                "{\"type\":\"object\",\"required\":[\"id\"]}");
        ConsumerRecords<byte[], byte[]> records = records(record(0, null, framed(42, JSON)));

        processor(endpoint(REMOVE + "&jsonSchemaValidation=true"), validator, null)
                .processSingleRecords(null, records, false, new PollProgress(records));

        Assert.assertEquals(1, delivered.size());
        Assert.assertEquals(JSON, delivered.get(0).getIn().getBody(String.class));
    }

    @Test
    public void anInvalidRecordReachesTheDlqWithItsHeader() throws Exception {
        JsonSchemaValidator validator = new JsonSchemaValidator(
                "{\"type\":\"object\",\"required\":[\"name\"]}");
        MockProducer<byte[], byte[]> dlq =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        byte[] value = framed(42, JSON);
        ConsumerRecords<byte[], byte[]> records = records(record(0, null, value));

        processor(endpoint(REMOVE + "&jsonSchemaValidation=true"), validator,
                new DlqProducerHelper("orders-dlq", dlq, ProducerSendGuard.of(500L, "PLAINTEXT")))
                .processSingleRecords(null, records, false, new PollProgress(records));

        Assert.assertTrue("an invalid record never reaches the integration flow", delivered.isEmpty());
        Assert.assertEquals(1, dlq.history().size());
        Assert.assertArrayEquals("the DLQ gets the value as it was consumed", value, dlq.history().get(0).value());
    }

    // --- The UI ---

    @Test
    public void theUiOffersTheOptionOnlyWhileTheSchemaRegistryIsOff() throws Exception {
        File sender = new File("src/main/resources/metadata/metadata-sender-1.5.0.xml");
        NodeList references = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(sender)
                .getElementsByTagName("AttributeReference");
        for (int i = 0; i < references.getLength(); i++) {
            Element reference = (Element) references.item(i);
            if ("removeConfluentHeader".equals(text(reference, "ReferenceName"))) {
                Element condition = (Element) reference.getElementsByTagName("EditCondition").item(0);
                Assert.assertNotNull("removeConfluentHeader has no EditCondition", condition);
                Assert.assertEquals("schemaRegistryEnabled", text(condition, "AttributeName"));
                Assert.assertEquals("false", text(condition, "AttributeValue"));
                return;
            }
        }
        Assert.fail("the sender 1.5 does not offer removeConfluentHeader");
    }

    // --- Helpers ---

    /** {@code json} behind the Confluent header of {@code schemaId}. */
    private static byte[] framed(int schemaId, String json) {
        return framed(schemaId, json.getBytes(StandardCharsets.UTF_8));
    }

    /** {@code payload} behind the Confluent header of {@code schemaId}. */
    private static byte[] framed(int schemaId, byte[] payload) {
        return ByteBuffer.allocate(5 + payload.length).put((byte) 0).putInt(schemaId).put(payload).array();
    }

    private CpiKafkaPlusEndpoint endpoint(String options) throws Exception {
        DefaultCamelContext ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
        contexts.add(ctx);
        return (CpiKafkaPlusEndpoint) ctx.getEndpoint("cpi-kafka-plus:orders?bootstrapServers=localhost:9092"
                + "&groupId=g&commitStrategy=BATCH_COMPLETE" + options);
    }

    /** The message the integration flow gets for one record, delivered on its own. */
    private Message deliverOne(CpiKafkaPlusEndpoint endpoint, byte[] key, byte[] value) throws Exception {
        ConsumerRecords<byte[], byte[]> records = records(record(0, key, value));
        processor(endpoint, null, null).processSingleRecords(null, records, false, new PollProgress(records));
        Assert.assertEquals(1, delivered.size());
        return delivered.get(0).getIn();
    }

    private static ConsumerRecord<byte[], byte[]> record(long offset, byte[] key, byte[] value) {
        return new ConsumerRecord<>("orders", 0, offset, key, value);
    }

    @SafeVarargs
    private static ConsumerRecords<byte[], byte[]> records(ConsumerRecord<byte[], byte[]>... records) {
        return new ConsumerRecords<>(Collections.singletonMap(new TopicPartition("orders", 0),
                Arrays.asList(records)));
    }

    private RecordProcessor processor(CpiKafkaPlusEndpoint endpoint, JsonSchemaValidator validator,
                                      DlqProducerHelper dlq) {
        return new RecordProcessor(endpoint, new AdapterTracingHelper(endpoint), validator, dlq, null,
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
}
