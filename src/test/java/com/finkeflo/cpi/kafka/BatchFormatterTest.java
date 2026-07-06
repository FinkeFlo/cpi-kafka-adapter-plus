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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for {@link BatchFormatter} — no Docker required.
 */
public class BatchFormatterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final BiFunction<String, byte[], String> STRING_DESER =
            (topic, data) -> data != null ? new String(data, StandardCharsets.UTF_8) : null;

    @Test
    public void testToJsonArrayMultipleRecords() throws Exception {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k1", "{\"name\":\"Alice\"}"),
                rec("t", 0, 1, "k2", "{\"name\":\"Bob\"}")
        );

        String json = BatchFormatter.toJsonArray(records, STRING_DESER, STRING_DESER);
        JsonNode root = MAPPER.readTree(json);
        JsonNode array = root.get("kafkaRecords").get("record");

        Assert.assertTrue("Root must be an object", root.isObject());
        Assert.assertTrue("kafkaRecords must be an object", root.get("kafkaRecords").isObject());
        Assert.assertTrue("record must be an array", array.isArray());
        Assert.assertEquals(2, array.size());
        Assert.assertEquals("Alice", array.get(0).get("value").get("name").asText());
        Assert.assertEquals("Bob", array.get(1).get("value").get("name").asText());
        Assert.assertEquals("k1", array.get(0).get("key").asText());
        Assert.assertEquals("k2", array.get(1).get("key").asText());
    }

    @Test
    public void testToJsonArrayNullKeys() throws Exception {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, null, "value1")
        );

        String json = BatchFormatter.toJsonArray(records, STRING_DESER, STRING_DESER);
        JsonNode array = MAPPER.readTree(json).get("kafkaRecords").get("record");

        Assert.assertTrue("Key should be null", array.get(0).get("key").isNull());
    }

    @Test
    public void testToJsonArrayPlainTextValues() throws Exception {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k", "hello world")
        );

        String json = BatchFormatter.toJsonArray(records, STRING_DESER, STRING_DESER);
        JsonNode array = MAPPER.readTree(json).get("kafkaRecords").get("record");

        Assert.assertEquals("hello world", array.get(0).get("value").asText());
    }

    @Test
    public void testToJsonArraySingleRecord() throws Exception {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 42, "k", "{\"x\":1}")
        );

        String json = BatchFormatter.toJsonArray(records, STRING_DESER, STRING_DESER);
        JsonNode array = MAPPER.readTree(json).get("kafkaRecords").get("record");

        Assert.assertTrue(array.isArray());
        Assert.assertEquals(1, array.size());
    }

    @Test
    public void testToJsonArrayEmptyList() throws Exception {
        List<ConsumerRecord<byte[], byte[]>> records = Collections.emptyList();

        String json = BatchFormatter.toJsonArray(records, STRING_DESER, STRING_DESER);

        Assert.assertEquals("{\"kafkaRecords\":{\"record\":[]}}", json);
    }

    @Test
    public void testToJsonArrayMetadataFields() throws Exception {
        ConsumerRecord<byte[], byte[]> record = rec("my-topic", 3, 99, "key", "val");
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(record);

        String json = BatchFormatter.toJsonArray(records, STRING_DESER, STRING_DESER);
        JsonNode node = MAPPER.readTree(json).get("kafkaRecords").get("record").get(0);

        Assert.assertEquals("my-topic", node.get("topic").asText());
        Assert.assertEquals(3, node.get("partition").asInt());
        Assert.assertEquals(99, node.get("offset").asLong());
        Assert.assertTrue("timestamp should be present", node.has("timestamp"));
    }

    @Test
    public void testToXmlMultipleRecords() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k1", "v1"),
                rec("t", 0, 1, "k2", "v2")
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER);

        Assert.assertTrue("Should start with XML declaration",
                xml.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        Assert.assertTrue("Should contain count attribute",
                xml.contains("<kafkaRecords count=\"2\">"));
        Assert.assertTrue("Should contain record elements", xml.contains("<record>"));
        Assert.assertTrue("Should contain value v1", xml.contains("<value format=\"text\"><![CDATA[v1]]></value>"));
        Assert.assertTrue("Should contain value v2", xml.contains("<value format=\"text\"><![CDATA[v2]]></value>"));
        Assert.assertTrue("Should end with closing tag", xml.endsWith("</kafkaRecords>"));
    }

    @Test
    public void testToXmlSpecialCharactersInValue() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k", "<tag>&\"quote\"'apos'</tag>")
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER);

        // Value uses CDATA — special chars preserved as-is
        Assert.assertTrue("Value should use CDATA with raw content",
                xml.contains("<value format=\"text\"><![CDATA[<tag>&\"quote\"'apos'</tag>]]></value>"));
        // Key still uses escapeXml
        Assert.assertTrue("Key should use escapeXml", xml.contains("<key>k</key>"));
    }

    @Test
    public void testToXmlEmptyList() {
        List<ConsumerRecord<byte[], byte[]>> records = Collections.emptyList();

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER);

        Assert.assertTrue(xml.contains("<kafkaRecords count=\"0\">"));
        Assert.assertTrue(xml.endsWith("</kafkaRecords>"));
        Assert.assertFalse("Should not contain record elements", xml.contains("<record>"));
    }

    @Test
    public void testEscapeXmlNull() {
        Assert.assertEquals("", BatchFormatter.escapeXml(null));
    }

    @Test
    public void testEscapeXmlEmpty() {
        Assert.assertEquals("", BatchFormatter.escapeXml(""));
    }

    @Test
    public void testEscapeXmlAllSpecialChars() {
        String input = "&<>\"'";
        String expected = "&amp;&lt;&gt;&quot;&apos;";
        Assert.assertEquals(expected, BatchFormatter.escapeXml(input));
    }

    @Test
    public void testEscapeXmlPlainText() {
        Assert.assertEquals("hello world", BatchFormatter.escapeXml("hello world"));
    }

    // --- CDATA wrapping tests ---

    @Test
    public void testAppendCdataPlainText() {
        StringBuilder sb = new StringBuilder();
        BatchFormatter.appendCdata(sb, "hello world");
        Assert.assertEquals("<![CDATA[hello world]]>", sb.toString());
    }

    @Test
    public void testAppendCdataWithCdataEndMarker() {
        StringBuilder sb = new StringBuilder();
        BatchFormatter.appendCdata(sb, "data]]>more");
        Assert.assertEquals("<![CDATA[data]]]]><![CDATA[>more]]>", sb.toString());
    }

    @Test
    public void testAppendCdataXmlContent() {
        StringBuilder sb = new StringBuilder();
        BatchFormatter.appendCdata(sb, "<root><child/></root>");
        Assert.assertEquals("<![CDATA[<root><child/></root>]]>", sb.toString());
    }

    @Test
    public void testAppendCdataNull() {
        StringBuilder sb = new StringBuilder();
        BatchFormatter.appendCdata(sb, null);
        Assert.assertEquals("", sb.toString());
    }

    @Test
    public void testAppendCdataEmpty() {
        StringBuilder sb = new StringBuilder();
        BatchFormatter.appendCdata(sb, "");
        Assert.assertEquals("", sb.toString());
    }

    // --- XML content start detection tests ---

    @Test
    public void testFindXmlContentStartNoProlog() {
        Assert.assertEquals(0, BatchFormatter.findXmlContentStart("<root/>"));
    }

    @Test
    public void testFindXmlContentStartWithProlog() {
        String value = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><root/>";
        int start = BatchFormatter.findXmlContentStart(value);
        Assert.assertEquals('<', value.charAt(start));
        Assert.assertEquals("<root/>", value.substring(start));
    }

    @Test
    public void testFindXmlContentStartWithPrologAndWhitespace() {
        String value = "  <?xml version=\"1.0\"?>  <root/>";
        int start = BatchFormatter.findXmlContentStart(value);
        Assert.assertEquals("<root/>", value.substring(start));
    }

    @Test
    public void testFindXmlContentStartPlainText() {
        Assert.assertEquals(0, BatchFormatter.findXmlContentStart("hello world"));
    }

    @Test
    public void testFindXmlContentStartLeadingWhitespace() {
        String value = "  <root/>";
        int start = BatchFormatter.findXmlContentStart(value);
        Assert.assertEquals(2, start);
    }

    // --- toXml with embedXml tests ---

    @Test
    public void testToXmlDefaultUsesCdata() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k", "<Bestellung><Kopf/></Bestellung>")
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER, false);

        Assert.assertTrue("Value should use CDATA",
                xml.contains("<value format=\"text\"><![CDATA[<Bestellung><Kopf/></Bestellung>]]></value>"));
        Assert.assertTrue("Key should use escapeXml", xml.contains("<key>k</key>"));
    }

    @Test
    public void testToXmlEmbedValidXml() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k", "<Bestellung><Kopf>test</Kopf></Bestellung>")
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER, true);

        Assert.assertTrue("XML value should be embedded directly",
                xml.contains("<value format=\"xml\"><Bestellung><Kopf>test</Kopf></Bestellung></value>"));
    }

    @Test
    public void testToXmlFormatAttributeXmlForEmbeddedValue() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k", "<Bestellung><Kopf>test</Kopf></Bestellung>")
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER, true);

        Assert.assertTrue("Embedded XML value must carry format=\"xml\" attribute",
                xml.contains("<value format=\"xml\"><Bestellung><Kopf>test</Kopf></Bestellung></value>"));
    }

    @Test
    public void testToXmlFormatAttributeTextForNullValue() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k", null)
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER, true);

        Assert.assertTrue("Null value must emit format=\"text\" element",
                xml.contains("<value format=\"text\"></value>"));
    }

    @Test
    public void testToXmlFormatAttributeTextForEmptyValue() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k", "")
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER, true);

        Assert.assertTrue("Empty-string value must emit format=\"text\" element",
                xml.contains("<value format=\"text\"></value>"));
    }

    @Test
    public void testToXmlFormatAttributeMixedBatch() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k1", "<Bestellung><Nr>1</Nr></Bestellung>"),
                rec("t", 0, 1, "k2", "{\"orderId\":\"ABC\"}"),
                rec("t", 0, 2, "k3", "plain text"),
                rec("t", 0, 3, "k4", null)
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER, true);

        Assert.assertTrue("XML record gets format=\"xml\"",
                xml.contains("<value format=\"xml\"><Bestellung><Nr>1</Nr></Bestellung></value>"));
        Assert.assertTrue("JSON record gets format=\"text\" with CDATA",
                xml.contains("<value format=\"text\"><![CDATA[{\"orderId\":\"ABC\"}]]></value>"));
        Assert.assertTrue("Plain text record gets format=\"text\" with CDATA",
                xml.contains("<value format=\"text\"><![CDATA[plain text]]></value>"));
        Assert.assertTrue("Null value record gets format=\"text\" empty element",
                xml.contains("<value format=\"text\"></value>"));
    }

    @Test
    public void testToXmlEmbedStripsProlog() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k", "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Root/>")
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER, true);

        Assert.assertTrue("Prolog should be stripped",
                xml.contains("<value format=\"xml\"><Root/></value>"));
        Assert.assertFalse("No <?xml in value",
                xml.contains("<value format=\"xml\"><?xml"));
    }

    @Test
    public void testToXmlEmbedFallbackCdataForPlainText() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k", "hello world")
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER, true);

        Assert.assertTrue("Non-XML should fall back to CDATA",
                xml.contains("<value format=\"text\"><![CDATA[hello world]]></value>"));
    }

    @Test
    public void testToXmlEmbedFallbackCdataForJson() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k", "{\"name\":\"Alice\"}")
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER, true);

        Assert.assertTrue("JSON should fall back to CDATA",
                xml.contains("<value format=\"text\"><![CDATA[{\"name\":\"Alice\"}]]></value>"));
    }

    @Test
    public void testToXmlEmbedNullValue() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k", null)
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER, true);

        Assert.assertTrue("Null value should be empty",
                xml.contains("<value format=\"text\"></value>"));
    }

    @Test
    public void testToXmlCdataWithSpecialEndMarker() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k", "data]]>more")
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER, false);

        Assert.assertTrue("CDATA end marker should be escaped",
                xml.contains("<value format=\"text\"><![CDATA[data]]]]><![CDATA[>more]]></value>"));
    }

    @Test
    public void testToXmlBackwardsCompatibleSignature() {
        List<ConsumerRecord<byte[], byte[]>> records = Arrays.asList(
                rec("t", 0, 0, "k", "value1")
        );

        String xml = BatchFormatter.toXml(records, STRING_DESER, STRING_DESER);

        Assert.assertTrue("Old signature should use CDATA",
                xml.contains("<value format=\"text\"><![CDATA[value1]]></value>"));
    }

    // --- Helpers ---

    private static ConsumerRecord<byte[], byte[]> rec(String topic, int partition, long offset,
                                                       String key, String value) {
        byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;
        byte[] valueBytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : null;
        return new ConsumerRecord<>(topic, partition, offset, keyBytes, valueBytes);
    }
}
