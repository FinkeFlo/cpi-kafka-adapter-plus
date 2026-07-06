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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class BatchParserTest {

    // -----------------------------------------------------------------------
    //  JSON: Happy path
    // -----------------------------------------------------------------------

    @Test
    public void testParseJsonBasic() {
        String json = "[{\"key\": \"k1\", \"value\": \"hello\"}, {\"key\": \"k2\", \"value\": \"world\"}]";
        List<BatchRecord> records = BatchParser.parseJson(json);
        Assert.assertEquals(2, records.size());
        Assert.assertEquals("k1", records.get(0).getKey());
        Assert.assertEquals("hello", records.get(0).getValue());
        Assert.assertEquals("k2", records.get(1).getKey());
        Assert.assertEquals("world", records.get(1).getValue());
    }

    @Test
    public void testParseJsonObjectValue() {
        String json = "[{\"key\": \"k1\", \"value\": {\"id\": 1, \"name\": \"Alice\"}}]";
        List<BatchRecord> records = BatchParser.parseJson(json);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("k1", records.get(0).getKey());
        Assert.assertEquals("{\"id\":1,\"name\":\"Alice\"}", records.get(0).getValue());
    }

    @Test
    public void testParseJsonNullKey() {
        String json = "[{\"key\": null, \"value\": \"msg\"}]";
        List<BatchRecord> records = BatchParser.parseJson(json);
        Assert.assertNull(records.get(0).getKey());
        Assert.assertEquals("msg", records.get(0).getValue());
    }

    @Test
    public void testParseJsonMissingKey() {
        String json = "[{\"value\": \"msg without key\"}]";
        List<BatchRecord> records = BatchParser.parseJson(json);
        Assert.assertNull(records.get(0).getKey());
        Assert.assertEquals("msg without key", records.get(0).getValue());
    }

    @Test
    public void testParseJsonNullValueTombstone() {
        String json = "[{\"key\": \"k1\", \"value\": null}]";
        List<BatchRecord> records = BatchParser.parseJson(json);
        Assert.assertEquals("k1", records.get(0).getKey());
        Assert.assertNull(records.get(0).getValue());
    }

    @Test
    public void testParseJsonIgnoresUnknownFields() {
        // Round-trip symmetry: consumer output has topic, partition, offset, timestamp
        String json = "[{\"key\": \"k1\", \"value\": \"v1\", \"topic\": \"t\", "
                + "\"partition\": 0, \"offset\": 42, \"timestamp\": 12345}]";
        List<BatchRecord> records = BatchParser.parseJson(json);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("k1", records.get(0).getKey());
        Assert.assertEquals("v1", records.get(0).getValue());
    }

    @Test
    public void testParseJsonNumericKey() {
        String json = "[{\"key\": 42, \"value\": \"msg\"}]";
        List<BatchRecord> records = BatchParser.parseJson(json);
        Assert.assertEquals("42", records.get(0).getKey());
    }

    @Test
    public void testParseJsonObjectRootWithKafkaRecords() {
        String json = "{\"kafkaRecords\":[{\"key\":\"k1\",\"value\":\"v1\"},{\"key\":\"k2\",\"value\":\"v2\"}]}";
        List<BatchRecord> records = BatchParser.parseJson(json);
        Assert.assertEquals(2, records.size());
        Assert.assertEquals("k1", records.get(0).getKey());
        Assert.assertEquals("v1", records.get(0).getValue());
        Assert.assertEquals("k2", records.get(1).getKey());
        Assert.assertEquals("v2", records.get(1).getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseJsonObjectRootEmptyKafkaRecords() {
        BatchParser.parseJson("{\"kafkaRecords\":[]}");
    }

    @Test
    public void testParseJsonNestedRecordForm() {
        // Current consumer output: {"kafkaRecords":{"record":[...]}}
        String json = "{\"kafkaRecords\":{\"record\":[{\"key\":\"k1\",\"value\":\"v1\"},{\"key\":\"k2\",\"value\":\"v2\"}]}}";
        List<BatchRecord> records = BatchParser.parseJson(json);
        Assert.assertEquals(2, records.size());
        Assert.assertEquals("k1", records.get(0).getKey());
        Assert.assertEquals("v1", records.get(0).getValue());
        Assert.assertEquals("k2", records.get(1).getKey());
        Assert.assertEquals("v2", records.get(1).getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseJsonNestedRecordFormEmpty() {
        BatchParser.parseJson("{\"kafkaRecords\":{\"record\":[]}}");
    }

    // -----------------------------------------------------------------------
    //  JSON: Error cases
    // -----------------------------------------------------------------------

    @Test(expected = IllegalArgumentException.class)
    public void testParseJsonEmptyBody() {
        BatchParser.parseJson("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseJsonNullBody() {
        BatchParser.parseJson(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseJsonNotArray() {
        BatchParser.parseJson("{\"key\": \"k1\", \"value\": \"v1\"}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseJsonEmptyArray() {
        BatchParser.parseJson("[]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseJsonMissingValue() {
        BatchParser.parseJson("[{\"key\": \"k1\"}]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseJsonElementNotObject() {
        BatchParser.parseJson("[\"just a string\"]");
    }

    @Test
    public void testParseJsonErrorMessageContainsIndex() {
        try {
            BatchParser.parseJson("[{\"key\": \"k1\", \"value\": \"v1\"}, {\"key\": \"k2\"}]");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("index 1"));
        }
    }

    // -----------------------------------------------------------------------
    //  XML: Happy path
    // -----------------------------------------------------------------------

    @Test
    public void testParseXmlBasic() {
        String xml = "<kafkaRecords><record><key>k1</key><value>hello</value></record>"
                + "<record><key>k2</key><value>world</value></record></kafkaRecords>";
        List<BatchRecord> records = BatchParser.parseXml(xml);
        Assert.assertEquals(2, records.size());
        Assert.assertEquals("k1", records.get(0).getKey());
        Assert.assertEquals("hello", records.get(0).getValue());
        Assert.assertEquals("k2", records.get(1).getKey());
        Assert.assertEquals("world", records.get(1).getValue());
    }

    @Test
    public void testParseXmlCdata() {
        String xml = "<kafkaRecords><record><key>k1</key>"
                + "<value><![CDATA[<order><id>1</id></order>]]></value></record></kafkaRecords>";
        List<BatchRecord> records = BatchParser.parseXml(xml);
        Assert.assertEquals("<order><id>1</id></order>", records.get(0).getValue());
    }

    @Test
    public void testParseXmlMissingKey() {
        String xml = "<kafkaRecords><record><value>msg</value></record></kafkaRecords>";
        List<BatchRecord> records = BatchParser.parseXml(xml);
        Assert.assertNull(records.get(0).getKey());
        Assert.assertEquals("msg", records.get(0).getValue());
    }

    @Test
    public void testParseXmlEmptyKey() {
        // Empty key = explicit empty string, NOT null
        String xml = "<kafkaRecords><record><key></key><value>msg</value></record></kafkaRecords>";
        List<BatchRecord> records = BatchParser.parseXml(xml);
        Assert.assertEquals("", records.get(0).getKey());
    }

    @Test
    public void testParseXmlEmptyValueTombstone() {
        String xml = "<kafkaRecords><record><key>k1</key><value/></record></kafkaRecords>";
        List<BatchRecord> records = BatchParser.parseXml(xml);
        Assert.assertNull(records.get(0).getValue());
    }

    @Test
    public void testParseXmlIgnoresUnknownElements() {
        String xml = "<kafkaRecords><record><key>k1</key><value>v1</value>"
                + "<topic>t</topic><partition>0</partition><offset>42</offset>"
                + "<timestamp>12345</timestamp></record></kafkaRecords>";
        List<BatchRecord> records = BatchParser.parseXml(xml);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("k1", records.get(0).getKey());
        Assert.assertEquals("v1", records.get(0).getValue());
    }

    @Test
    public void testParseXmlEscapedContent() {
        String xml = "<kafkaRecords><record><key>k1</key>"
                + "<value>{&quot;name&quot;: &quot;O&apos;Brien &amp; Co&quot;}</value>"
                + "</record></kafkaRecords>";
        List<BatchRecord> records = BatchParser.parseXml(xml);
        Assert.assertEquals("{\"name\": \"O'Brien & Co\"}", records.get(0).getValue());
    }

    @Test
    public void testParseXmlChildElements() {
        // value contains child elements instead of CDATA — should re-serialize
        String xml = "<kafkaRecords><record><key>k1</key>"
                + "<value><order><id>1</id></order></value></record></kafkaRecords>";
        List<BatchRecord> records = BatchParser.parseXml(xml);
        Assert.assertTrue(records.get(0).getValue().contains("<order>"));
        Assert.assertTrue(records.get(0).getValue().contains("<id>1</id>"));
    }

    // -----------------------------------------------------------------------
    //  XML: Error cases
    // -----------------------------------------------------------------------

    @Test(expected = IllegalArgumentException.class)
    public void testParseXmlEmptyBody() {
        BatchParser.parseXml("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseXmlNullBody() {
        BatchParser.parseXml(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseXmlWrongRoot() {
        BatchParser.parseXml("<orders><order><id>1</id></order></orders>");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseXmlNoRecords() {
        BatchParser.parseXml("<kafkaRecords></kafkaRecords>");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseXmlMissingValue() {
        BatchParser.parseXml("<kafkaRecords><record><key>k1</key></record></kafkaRecords>");
    }

    @Test
    public void testParseXmlErrorMessageContainsIndex() {
        try {
            BatchParser.parseXml("<kafkaRecords>"
                    + "<record><key>k1</key><value>v1</value></record>"
                    + "<record><key>k2</key></record></kafkaRecords>");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("index 1"));
        }
    }

    @Test
    public void testParseXmlWrongRootErrorMessage() {
        try {
            BatchParser.parseXml("<orders><order/></orders>");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("<orders>"));
            Assert.assertTrue(e.getMessage().contains("<kafkaRecords>"));
        }
    }
}
