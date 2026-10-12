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

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

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

    // -----------------------------------------------------------------------
    //  Payload fidelity (#181)
    // -----------------------------------------------------------------------

    @Test
    public void jsonRecordValuesKeepTheirDecimalText() {
        List<BatchRecord> records = BatchParser.parseJson(
                "[{\"key\":1.50,\"value\":{\"amount\":99.90,\"big\":12345678901234567890.12}}]");
        Assert.assertEquals("1.50", records.get(0).getKey());
        Assert.assertEquals("{\"amount\":99.90,\"big\":12345678901234567890.12}", records.get(0).getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void jsonBodyWithContentAfterTheArrayIsRejected() {
        BatchParser.parseJson("[{\"value\":\"a\"}] [{\"value\":\"b\"}]");
    }

    @Test
    public void aRecordInsideAValueIsPayloadNotAnotherRecord() {
        List<BatchRecord> records = BatchParser.parseXml(
                "<kafkaRecords><record><key>k1</key><value format=\"xml\">"
                + "<record><key>inner</key><value>x</value></record>"
                + "</value></record></kafkaRecords>");
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("k1", records.get(0).getKey());
        Assert.assertTrue(records.get(0).getValue(), records.get(0).getValue().contains("<key>inner</key>"));
    }

    @Test
    public void aKeyInsideTheValueDoesNotBecomeTheRecordKey() {
        List<BatchRecord> records = BatchParser.parseXml(
                "<kafkaRecords><record><value format=\"xml\"><Order><key>payload-key</key></Order></value>"
                + "<key>real</key></record></kafkaRecords>");
        Assert.assertEquals("real", records.get(0).getKey());
    }

    @Test
    public void aRecordWithoutItsOwnKeyHasNoKeyEvenIfThePayloadHasOne() {
        List<BatchRecord> records = BatchParser.parseXml(
                "<kafkaRecords><record><value format=\"xml\"><Order><key>payload-key</key></Order></value>"
                + "</record></kafkaRecords>");
        Assert.assertNull(records.get(0).getKey());
    }

    @Test
    public void recordsInsideAWrapperElementGetAnErrorNamingTheCause() {
        try {
            BatchParser.parseXml("<kafkaRecords><batch><record><value>x</value></record></batch></kafkaRecords>");
            Assert.fail("expected a wrapped record to be rejected");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("direct child of <kafkaRecords>"));
        }
    }

    // -----------------------------------------------------------------------
    //  XML: the content of <value> (#247)
    // -----------------------------------------------------------------------

    @Test
    public void textAroundTheElementsOfAValueIsKept() {
        Assert.assertEquals("Preis: <b>10</b> EUR", xmlValue("<value>Preis: <b>10</b> EUR</value>"));
        Assert.assertEquals("a &amp; b <i>c</i> <i>d</i>",
                xmlValue("<value>a &amp; b <i>c</i> <i>d</i></value>"));
    }

    @Test
    public void cdataCommentsAndProcessingInstructionsNextToTheElementsAreKept() {
        Assert.assertEquals("<![CDATA[<raw>]]><x/>", xmlValue("<value><![CDATA[<raw>]]><x/></value>"));
        Assert.assertEquals("<!-- note --><a/><?pi data?>",
                xmlValue("<value>\n  <!-- note -->\n  <a/>\n  <?pi data?>\n</value>"));
    }

    @Test
    public void indentationBetweenTheElementsOfAValueIsStillDropped() {
        Assert.assertEquals("<a/><b>x</b>", xmlValue("<value>\n  <a/>\n\t<b>x</b>\r\n</value>"));
    }

    /**
     * A value whose first element is {@code <html>} is XML like any other. Left to the transformer's
     * default, it was written with the HTML output method: {@code <br>} instead of {@code <br/>},
     * which no XML parser accepts, with a {@code META} element added to {@code <head>} and indented.
     */
    @Test
    public void aValueWhoseFirstElementIsHtmlIsStillWrittenAsXml() {
        Assert.assertEquals("<html><head><title>t</title></head><body><br/><p>x</p></body></html>",
                xmlValue("<value><html><head><title>t</title></head><body><br/><p>x</p></body></html></value>"));
        Assert.assertEquals("Text <HTML><br/></HTML>", xmlValue("<value>Text <HTML><br/></HTML></value>"));
    }

    @Test
    public void aValueKeepsTheNamespacesDeclaredOnKafkaRecords() {
        List<BatchRecord> records = BatchParser.parseXml(
                "<kafkaRecords xmlns:ns=\"urn:x\" xmlns=\"urn:d\"><record><value><ns:order>1</ns:order>"
                + "</value></record></kafkaRecords>");
        Assert.assertEquals("<ns:order xmlns:ns=\"urn:x\" xmlns=\"urn:d\">1</ns:order>",
                records.get(0).getValue());
    }

    @Test
    public void aValueKeepsTheDefaultNamespaceItsElementsAreIn() {
        List<BatchRecord> records = BatchParser.parseXml(
                "<kafkaRecords xmlns=\"urn:d\"><record><key>k1</key><value><order><id>1</id></order></value>"
                + "<headers><header name=\"h\">v</header></headers></record></kafkaRecords>");
        Assert.assertEquals("k1", records.get(0).getKey());
        Assert.assertEquals("v", records.get(0).getHeaders().get("h"));
        Assert.assertEquals("<order xmlns=\"urn:d\"><id>1</id></order>", records.get(0).getValue());
    }

    @Test
    public void theNearestDeclarationWinsAndOneOnTheElementItselfIsKept() {
        List<BatchRecord> records = BatchParser.parseXml(
                "<kafkaRecords xmlns:ns=\"urn:outer\" xmlns:p=\"urn:p\" xmlns=\"urn:d\">"
                + "<record xmlns:ns=\"urn:inner\" xmlns=\"\"><value xmlns:v=\"urn:v\">"
                + "<ns:a v:x=\"1\"/><b xmlns:p=\"urn:own\" p:y=\"2\"/></value></record></kafkaRecords>");
        Assert.assertEquals("<ns:a xmlns:ns=\"urn:inner\" xmlns:p=\"urn:p\" xmlns:v=\"urn:v\" v:x=\"1\"/>"
                + "<b xmlns:ns=\"urn:inner\" xmlns:p=\"urn:own\" xmlns:v=\"urn:v\" p:y=\"2\"/>",
                records.get(0).getValue());
    }

    @Test
    public void aPrefixThatIsDeclaredNowhereIsRejected() {
        try {
            BatchParser.parseXml("<kafkaRecords><record><value><ns:order/></value></record></kafkaRecords>");
            Assert.fail("expected an undeclared prefix to be rejected");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("Producer batch mode failed to parse XML"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("ns"));
        }
    }

    @Test
    public void aDoctypeIsStillRejected() {
        try {
            BatchParser.parseXml("<!DOCTYPE kafkaRecords [<!ENTITY x SYSTEM \"file:///etc/hostname\">]>"
                    + "<kafkaRecords><record><value>&x;</value></record></kafkaRecords>");
            Assert.fail("expected a DOCTYPE to be rejected");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("DOCTYPE"));
        }
    }

    /**
     * Batches without namespace declarations, and values that declare their namespaces themselves,
     * produce exactly the records they did before the parser became namespace-aware, compared
     * against a copy of the previous parser. Only values with element children go through the
     * serializer; the others are listed to show that the text path is untouched. An element called
     * {@code html} in no namespace is left out on purpose: it is now written as XML (see above).
     */
    @Test
    public void valuesWithoutOutsideNamespacesAreTheSameAsBefore() throws Exception {
        String[] values = {
            "<value>hello</value>",
            "<value>{&quot;name&quot;: &quot;O&apos;Brien &amp; Co&quot;}</value>",
            "<value><![CDATA[<order><id>1</id></order>]]></value>",
            "<value>a<![CDATA[<b>]]>c</value>",
            "<value/>",
            "<value></value>",
            "<value>  </value>",
            "<value>abc<!-- c -->def</value>",
            "<value format=\"xml\"><order><id>1</id><amount>99.90</amount></order></value>",
            "<value><a z=\"1\" b=\"&amp;&lt;&quot;&gt;\" m='x&apos;' n=\"x&#10;y&#9;\"/></value>",
            "<value><a>t&amp;<![CDATA[<cd>]]><!--c--><?pi d?>&gt;</a></value>",
            "<value><a/><b/><c>  </c><d></d></value>",
            "<value>\n  <order>\n    <id>1</id>\n  </order>\n</value>",
            "<value>\n  <a>1</a>\n  <b>2</b>\n</value>",
            "<value><a>\u00e4\u00f6\u00fc\u00df \ud83d\ude00 &#169; &#x20AC; \u0085 \u2028</a></value>",
            "<value><a xml:lang=\"de\" xml:space=\"preserve\"> x </a></value>",
            "<value><record><key>inner</key><value>x</value></record></value>",
            "<value><html xmlns=\"http://www.w3.org/1999/xhtml\" lang=\"de\"><body><br/></body></html></value>",
            "<value><x:o xmlns:x=\"urn:x\" xmlns=\"urn:d\"><x:i y=\"1\"/><j/></x:o></value>",
            "<value><a xmlns=\"urn:a\"><b xmlns=\"\"/></a></value>",
            "<value><p:a xmlns:p=\"urn:p\" p:x=\"1\" y=\"2\"><p:b/><c xmlns=\"urn:c\"><d/></c></p:a></value>",
        };
        for (String value : values) {
            String xml = "<kafkaRecords>\n  <record>\n    <key>k</key>\n    " + value
                    + "\n    <headers><header name=\"h\">v</header></headers>\n  </record>\n</kafkaRecords>";
            BatchRecord record = BatchParser.parseXml(xml).get(0);
            Assert.assertEquals(value, valueBeforeNamespaceAwareness(xml), record.getValue());
            Assert.assertEquals(value, "k", record.getKey());
            Assert.assertEquals(value, "v", record.getHeaders().get("h"));
        }
    }

    private static String xmlValue(String valueElement) {
        return BatchParser.parseXml("<kafkaRecords><record>" + valueElement + "</record></kafkaRecords>")
                .get(0).getValue();
    }

    /** The value of the first record as the parser extracted it before #247, verbatim. */
    private static String valueBeforeNamespaceAwareness(String xml) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        Document doc = factory.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
        Element valueEl = null;
        NodeList recordChildren = ((Element) doc.getDocumentElement().getElementsByTagName("record").item(0))
                .getChildNodes();
        for (int i = 0; i < recordChildren.getLength() && valueEl == null; i++) {
            if ("value".equals(recordChildren.item(i).getNodeName())) {
                valueEl = (Element) recordChildren.item(i);
            }
        }
        NodeList children = valueEl.getChildNodes();
        boolean hasElementChildren = false;
        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
                hasElementChildren = true;
                break;
            }
        }
        if (hasElementChildren) {
            StringBuilder sb = new StringBuilder();
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            for (int i = 0; i < children.getLength(); i++) {
                Node child = children.item(i);
                if (child.getNodeType() == Node.ELEMENT_NODE) {
                    StringWriter writer = new StringWriter();
                    transformer.transform(new DOMSource(child), new StreamResult(writer));
                    sb.append(writer.toString());
                }
            }
            return sb.toString();
        }
        String text = valueEl.getTextContent();
        if (text == null || text.isEmpty()) {
            return null;
        }
        return text;
    }
}
