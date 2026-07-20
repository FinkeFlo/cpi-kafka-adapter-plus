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
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Parses JSON array or XML kafkaRecords into a list of {@link BatchRecord}.
 * Counterpart to {@link BatchFormatter} (which formats consumer output).
 */
public final class BatchParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BatchParser() {}

    /**
     * Parse a JSON array of record objects into BatchRecords.
     * Each element must have at least a "value" field. "key" is optional.
     * Unknown fields (topic, partition, offset, timestamp) are ignored.
     */
    public static List<BatchRecord> parseJson(String body) {
        if (body == null || body.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Producer batch mode is enabled but message body is empty");
        }

        JsonNode root;
        try {
            root = MAPPER.readTree(body);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Producer batch mode failed to parse JSON: " + e.getMessage(), e);
        }

        ArrayNode array = extractRecordArray(root);

        if (array.size() == 0) {
            throw new IllegalArgumentException(
                    "Producer batch mode received 0 records — nothing to send");
        }

        List<BatchRecord> records = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            JsonNode element = array.get(i);
            if (!element.isObject()) {
                throw new IllegalArgumentException(
                        "Record at index " + i + " is not a JSON object");
            }
            if (!element.has("value")) {
                throw new IllegalArgumentException(
                        "Record at index " + i + " is missing required 'value' field");
            }

            // Key: optional, nullable
            String key = null;
            if (element.has("key") && !element.get("key").isNull()) {
                key = element.get("key").isTextual()
                        ? element.get("key").asText()
                        : element.get("key").toString();
            }

            // Value: required, nullable (tombstone)
            String value = null;
            JsonNode valueNode = element.get("value");
            if (!valueNode.isNull()) {
                value = valueNode.isTextual()
                        ? valueNode.asText()
                        : valueNode.toString();
            }

            // Headers: optional
            java.util.Map<String, String> headers = null;
            if (element.has("headers") && element.get("headers").isObject()) {
                headers = new java.util.HashMap<>();
                java.util.Iterator<java.util.Map.Entry<String, JsonNode>> fields = element.get("headers").fields();
                while (fields.hasNext()) {
                    java.util.Map.Entry<String, JsonNode> field = fields.next();
                    if (!field.getValue().isNull()) {
                        headers.put(field.getKey(), field.getValue().isTextual() ? field.getValue().asText() : field.getValue().toString());
                    }
                }
            }

            records.add(new BatchRecord(key, value, headers));
        }
        return records;
    }

    /**
     * Accepts three root shapes for backward compatibility:
     * <ul>
     *   <li>{@code {"kafkaRecords":{"record":[...]}}} — current consumer output (CPI JSON→XML compatible)</li>
     *   <li>{@code {"kafkaRecords":{"record":{...}}}} — single record object instead of array</li>
     *   <li>{@code {"kafkaRecords":[...]}} — legacy wrapped-array form</li>
     *   <li>{@code [...]} — legacy root-array form</li>
     * </ul>
     */
    private static ArrayNode extractRecordArray(JsonNode root) {
        if (root.isArray()) {
            return (ArrayNode) root;
        }
        if (root.isObject() && root.has("kafkaRecords")) {
            JsonNode wrapped = root.get("kafkaRecords");
            if (wrapped.isArray()) {
                return (ArrayNode) wrapped;
            }
            if (wrapped.isObject() && wrapped.has("record")) {
                JsonNode recordNode = wrapped.get("record");
                if (recordNode.isArray()) {
                    return (ArrayNode) recordNode;
                } else if (recordNode.isObject()) {
                    ArrayNode arrayNode = MAPPER.createArrayNode();
                    arrayNode.add(recordNode);
                    return arrayNode;
                }
            }
        }
        throw new IllegalArgumentException(
                "Producer batch mode expects a JSON array, "
                + "{\"kafkaRecords\":[...]}, {\"kafkaRecords\":{\"record\":[...]}}, or a single record object, but received: "
                + root.getNodeType());
    }

    /**
     * Parse an XML document with root element kafkaRecords into BatchRecords.
     * Each record element must have at least a value child. key is optional.
     * Unknown child elements are ignored.
     */
    public static List<BatchRecord> parseXml(String body) {
        if (body == null || body.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Producer batch mode is enabled but message body is empty");
        }

        Document doc;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            doc = builder.parse(new InputSource(new StringReader(body)));
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Producer batch mode failed to parse XML: " + e.getMessage(), e);
        }

        Element root = doc.getDocumentElement();
        if (!"kafkaRecords".equals(root.getTagName())) {
            throw new IllegalArgumentException(
                    "Producer batch mode expects root element <kafkaRecords>, but found: <"
                    + root.getTagName() + ">");
        }

        NodeList recordNodes = root.getElementsByTagName("record");
        if (recordNodes.getLength() == 0) {
            throw new IllegalArgumentException(
                    "Producer batch mode received 0 records — nothing to send");
        }

        List<BatchRecord> records = new ArrayList<>(recordNodes.getLength());
        for (int i = 0; i < recordNodes.getLength(); i++) {
            Element recordEl = (Element) recordNodes.item(i);

            // Key: optional; absent = null, empty string = explicit empty key
            String key = null;
            NodeList keyNodes = recordEl.getElementsByTagName("key");
            if (keyNodes.getLength() > 0) {
                key = keyNodes.item(0).getTextContent();
            }

            // Value: required
            NodeList valueNodes = recordEl.getElementsByTagName("value");
            if (valueNodes.getLength() == 0) {
                throw new IllegalArgumentException(
                        "Record at index " + i + " is missing required <value> element");
            }

            Element valueEl = (Element) valueNodes.item(0);
            String value = extractValueContent(valueEl);

            // Headers: optional
            java.util.Map<String, String> headers = null;
            NodeList headersNodes = recordEl.getElementsByTagName("headers");
            if (headersNodes.getLength() > 0) {
                Element headersEl = (Element) headersNodes.item(0);
                NodeList headerList = headersEl.getElementsByTagName("header");
                if (headerList.getLength() > 0) {
                    headers = new java.util.HashMap<>();
                    for (int j = 0; j < headerList.getLength(); j++) {
                        Element hEl = (Element) headerList.item(j);
                        String hName = hEl.getAttribute("name");
                        if (hName != null && !hName.isEmpty()) {
                            headers.put(hName, hEl.getTextContent());
                        }
                    }
                }
            }

            records.add(new BatchRecord(key, value, headers));
        }
        return records;
    }

    /**
     * Extract the content of a value element.
     * If it has child elements (not CDATA/text), re-serialize them to XML string.
     * If it has only text content, return the text.
     * Empty element returns null (tombstone).
     */
    private static String extractValueContent(Element valueEl) {
        // Check for child elements
        NodeList children = valueEl.getChildNodes();
        boolean hasElementChildren = false;
        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
                hasElementChildren = true;
                break;
            }
        }

        if (hasElementChildren) {
            // Re-serialize child elements to XML string
            StringBuilder sb = new StringBuilder();
            try {
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
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Failed to serialize XML child elements in <value>: " + e.getMessage(), e);
            }
            return sb.toString();
        }

        // Text/CDATA content
        String text = valueEl.getTextContent();
        if (text == null || text.isEmpty()) {
            return null; // tombstone
        }
        return text;
    }
}
