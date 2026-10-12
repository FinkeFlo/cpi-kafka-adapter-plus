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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
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

    private static final ObjectMapper MAPPER = PayloadJson.newMapper();

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
                JsonNode headersNode = element.get("headers");
                
                // Support CPI XML-to-JSON converted headers
                if (headersNode.has("header") && (headersNode.get("header").isArray() || headersNode.get("header").isObject())) {
                    JsonNode headerNode = headersNode.get("header");
                    if (headerNode.isObject()) {
                        extractCpiJsonHeader(headerNode, headers);
                    } else {
                        for (JsonNode hNode : headerNode) {
                            extractCpiJsonHeader(hNode, headers);
                        }
                    }
                } else {
                    // Standard native JSON map
                    java.util.Iterator<java.util.Map.Entry<String, JsonNode>> fields = headersNode.fields();
                    while (fields.hasNext()) {
                        java.util.Map.Entry<String, JsonNode> field = fields.next();
                        if (!field.getValue().isNull()) {
                            headers.put(field.getKey(), field.getValue().isTextual() ? field.getValue().asText() : field.getValue().toString());
                        }
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
            // Namespace-aware, so that a value can take along the declarations it was written under.
            factory.setNamespaceAware(true);
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

        List<Element> recordEls = childElements(root, "record");
        if (recordEls.isEmpty() && root.getElementsByTagName("record").getLength() > 0) {
            // Accepted before #181, which read records at any depth; name the cause instead of
            // reporting an empty batch.
            throw new IllegalArgumentException(
                    "Producer batch mode found <record> elements, but <record> must be a direct child of "
                    + "<kafkaRecords> — remove the wrapper element around the records");
        }
        if (recordEls.isEmpty()) {
            throw new IllegalArgumentException(
                    "Producer batch mode received 0 records — nothing to send");
        }

        List<BatchRecord> records = new ArrayList<>(recordEls.size());
        for (int i = 0; i < recordEls.size(); i++) {
            Element recordEl = recordEls.get(i);

            // Key: optional; absent = null, empty string = explicit empty key
            String key = null;
            List<Element> keyEls = childElements(recordEl, "key");
            if (!keyEls.isEmpty()) {
                key = keyEls.get(0).getTextContent();
            }

            // Value: required
            List<Element> valueEls = childElements(recordEl, "value");
            if (valueEls.isEmpty()) {
                throw new IllegalArgumentException(
                        "Record at index " + i + " is missing required <value> element");
            }

            Element valueEl = valueEls.get(0);
            String value = extractValueContent(valueEl);

            // Headers: optional
            java.util.Map<String, String> headers = null;
            List<Element> headersEls = childElements(recordEl, "headers");
            Element headersEl = headersEls.isEmpty() ? null : headersEls.get(0);

            if (headersEl != null) {
                List<Element> headerList = childElements(headersEl, "header");
                if (!headerList.isEmpty()) {
                    headers = new java.util.HashMap<>();
                    for (Element hEl : headerList) {
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
     * Direct child elements of {@code parent} named {@code name}, in document order. The record
     * structure is read only one level deep: a {@code <record>}, {@code <key>} or {@code <header>} inside
     * a value is payload, and a deep search turned it into an extra record or a wrong partition key.
     */
    private static List<Element> childElements(Element parent, String name) {
        List<Element> result = new ArrayList<>();
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE && name.equals(child.getNodeName())) {
                result.add((Element) child);
            }
        }
        return result;
    }

    /**
     * Extract the content of a value element.
     * Without child elements the value is its text (CDATA included); an empty element is a
     * tombstone. With child elements every child node is re-serialized as XML, the text between
     * the elements included: serializing only the elements turned {@code Preis: <b>10</b> EUR}
     * into {@code <b>10</b>} (#247). Whitespace-only text in a value that has no other text is
     * indentation between the elements and stays dropped, as before.
     */
    private static String extractValueContent(Element valueEl) {
        NodeList children = valueEl.getChildNodes();
        boolean hasElementChildren = false;
        boolean hasText = false;
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                hasElementChildren = true;
            } else if (child.getNodeType() == Node.CDATA_SECTION_NODE
                    || (child.getNodeType() == Node.TEXT_NODE && !isXmlWhitespace(child.getNodeValue()))) {
                hasText = true;
            }
        }

        if (hasElementChildren) {
            Map<String, String> namespaces = inScopeNamespaces(valueEl);
            StringBuilder sb = new StringBuilder();
            try {
                Transformer transformer = TransformerFactory.newInstance().newTransformer();
                transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                Transformer xmlTransformer = null;
                for (int i = 0; i < children.getLength(); i++) {
                    Node child = children.item(i);
                    if (child.getNodeType() == Node.TEXT_NODE && !hasText) {
                        continue;
                    }
                    Transformer serializer = transformer;
                    if (child.getNodeType() == Node.ELEMENT_NODE) {
                        declareNamespaces((Element) child, namespaces);
                        if (isHtmlWithoutNamespace(child)) {
                            if (xmlTransformer == null) {
                                xmlTransformer = TransformerFactory.newInstance().newTransformer();
                                xmlTransformer.setOutputProperty(OutputKeys.METHOD, "xml");
                                xmlTransformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                            }
                            serializer = xmlTransformer;
                        }
                    }
                    StringWriter writer = new StringWriter();
                    serializer.transform(new DOMSource(child), new StreamResult(writer));
                    sb.append(writer.toString());
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

    /**
     * An element called {@code html}, in any case, in no namespace. Left to its default, the
     * transformer writes such an element with the HTML output method: {@code <br>} instead of
     * {@code <br/>}, which no XML parser reads, a {@code META} element added to {@code <head>}, and
     * indented. Only these elements get the XML method explicitly: under it the namespace
     * declarations of other elements would come out in a different order than they always did.
     */
    private static boolean isHtmlWithoutNamespace(Node element) {
        String namespace = element.getNamespaceURI();
        return "html".equalsIgnoreCase(element.getLocalName()) && (namespace == null || namespace.isEmpty());
    }

    private static boolean isXmlWhitespace(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                return false;
            }
        }
        return true;
    }

    /**
     * The namespace declarations in scope at {@code valueEl}, by prefix ({@code ""} for the default
     * namespace); the declaration nearest to the value wins. The value is serialized without its
     * ancestors, so declarations made on {@code <kafkaRecords>}, {@code <record>} or {@code <value>}
     * would otherwise be lost (#247).
     */
    private static Map<String, String> inScopeNamespaces(Element valueEl) {
        Map<String, String> namespaces = new LinkedHashMap<>();
        for (Node node = valueEl; node != null && node.getNodeType() == Node.ELEMENT_NODE;
                node = node.getParentNode()) {
            NamedNodeMap attributes = node.getAttributes();
            for (int i = 0; i < attributes.getLength(); i++) {
                Node attribute = attributes.item(i);
                if (!XMLConstants.XMLNS_ATTRIBUTE_NS_URI.equals(attribute.getNamespaceURI())) {
                    continue;
                }
                String prefix = XMLConstants.XMLNS_ATTRIBUTE.equals(attribute.getNodeName())
                        ? XMLConstants.DEFAULT_NS_PREFIX : attribute.getLocalName();
                if (!namespaces.containsKey(prefix)) {
                    namespaces.put(prefix, attribute.getNodeValue());
                }
            }
        }
        return namespaces;
    }

    /**
     * Declares the namespaces in scope at the value on one of its top-level elements, as an XSLT
     * {@code copy-of} would, so the value is the same XML on its own as it was inside the batch.
     * A declaration the element makes itself takes precedence.
     */
    private static void declareNamespaces(Element element, Map<String, String> namespaces) {
        for (Map.Entry<String, String> namespace : namespaces.entrySet()) {
            String prefix = namespace.getKey();
            // An empty URI only undeclares an outer default namespace, which the serialized value
            // does not have anyway; the xml prefix is bound without a declaration.
            if (namespace.getValue().isEmpty() || XMLConstants.XML_NS_PREFIX.equals(prefix)) {
                continue;
            }
            boolean isDefault = prefix.isEmpty();
            if (!element.hasAttributeNS(XMLConstants.XMLNS_ATTRIBUTE_NS_URI,
                    isDefault ? XMLConstants.XMLNS_ATTRIBUTE : prefix)) {
                element.setAttributeNS(XMLConstants.XMLNS_ATTRIBUTE_NS_URI,
                        isDefault ? XMLConstants.XMLNS_ATTRIBUTE : XMLConstants.XMLNS_ATTRIBUTE + ":" + prefix,
                        namespace.getValue());
            }
        }
    }

    private static void extractCpiJsonHeader(JsonNode node, java.util.Map<String, String> headers) {
        if (node != null && node.isObject() && node.has("@name") && node.has("$")) {
            String key = node.get("@name").asText();
            JsonNode valNode = node.get("$");
            if (!valNode.isNull()) {
                String val = valNode.isTextual() ? valNode.asText() : valNode.toString();
                headers.put(key, val);
            }
        }
    }
}
