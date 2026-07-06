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
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Formats a batch of Kafka ConsumerRecords into JSON Array or XML List.
 */
public final class BatchFormatter {

    private static final Logger LOG = LoggerFactory.getLogger(BatchFormatter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BatchFormatter() {}

    /**
     * Format records as a JSON object mirroring the XML_LIST shape:
     * {@code {"kafkaRecords": {"record": [ ... ]}}}.
     *
     * The extra {@code record} object level is required so the root has a
     * single non-array member — the CPI standard JSON→XML converter rejects
     * root objects whose only member is a multi-element array.
     *
     * Each record element: {"key": ..., "value": ..., "topic": ..., "partition": ..., "offset": ..., "timestamp": ...}
     */
    public static String toJsonArray(
            List<ConsumerRecord<byte[], byte[]>> records,
            BiFunction<String, byte[], String> keyDeserializer,
            BiFunction<String, byte[], String> valueDeserializer) throws Exception {

        ArrayNode array = MAPPER.createArrayNode();

        for (ConsumerRecord<byte[], byte[]> record : records) {
            ObjectNode node = MAPPER.createObjectNode();
            String key = keyDeserializer.apply(record.topic(), record.key());
            String value = valueDeserializer.apply(record.topic(), record.value());

            if (key != null) {
                // Try to parse as JSON, if it fails, use as plain string
                try {
                    node.set("key", MAPPER.readTree(key));
                } catch (Exception e) {
                    node.put("key", key);
                }
            } else {
                node.putNull("key");
            }

            if (value != null) {
                try {
                    node.set("value", MAPPER.readTree(value));
                } catch (Exception e) {
                    node.put("value", value);
                }
            } else {
                node.putNull("value");
            }

            node.put("topic", record.topic());
            node.put("partition", record.partition());
            node.put("offset", record.offset());
            node.put("timestamp", record.timestamp());

            array.add(node);
        }

        ObjectNode inner = MAPPER.createObjectNode();
        inner.set("record", array);
        ObjectNode root = MAPPER.createObjectNode();
        root.set("kafkaRecords", inner);
        return MAPPER.writeValueAsString(root);
    }

    /**
     * Format records as an XML list. Delegates to the 4-arg version with embedXml=false.
     */
    public static String toXml(
            List<ConsumerRecord<byte[], byte[]>> records,
            BiFunction<String, byte[], String> keyDeserializer,
            BiFunction<String, byte[], String> valueDeserializer) {
        return toXml(records, keyDeserializer, valueDeserializer, false);
    }

    /**
     * Format records as an XML list.
     *
     * @param embedXml when true, values that look like XML are embedded as child
     *                 elements (prolog stripped); non-XML values use CDATA.
     *                 When false, all values use CDATA wrapping.
     */
    public static String toXml(
            List<ConsumerRecord<byte[], byte[]>> records,
            BiFunction<String, byte[], String> keyDeserializer,
            BiFunction<String, byte[], String> valueDeserializer,
            boolean embedXml) {

        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        sb.append("<kafkaRecords count=\"").append(records.size()).append("\">\n");

        for (ConsumerRecord<byte[], byte[]> record : records) {
            String key = keyDeserializer.apply(record.topic(), record.key());
            String value = valueDeserializer.apply(record.topic(), record.value());

            sb.append("  <record>\n");
            sb.append("    <key>").append(key != null ? escapeXml(key) : "").append("</key>\n");
            sb.append("    ");
            appendValueElement(sb, value, embedXml);
            sb.append("\n");
            sb.append("    <topic>").append(escapeXml(record.topic())).append("</topic>\n");
            sb.append("    <partition>").append(record.partition()).append("</partition>\n");
            sb.append("    <offset>").append(record.offset()).append("</offset>\n");
            sb.append("    <timestamp>").append(record.timestamp()).append("</timestamp>\n");
            sb.append("  </record>\n");
        }

        sb.append("</kafkaRecords>");
        return sb.toString();
    }

    /**
     * Appends a complete {@code <value format="...">...</value>} element to the
     * StringBuilder. The attribute is always set:
     * <ul>
     *   <li>{@code format="xml"} — value was embedded directly as XML children;
     *       downstream can use XPath like {@code value/Bestellung/...}</li>
     *   <li>{@code format="text"} — value is CDATA-wrapped text (or empty);
     *       downstream must extract via {@code string(value)}</li>
     * </ul>
     * Null/empty values emit {@code <value format="text"></value>}.
     */
    private static void appendValueElement(StringBuilder sb, String value, boolean embedXml) {
        if (value == null || value.isEmpty()) {
            sb.append("<value format=\"text\"></value>");
            return;
        }

        if (embedXml) {
            int contentStart = findXmlContentStart(value);
            int contentEnd = value.length();
            while (contentEnd > contentStart && Character.isWhitespace(value.charAt(contentEnd - 1))) {
                contentEnd--;
            }
            if (contentStart < contentEnd
                    && value.charAt(contentStart) == '<'
                    && value.charAt(contentEnd - 1) == '>') {
                sb.append("<value format=\"xml\">");
                sb.append(value, contentStart, contentEnd);
                sb.append("</value>");
                return;
            }
        }

        sb.append("<value format=\"text\">");
        appendCdata(sb, value);
        sb.append("</value>");
    }

    static String escapeXml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
    }

    /**
     * Appends the value wrapped in CDATA to the StringBuilder.
     * Handles the rare case where the value contains "]]>" by splitting.
     * Zero-alloc on the common path (no "]]>" in value).
     */
    static void appendCdata(StringBuilder sb, String text) {
        if (text == null || text.isEmpty()) {
            return;
        }
        sb.append("<![CDATA[");
        if (text.contains("]]>")) {
            sb.append(text.replace("]]>", "]]]]><![CDATA[>"));
        } else {
            sb.append(text);
        }
        sb.append("]]>");
    }

    /**
     * Finds the start index of the actual XML content, skipping any leading
     * whitespace and optional {@code <?xml ...?>} prolog.
     * Zero-alloc: operates on charAt() only, no Matcher or substring created.
     *
     * @return index into value where content starts
     */
    static int findXmlContentStart(String value) {
        int len = value.length();
        int i = 0;

        // Skip leading whitespace
        while (i < len && Character.isWhitespace(value.charAt(i))) {
            i++;
        }

        // Check for <?xml ...?> prolog
        if (i + 5 < len && value.charAt(i) == '<' && value.charAt(i + 1) == '?'
                && value.charAt(i + 2) == 'x' && value.charAt(i + 3) == 'm'
                && value.charAt(i + 4) == 'l') {
            // Scan forward to find "?>"
            int searchFrom = i + 5;
            while (searchFrom + 1 < len) {
                if (value.charAt(searchFrom) == '?' && value.charAt(searchFrom + 1) == '>') {
                    i = searchFrom + 2;
                    // Skip whitespace after prolog
                    while (i < len && Character.isWhitespace(value.charAt(i))) {
                        i++;
                    }
                    break;
                }
                searchFrom++;
            }
        }

        return i;
    }
}
