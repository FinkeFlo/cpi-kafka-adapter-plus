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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Golden-master tests for byte/string-exact batch serialization formats.
 */
public class BatchFormatterGoldenTest {

    private static final String TOPIC = "golden-topic";
    private static final BiFunction<String, byte[], String> STRING_DESER =
            (topic, data) -> data != null ? new String(data, StandardCharsets.UTF_8) : null;

    @Test
    public void testJsonArrayMatchesGoldenFixture() throws Exception {
        String actual = BatchFormatter.toJsonArray(records(), STRING_DESER, STRING_DESER);

        Assert.assertEquals("JSON_ARRAY output must match golden fixture exactly",
                loadGolden("batch-json-array.json"), normalizeLineEndings(actual));
    }

    @Test
    public void testXmlListMatchesGoldenFixture() throws Exception {
        String actual = BatchFormatter.toXml(records(), STRING_DESER, STRING_DESER, false);

        Assert.assertEquals("XML_LIST output must match golden fixture exactly",
                loadGolden("batch-xml-list.xml"), normalizeLineEndings(actual));
    }

    private static List<ConsumerRecord<byte[], byte[]>> records() {
        return Arrays.asList(
                rec(2, 101L, 1700000000123L,
                        "{\"id\":\"key-1\"}",
                        "{\"orderId\":\"A-100\",\"amount\":42,\"active\":true}",
                        "alpha", "one"),
                rec(2, 102L, 1700000000456L,
                        "plain&key<2>\"'",
                        "Text with XML chars <tag attr=\"v\">&</tag> and CDATA end ]]> marker",
                        "beta", "two")
        );
    }

    private static ConsumerRecord<byte[], byte[]> rec(int partition, long offset, long timestamp,
                                                       String key, String value,
                                                       String headerName, String headerValue) {
        byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;
        byte[] valueBytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : null;
        Headers headers = new RecordHeaders()
                .add("hdr-" + headerName, headerValue.getBytes(StandardCharsets.UTF_8))
                .add("hdr-fixed", "fixed".getBytes(StandardCharsets.UTF_8));
        return new ConsumerRecord<byte[], byte[]>(TOPIC, partition, offset, timestamp,
                TimestampType.CREATE_TIME, keyBytes != null ? keyBytes.length : ConsumerRecord.NULL_SIZE,
                valueBytes != null ? valueBytes.length : ConsumerRecord.NULL_SIZE, keyBytes, valueBytes,
                headers, Optional.<Integer>of(Integer.valueOf(7)));
    }

    private static String loadGolden(String name) throws IOException {
        InputStream in = BatchFormatterGoldenTest.class.getClassLoader()
                .getResourceAsStream("golden/" + name);
        Assert.assertNotNull("Missing golden fixture: golden/" + name, in);
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
            return normalizeLineEndings(new String(out.toByteArray(), StandardCharsets.UTF_8));
        } finally {
            in.close();
        }
    }

    private static String normalizeLineEndings(String value) {
        return value.replace("\r\n", "\n").replace('\r', '\n');
    }
}
