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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;

/**
 * The header Confluent's serializers write in front of a value: the magic byte {@code 0x00} and the
 * schema ID as a 4-byte big-endian integer, followed by the serialized value.
 */
final class ConfluentWireFormat {

    static final byte MAGIC_BYTE = 0x00;
    static final int HEADER_LENGTH = 5;

    /**
     * Checks that JSON follows a Confluent header. Without Jackson's default limits, so that a value is
     * not rejected only because it is large: the IFlow gets it either way.
     */
    private static final JsonFactory JSON_CHECK = JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder()
                    .maxStringLength(Integer.MAX_VALUE)
                    .maxNumberLength(Integer.MAX_VALUE)
                    .maxNameLength(Integer.MAX_VALUE)
                    .maxNestingDepth(Integer.MAX_VALUE)
                    .build())
            .build();

    private ConfluentWireFormat() {
    }

    /** Whether {@code data} is long enough for the header and starts with the magic byte. */
    static boolean hasHeader(byte[] data) {
        return data != null && data.length >= HEADER_LENGTH && data[0] == MAGIC_BYTE;
    }

    /** The schema ID in bytes 1 to 4; {@code data} must be at least {@link #HEADER_LENGTH} bytes long. */
    static int schemaId(byte[] data) {
        return ByteBuffer.wrap(data, 1, 4).getInt();
    }

    /** Whether {@code text} is exactly one JSON value, with nothing but whitespace around it. */
    static boolean isOneJsonValue(String text) {
        try (JsonParser parser = JSON_CHECK.createParser(text)) {
            if (parser.nextToken() == null) {
                return false;
            }
            parser.skipChildren();
            return parser.nextToken() == null;
        } catch (IOException e) {
            return false;
        }
    }
}
