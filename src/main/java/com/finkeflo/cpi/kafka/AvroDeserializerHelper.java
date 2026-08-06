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
import java.io.Closeable;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Avro deserialization using a lightweight JDK-only Schema Registry client.
 * Replaces the Confluent {@code KafkaAvroDeserializer} — no Confluent dependencies required.
 *
 * <p>Wire format (Confluent-compatible):
 * <pre>
 *   Byte 0:    0x00  (magic byte)
 *   Bytes 1-4: schema ID (big-endian int)
 *   Bytes 5+:  Avro binary payload
 * </pre>
 */
public class AvroDeserializerHelper implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDeserializerHelper.class);

    static final byte MAGIC_BYTE = 0x00;
    static final int  HEADER_LENGTH = 5; // magic byte + 4-byte schema ID

    private final SchemaRegistryHttpClient registryClient;
    private final String                   avroOutputFormat;

    public AvroDeserializerHelper(CpiKafkaPlusEndpoint endpoint) {
        CredentialHelper.UserCredentials creds =
                (endpoint.getSchemaRegistryCredentialAlias() != null
                        && !endpoint.getSchemaRegistryCredentialAlias().isEmpty())
                ? CredentialHelper.getUserCredential(endpoint.getSchemaRegistryCredentialAlias())
                : null;

        String username = creds != null ? creds.username() : null;
        String password = creds != null ? creds.password() : null;

        this.registryClient   = new SchemaRegistryHttpClient(endpoint.getSchemaRegistryUrl(), username, password);
        this.avroOutputFormat = endpoint.getAvroOutputFormat();

        LOG.info("[CPI-KAFKA-PLUS-DIAG] Avro deserializer initialized with Schema Registry at '{}', output format: {}",
                endpoint.getSchemaRegistryUrl(), avroOutputFormat);
    }

    /**
     * Deserializes Avro-encoded bytes to JSON or XML.
     *
     * @param data raw bytes from Kafka (Confluent wire format: magic byte + schema ID + Avro binary)
     * @return deserialized string in the configured output format, or {@code null} for empty input
     */
    public String deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        if (data.length < HEADER_LENGTH) {
            throw new RuntimeException(
                    "Avro message too short (" + data.length + " bytes) — expected at least "
                    + HEADER_LENGTH + " bytes (Confluent wire format header). "
                    + "Is this topic actually Avro-encoded?");
        }

        ByteBuffer buf = ByteBuffer.wrap(data);
        byte magic = buf.get();
        if (magic != MAGIC_BYTE) {
            throw new RuntimeException(
                    "Invalid Avro magic byte: 0x" + Integer.toHexString(magic & 0xFF)
                    + " (expected 0x00). Message may not be in Confluent wire format.");
        }

        int schemaId = buf.getInt();

        try {
            Schema schema = registryClient.fetchSchemaById(schemaId);

            // Remaining bytes after the 5-byte header are the Avro binary payload
            byte[] avroBytes = new byte[buf.remaining()];
            buf.get(avroBytes);

            org.apache.avro.generic.GenericDatumReader<GenericRecord> reader =
                    new org.apache.avro.generic.GenericDatumReader<>(schema);
            org.apache.avro.io.Decoder decoder =
                    org.apache.avro.io.DecoderFactory.get().binaryDecoder(avroBytes, null);
            GenericRecord record = reader.read(null, decoder);

            return convertGenericRecord(record);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] Avro deserialization failed for topic '{}' schemaId={}: {}",
                    topic, schemaId, e.getMessage(), e);
            throw new RuntimeException("Avro deserialization failed: " + e.getMessage(), e);
        }
    }

    private String convertGenericRecord(GenericRecord record) throws Exception {
        if ("XML".equalsIgnoreCase(avroOutputFormat)) {
            return genericRecordToXml(record);
        }
        return genericRecordToJson(record);
    }

    private String genericRecordToJson(GenericRecord record) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
        org.apache.avro.io.JsonEncoder encoder =
                EncoderFactory.get().jsonEncoder(record.getSchema(), baos);
        writer.write(record, encoder);
        encoder.flush();
        return baos.toString("UTF-8");
    }

    private String genericRecordToXml(GenericRecord record) {
        StringBuilder sb = new StringBuilder();
        sb.append("<record>");
        for (Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());
            sb.append('<').append(field.name()).append('>');
            if (value instanceof GenericRecord) {
                sb.append(genericRecordToXml((GenericRecord) value));
            } else if (value != null) {
                sb.append(BatchFormatter.escapeXml(value.toString()));
            }
            sb.append("</").append(field.name()).append('>');
        }
        sb.append("</record>");
        return sb.toString();
    }

    @Override
    public void close() {
        // SchemaRegistryHttpClient is stateless (no persistent connections) — nothing to close
    }
}
