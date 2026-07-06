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
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

/**
 * Handles Avro deserialization using Confluent Schema Registry.
 * Converts Avro GenericRecord to JSON or XML string.
 */
public class AvroDeserializerHelper implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDeserializerHelper.class);
    private final KafkaAvroDeserializer avroDeserializer;
    private final String avroOutputFormat;

    public AvroDeserializerHelper(CpiKafkaPlusEndpoint endpoint) {
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", endpoint.getSchemaRegistryUrl());

        // Schema Registry authentication
        CredentialHelper.configureSchemaRegistryAuth(config, endpoint.getSchemaRegistryCredentialAlias());

        config.put("specific.avro.reader", false);

        this.avroDeserializer = new KafkaAvroDeserializer();
        this.avroDeserializer.configure(config, false);

        this.avroOutputFormat = endpoint.getAvroOutputFormat();

        LOG.info("Avro deserializer initialized with Schema Registry at '{}', output format: {}",
                endpoint.getSchemaRegistryUrl(), avroOutputFormat);
    }

    /**
     * Deserialize Avro-encoded bytes to JSON or XML string.
     *
     * @param topic Kafka topic name (used for subject resolution)
     * @param data  Raw bytes from Kafka (Confluent wire format: magic byte + schema ID + Avro payload)
     * @return Deserialized string in configured output format
     */
    public String deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try {
            Object deserialized = avroDeserializer.deserialize(topic, data);
            if (deserialized == null) {
                return null;
            }

            if (deserialized instanceof GenericRecord) {
                return convertGenericRecord((GenericRecord) deserialized);
            }

            // Primitive types (String, Integer, etc.)
            return deserialized.toString();
        } catch (Exception e) {
            LOG.error("Failed to deserialize Avro message from topic '{}': {}", topic, e.getMessage(), e);
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
        org.apache.avro.io.JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), baos);
        writer.write(record, encoder);
        encoder.flush();
        return baos.toString("UTF-8");
    }

    private String genericRecordToXml(GenericRecord record) {
        StringBuilder sb = new StringBuilder();
        sb.append("<record>");
        for (Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());
            sb.append("<").append(field.name()).append(">");
            if (value instanceof GenericRecord) {
                sb.append(genericRecordToXml((GenericRecord) value));
            } else if (value != null) {
                sb.append(BatchFormatter.escapeXml(value.toString()));
            }
            sb.append("</").append(field.name()).append(">");
        }
        sb.append("</record>");
        return sb.toString();
    }

    @Override
    public void close() {
        try {
            avroDeserializer.close();
        } catch (Exception e) {
            LOG.debug("Error closing Avro value deserializer", e);
        }
    }
}
