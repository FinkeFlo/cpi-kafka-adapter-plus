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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Avro serialization using a lightweight JDK-only Schema Registry client.
 * Replaces the Confluent {@code KafkaAvroSerializer} and {@code CachedSchemaRegistryClient} —
 * no Confluent dependencies required.
 *
 * <p>Output wire format (Confluent-compatible):
 * <pre>
 *   Byte 0:    0x00  (magic byte)
 *   Bytes 1-4: schema ID (big-endian int)
 *   Bytes 5+:  Avro binary payload
 * </pre>
 */
public class AvroSerializerHelper implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSerializerHelper.class);

    private static final String TOPIC_NAME_STRATEGY = "TopicNameStrategy";

    private final SchemaRegistryHttpClient registryClient;
    private final String                   strategyName;
    private final boolean                  autoRegisterSchemas;

    public AvroSerializerHelper(CpiKafkaPlusEndpoint endpoint) {
        this.autoRegisterSchemas = endpoint.isAutoRegisterSchemas();
        this.strategyName = endpoint.getSubjectNameStrategy() != null
                ? endpoint.getSubjectNameStrategy() : TOPIC_NAME_STRATEGY;

        CredentialHelper.UserCredentials creds =
                (endpoint.getSchemaRegistryCredentialAlias() != null
                        && !endpoint.getSchemaRegistryCredentialAlias().isEmpty())
                ? CredentialHelper.getUserCredential(endpoint.getSchemaRegistryCredentialAlias())
                : null;
        String username = creds != null ? creds.username() : null;
        String password = creds != null ? creds.password() : null;

        this.registryClient = new SchemaRegistryHttpClient(endpoint.getSchemaRegistryUrl(), username, password);

        LOG.info("[CPI-KAFKA-PLUS-DIAG] Avro serializer initialized with Schema Registry at '{}', "
                + "autoRegister={}, strategy={}",
                endpoint.getSchemaRegistryUrl(), autoRegisterSchemas, strategyName);
    }

    /**
     * Serializes JSON data to Avro binary in Confluent wire format.
     *
     * <p>Schema resolution order:
     * <ol>
     *   <li>Cached schema from a previous call</li>
     *   <li>Schema Registry lookup by subject ({@code topic-value})</li>
     *   <li>If {@code autoRegisterSchemas=true} and subject not found: infer schema from JSON and register</li>
     * </ol>
     *
     * @param topic    Kafka topic name (used for subject resolution)
     * @param jsonData JSON string representation of the Avro record
     * @return Avro binary bytes in Confluent wire format, or {@code null} for empty input
     */
    public byte[] serialize(String topic, String jsonData) {
        if (jsonData == null || jsonData.isEmpty()) {
            return null;
        }
        try {
            String subject  = resolveSubject(topic);
            Schema schema   = resolveSchema(subject, jsonData);
            int    schemaId = registryClient.getSchemaId(subject);

            // Parse JSON → GenericRecord using Avro's JsonDecoder
            GenericDatumReader<GenericRecord> reader  = new GenericDatumReader<>(schema);
            org.apache.avro.io.Decoder        decoder = DecoderFactory.get().jsonDecoder(schema, jsonData);
            GenericRecord                     record  = reader.read(null, decoder);

            // Encode GenericRecord → Confluent wire format
            return encodeWireFormat(schemaId, schema, record);
        } catch (Exception e) {
            if (e instanceof RuntimeException
                    && e.getMessage() != null
                    && e.getMessage().startsWith("Avro serialization failed:")) {
                throw (RuntimeException) e;
            }
            LOG.error("[CPI-KAFKA-PLUS-DIAG] Avro serialization failed for topic '{}': {}",
                    topic, e.getMessage(), e);
            throw new RuntimeException("Avro serialization failed: " + e.getMessage(), e);
        }
    }

    // -----------------------------------------------------------------------
    // Wire format encoding
    // -----------------------------------------------------------------------

    private static byte[] encodeWireFormat(int schemaId, Schema schema, GenericRecord record)
            throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // 5-byte header: magic byte + schema ID
        out.write(AvroDeserializerHelper.MAGIC_BYTE);
        out.write(ByteBuffer.allocate(4).putInt(schemaId).array());

        // Avro binary payload
        GenericDatumWriter<GenericRecord> writer  = new GenericDatumWriter<>(schema);
        BinaryEncoder                     encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();

        return out.toByteArray();
    }

    // -----------------------------------------------------------------------
    // Schema resolution
    // -----------------------------------------------------------------------

    private Schema resolveSchema(String subject, String jsonData) throws Exception {
        try {
            return registryClient.fetchSchemaBySubject(subject);
        } catch (SchemaRegistryHttpClient.SchemaRegistryException e) {
            if (e.getHttpStatus() == 404 && autoRegisterSchemas) {
                LOG.info("[CPI-KAFKA-PLUS-DIAG] Subject '{}' not found in registry, "
                        + "inferring schema from JSON and registering", subject);
                String schemaJson = inferSchemaFromJson(jsonData, subject).toString();
                registryClient.registerSchema(subject, schemaJson);
                return registryClient.fetchSchemaBySubject(subject);
            }
            if (e.getHttpStatus() == 404) {
                throw new RuntimeException(
                        "Schema subject '" + subject + "' not found in Schema Registry. "
                        + "Create the schema in the registry or enable 'Auto Register Schemas'.", e);
            }
            throw e;
        }
    }

    // -----------------------------------------------------------------------
    // Subject name strategy
    // -----------------------------------------------------------------------

    private String resolveSubject(String topic) {
        if (TOPIC_NAME_STRATEGY.equals(strategyName)) {
            return topic + "-value";
        }
        throw new IllegalStateException(
                "SubjectNameStrategy '" + strategyName + "' is not supported for Avro serialization "
                + "from JSON input. The subject cannot be resolved without the record name. "
                + "Use TopicNameStrategy (default) instead.");
    }

    // -----------------------------------------------------------------------
    // Schema inference from JSON (auto-register path)
    // -----------------------------------------------------------------------

    private Schema inferSchemaFromJson(String jsonData, String subject) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode     root   = mapper.readTree(jsonData);
        if (!root.isObject()) {
            throw new IllegalArgumentException(
                    "Avro schema inference requires a JSON object at root level, got " + root.getNodeType());
        }
        String recordName = sanitizeAvroName(subject);
        Schema schema     = buildRecordSchema(root, recordName);
        LOG.info("[CPI-KAFKA-PLUS-DIAG] Inferred Avro schema: {}", schema.toString(true));
        return schema;
    }

    private Schema buildRecordSchema(JsonNode node, String name) {
        List<Schema.Field> fields = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            Schema fieldSchema = inferFieldType(entry.getValue(), sanitizeAvroName(entry.getKey()));
            fields.add(new Schema.Field(entry.getKey(), fieldSchema));
        }
        Schema record = Schema.createRecord(name, null, "com.finkeflo.cpi.kafka.generated", false);
        record.setFields(fields);
        return record;
    }

    private Schema inferFieldType(JsonNode node, String nestedName) {
        if (node.isTextual())           return Schema.create(Schema.Type.STRING);
        if (node.isBoolean())           return Schema.create(Schema.Type.BOOLEAN);
        if (node.isIntegralNumber())    return Schema.create(Schema.Type.LONG);
        if (node.isFloatingPointNumber()) return Schema.create(Schema.Type.DOUBLE);
        if (node.isObject())            return buildRecordSchema(node, nestedName);
        if (node.isArray()) {
            if (node.size() == 0) return Schema.createArray(Schema.create(Schema.Type.STRING));
            return Schema.createArray(inferFieldType(node.get(0), nestedName + "_item"));
        }
        return Schema.create(Schema.Type.STRING);
    }

    private static String sanitizeAvroName(String name) {
        String s = name.replaceAll("[^A-Za-z0-9_]", "_");
        if (s.isEmpty() || Character.isDigit(s.charAt(0))) s = "_" + s;
        return s;
    }

    @Override
    public void close() {
        // SchemaRegistryHttpClient is stateless (no persistent connections) — nothing to close
    }
}
