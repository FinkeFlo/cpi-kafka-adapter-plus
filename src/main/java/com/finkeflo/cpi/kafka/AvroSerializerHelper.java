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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Handles Avro serialization using Confluent Schema Registry.
 * Converts JSON string input to Avro binary (Confluent wire format).
 */
public class AvroSerializerHelper implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSerializerHelper.class);

    private static final String TOPIC_NAME_STRATEGY = "TopicNameStrategy";

    private final KafkaAvroSerializer avroSerializer;
    private final SchemaRegistryClient schemaRegistryClient;
    private final String strategyName;
    private final boolean autoRegisterSchemas;
    private final Map<String, Schema> schemaCache = new HashMap<>();

    public AvroSerializerHelper(CpiKafkaPlusEndpoint endpoint) {
        this.autoRegisterSchemas = endpoint.isAutoRegisterSchemas();

        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", endpoint.getSchemaRegistryUrl());
        config.put("auto.register.schemas", autoRegisterSchemas);

        this.strategyName = endpoint.getSubjectNameStrategy() != null
                ? endpoint.getSubjectNameStrategy() : TOPIC_NAME_STRATEGY;
        String strategy = resolveSubjectNameStrategy(this.strategyName);
        config.put("value.subject.name.strategy", strategy);

        // Schema Registry authentication
        CredentialHelper.configureSchemaRegistryAuth(config, endpoint.getSchemaRegistryCredentialAlias());

        this.schemaRegistryClient = new CachedSchemaRegistryClient(
                Collections.singletonList(endpoint.getSchemaRegistryUrl()), 1000, config);

        this.avroSerializer = new KafkaAvroSerializer();
        this.avroSerializer.configure(config, false);

        LOG.info("Avro serializer initialized with Schema Registry at '{}', autoRegister={}, strategy={}",
                endpoint.getSchemaRegistryUrl(), endpoint.isAutoRegisterSchemas(), endpoint.getSubjectNameStrategy());
    }

    /**
     * Serialize JSON data to Avro binary using Confluent wire format.
     * Schema resolution order:
     * 1. Cached schema (from previous call)
     * 2. Schema Registry lookup by subject
     * 3. If autoRegisterSchemas=true and subject not found: infer schema from JSON data
     *
     * @param topic   Kafka topic name (used for subject resolution)
     * @param jsonData JSON string representation of the record
     * @return Avro binary bytes in Confluent wire format (magic byte + schema ID + Avro payload)
     */
    public byte[] serialize(String topic, String jsonData) {
        if (jsonData == null || jsonData.isEmpty()) {
            return null;
        }

        try {
            String subject = resolveSubject(topic);
            Schema schema = getSchema(subject, jsonData, topic);

            // Parse JSON string into GenericRecord using the resolved schema
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            org.apache.avro.io.Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonData);
            GenericRecord record = reader.read(null, decoder);

            // Serialize GenericRecord to Confluent wire format (magic byte + schema ID + Avro binary)
            return avroSerializer.serialize(topic, record);
        } catch (Exception e) {
            LOG.error("Failed to serialize Avro message for topic '{}': {}", topic, e.getMessage(), e);
            throw new RuntimeException("Avro serialization failed: " + e.getMessage(), e);
        }
    }

    private Schema getSchema(String subject, String jsonData, String topic) throws Exception {
        Schema cached = schemaCache.get(subject);
        if (cached != null) {
            return cached;
        }

        try {
            SchemaMetadata meta = schemaRegistryClient.getLatestSchemaMetadata(subject);
            Schema schema = new Schema.Parser().parse(meta.getSchema());
            schemaCache.put(subject, schema);
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] Schema loaded from registry for subject '{}'", subject);
            return schema;
        } catch (RestClientException e) {
            if (e.getErrorCode() == 40401 && autoRegisterSchemas) {
                LOG.info("[CPI-KAFKA-PLUS-DIAG] Subject '{}' not found in registry, "
                        + "inferring schema from JSON and auto-registering", subject);
                Schema inferred = inferSchemaFromJson(jsonData, topic);
                schemaCache.put(subject, inferred);
                return inferred;
            }
            if (e.getErrorCode() == 40401) {
                throw new RuntimeException(
                        "Schema subject '" + subject + "' not found in Schema Registry. "
                        + "Create the schema in the registry or enable 'Auto Register Schemas'.", e);
            }
            throw e;
        }
    }

    // -----------------------------------------------------------------------
    // Schema inference from JSON
    // -----------------------------------------------------------------------

    private Schema inferSchemaFromJson(String jsonData, String topic) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonData);
        if (!root.isObject()) {
            throw new IllegalArgumentException(
                    "Avro schema inference requires a JSON object at root level, got " + root.getNodeType());
        }
        String recordName = sanitizeAvroName(topic);
        Schema schema = buildRecordSchema(root, recordName);
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
        if (node.isTextual()) {
            return Schema.create(Schema.Type.STRING);
        } else if (node.isBoolean()) {
            return Schema.create(Schema.Type.BOOLEAN);
        } else if (node.isIntegralNumber()) {
            return Schema.create(Schema.Type.LONG);
        } else if (node.isFloatingPointNumber()) {
            return Schema.create(Schema.Type.DOUBLE);
        } else if (node.isObject()) {
            return buildRecordSchema(node, nestedName);
        } else if (node.isArray()) {
            if (node.size() == 0) {
                return Schema.createArray(Schema.create(Schema.Type.STRING));
            }
            return Schema.createArray(inferFieldType(node.get(0), nestedName + "_item"));
        }
        // null or unknown → default to string
        return Schema.create(Schema.Type.STRING);
    }

    private static String sanitizeAvroName(String name) {
        String sanitized = name.replaceAll("[^A-Za-z0-9_]", "_");
        if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
            sanitized = "_" + sanitized;
        }
        return sanitized;
    }

    /**
     * Resolves the Schema Registry subject name based on the configured strategy.
     *
     * TopicNameStrategy uses "{topic}-key" / "{topic}-value" and can be resolved
     * without knowing the record schema. RecordNameStrategy and TopicRecordNameStrategy
     * require the fully-qualified record name, which is only available after the schema
     * is already fetched — a circular dependency. These strategies are not supported
     * for the manual JSON-to-GenericRecord schema lookup.
     */
    private String resolveSubject(String topic) {
        if (TOPIC_NAME_STRATEGY.equals(strategyName)) {
            return topic + "-value";
        }
        throw new IllegalStateException(
                "SubjectNameStrategy '" + strategyName + "' is not supported for Avro serialization "
                + "from JSON input. The schema subject cannot be resolved without knowing the record name, "
                + "which requires the schema itself. Use TopicNameStrategy (default) instead.");
    }

    private static String resolveSubjectNameStrategy(String strategyName) {
        if (strategyName == null) {
            return "io.confluent.kafka.serializers.subject.TopicNameStrategy";
        }
        switch (strategyName) {
            case "RecordNameStrategy":
                return "io.confluent.kafka.serializers.subject.RecordNameStrategy";
            case "TopicRecordNameStrategy":
                return "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy";
            default:
                return "io.confluent.kafka.serializers.subject.TopicNameStrategy";
        }
    }

    @Override
    public void close() {
        try {
            avroSerializer.close();
        } catch (Exception e) {
            LOG.debug("Error closing Avro value serializer", e);
        }
        try {
            schemaRegistryClient.close();
        } catch (Exception e) {
            LOG.debug("Error closing Schema Registry client", e);
        }
    }
}
