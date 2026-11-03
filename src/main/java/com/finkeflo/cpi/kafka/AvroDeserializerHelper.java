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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads values in the Confluent wire format through a lightweight JDK-only Schema Registry client.
 * Replaces the Confluent {@code KafkaAvroDeserializer} — no Confluent dependencies required.
 *
 * <p>Wire format (Confluent-compatible):
 * <pre>
 *   Byte 0:    0x00  (magic byte)
 *   Bytes 1-4: schema ID (big-endian int)
 *   Bytes 5+:  payload: Avro binary, or JSON text for a JSON Schema
 * </pre>
 *
 * <p>The registry's {@code schemaType} of the ID decides how the payload is read: Avro is converted
 * to JSON or XML, JSON is passed on as it is. Other types, such as Protobuf, are rejected.
 */
public class AvroDeserializerHelper implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDeserializerHelper.class);

    private final SchemaRegistryHttpClient registryClient;
    private final String                   avroOutputFormat;
    private final boolean                  registrySchemaValidation;
    /** Compiled registry JSON Schemas by ID, which is immutable. */
    private final ConcurrentHashMap<Integer, JsonSchemaValidator> validators = new ConcurrentHashMap<>();
    /** Why a registry schema cannot be used for validation, per schema ID, so that it is not compiled again. */
    private final ConcurrentHashMap<Integer, String> unusableSchemas = new ConcurrentHashMap<>();
    private final AtomicBoolean reportedXmlForJson = new AtomicBoolean();

    public AvroDeserializerHelper(CpiKafkaPlusEndpoint endpoint) {
        CredentialHelper.UserCredentials creds = CredentialHelper.requireUserCredential(
                endpoint.getSchemaRegistryCredentialAlias(),
                UiLabels.field("schemaRegistryCredentialAlias"));

        String username = creds != null ? creds.username() : null;
        String password = creds != null ? creds.password() : null;

        this.registryClient   = new SchemaRegistryHttpClient(endpoint.getSchemaRegistryUrl(), username, password);
        this.avroOutputFormat = endpoint.getAvroOutputFormat();
        this.registrySchemaValidation = endpoint.isRegistrySchemaValidation();

        LOG.info("[CPI-KAFKA-PLUS-DIAG] Avro deserializer initialized with Schema Registry at '{}', output format: {}",
                SchemaRegistryHttpClient.withoutUserInfo(endpoint.getSchemaRegistryUrl()), avroOutputFormat);
    }

    /**
     * Deserializes a value in the Confluent wire format: Avro to JSON or XML, JSON as it is.
     *
     * @param data raw bytes from Kafka (Confluent wire format: magic byte + schema ID + payload)
     * @return deserialized string, or {@code null} for empty input
     */
    public String deserialize(String topic, byte[] data) {
        return decode(topic, data).payload();
    }

    /**
     * As {@link #deserialize}, together with the schema ID and type, and, with
     * {@code registrySchemaValidation} on, the validator for a JSON value.
     *
     * @throws SchemaRegistryHttpClient.SchemaRegistryException if the registry cannot serve the schema
     * @throws RegistryValueException if the value cannot be read with its registered schema
     */
    DecodedValue decode(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return DecodedValue.of(null);
        }
        if (data.length < ConfluentWireFormat.HEADER_LENGTH) {
            throw new RuntimeException(
                    "Schema Registry value too short (" + data.length + " bytes) — expected at least "
                    + ConfluentWireFormat.HEADER_LENGTH + " bytes (Confluent wire format header). "
                    + "Is this topic actually written with a Schema Registry serializer?");
        }

        ByteBuffer buf = ByteBuffer.wrap(data);
        byte magic = buf.get();
        if (magic != ConfluentWireFormat.MAGIC_BYTE) {
            throw new RuntimeException(
                    "Invalid magic byte: 0x" + Integer.toHexString(magic & 0xFF)
                    + " (expected 0x00). Message may not be in Confluent wire format.");
        }

        int schemaId = buf.getInt();
        SchemaRegistryHttpClient.RegisteredSchema registered;
        try {
            registered = registryClient.fetchRegisteredSchema(schemaId);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw avroFailure(topic, schemaId, e);
        }
        if (SchemaRegistryHttpClient.RegisteredSchema.AVRO.equals(registered.type())) {
            return new DecodedValue(deserializeAvro(topic, schemaId, buf), schemaId, registered.type(), null);
        }
        if (SchemaRegistryHttpClient.RegisteredSchema.JSON.equals(registered.type())) {
            return decodeJson(topic, registered, data);
        }
        throw new RegistryValueException(schemaId, registered.type(), "schema ID " + schemaId + " has schemaType "
                + registered.type() + ", which cannot be deserialized; supported are AVRO and JSON");
    }

    private String deserializeAvro(String topic, int schemaId, ByteBuffer buf) {
        Schema schema;
        try {
            schema = registryClient.fetchSchemaById(schemaId);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw avroFailure(topic, schemaId, e);
        }

        // Remaining bytes after the 5-byte header are the Avro binary payload
        byte[] avroBytes = new byte[buf.remaining()];
        buf.get(avroBytes);
        GenericRecord record;
        try {
            org.apache.avro.generic.GenericDatumReader<GenericRecord> reader =
                    new org.apache.avro.generic.GenericDatumReader<>(schema);
            org.apache.avro.io.Decoder decoder =
                    org.apache.avro.io.DecoderFactory.get().binaryDecoder(avroBytes, null);
            record = reader.read(null, decoder);
        } catch (java.io.IOException | org.apache.avro.AvroRuntimeException e) {
            // The record's own bytes do not fit its schema, such as a truncated payload: not the registry.
            throw new RegistryValueException(schemaId, SchemaRegistryHttpClient.RegisteredSchema.AVRO,
                    "the Avro binary does not fit the registered schema: " + e, e);
        }
        try {
            return convertGenericRecord(record);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw avroFailure(topic, schemaId, e);
        }
    }

    private RuntimeException avroFailure(String topic, int schemaId, Exception e) {
        AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("avro.deserialization.failed")
                .with("errorCode", CpiKafkaPlusErrorCode.KP_SR_001.code())
                .with("topic", topic)
                .with("schemaId", schemaId), e);
        return new RuntimeException("Avro deserialization failed: " + e.getMessage(), e);
    }

    /**
     * The JSON after the header, strictly UTF-8 and exactly one JSON value, so that a corrupt record
     * fails here rather than reaching the integration flow with replacement characters.
     */
    private DecodedValue decodeJson(String topic, SchemaRegistryHttpClient.RegisteredSchema registered,
                                    byte[] data) {
        int schemaId = registered.id();
        String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(data, ConfluentWireFormat.HEADER_LENGTH,
                            data.length - ConfluentWireFormat.HEADER_LENGTH))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new RegistryValueException(schemaId, registered.type(),
                    "the value after the header is not valid UTF-8", e);
        }
        if (!ConfluentWireFormat.isOneJsonValue(text)) {
            throw new RegistryValueException(schemaId, registered.type(),
                    "the value after the header is not exactly one JSON value");
        }
        if ("XML".equalsIgnoreCase(avroOutputFormat) && reportedXmlForJson.compareAndSet(false, true)) {
            // ERROR, not WARN: only ERROR reaches the tenant trace (ADR 0004). Once per consumer.
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("consumer.config.ineffective")
                    .with("parameter", "avroOutputFormat")
                    .with("value", avroOutputFormat)
                    .with("topic", topic)
                    .with("schemaId", schemaId)
                    .with("schemaType", registered.type())
                    .with("reason", "applies to Avro values only; JSON Schema values are passed on as JSON"));
        }
        return new DecodedValue(text, schemaId, registered.type(),
                registrySchemaValidation ? validatorFor(registered) : null);
    }

    private JsonSchemaValidator validatorFor(SchemaRegistryHttpClient.RegisteredSchema registered) {
        JsonSchemaValidator cached = validators.get(registered.id());
        if (cached != null) {
            return cached;
        }
        String unusable = unusableSchemas.get(registered.id());
        if (unusable != null) {
            throw new RegistryValueException(registered.id(), registered.type(), unusable);
        }
        if (registered.hasReferences()) {
            unusable = "the schema refers to other registered schemas (references), which Validate with "
                    + "Registry Schema does not resolve";
            unusableSchemas.put(registered.id(), unusable);
            throw new RegistryValueException(registered.id(), registered.type(), unusable);
        }
        JsonSchemaValidator validator;
        try {
            validator = JsonSchemaValidator.forRegistrySchema(registered.text());
        } catch (IllegalArgumentException e) {
            unusable = "the registered JSON Schema cannot be used for validation: " + e.getMessage();
            unusableSchemas.put(registered.id(), unusable);
            throw new RegistryValueException(registered.id(), registered.type(), unusable, e);
        }
        validators.put(registered.id(), validator);
        return validator;
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
            String text = value != null && !(value instanceof GenericRecord) ? value.toString() : null;
            if (text != null && !BatchFormatter.isAllowedInXml(text)) {
                // XML cannot hold such a character at all, so the field carries its UTF-8 bytes (#284).
                sb.append('<').append(field.name()).append(" format=\"base64\">");
                sb.append(BatchFormatter.toBase64(text));
            } else {
                sb.append('<').append(field.name()).append('>');
                if (value instanceof GenericRecord) {
                    sb.append(genericRecordToXml((GenericRecord) value));
                } else if (text != null) {
                    sb.append(BatchFormatter.escapeXml(text));
                }
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
