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

import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import com.networknt.schema.resource.DisallowSchemaLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates JSON messages against a JSON Schema (draft-07).
 * The compiled schema is reused across poll cycles.
 */
public final class JsonSchemaValidator {

    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaValidator.class);

    private final JsonSchema schema;
    private final ObjectMapper objectMapper;

    public JsonSchemaValidator(String jsonSchemaString) {
        if (jsonSchemaString == null || jsonSchemaString.trim().isEmpty()) {
            throw new IllegalArgumentException("JSON Schema string must not be null or empty");
        }
        this.objectMapper = new ObjectMapper();
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        JsonNode schemaNode = parseSchemaDocument(objectMapper, jsonSchemaString);
        try {
            this.schema = factory.getSchema(schemaNode);
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] JSON Schema validator compiled successfully");
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON Schema: " + e.getMessage(), e);
        }
    }

    private JsonSchemaValidator(JsonSchema schema, ObjectMapper objectMapper) {
        this.schema = schema;
        this.objectMapper = objectMapper;
    }

    /**
     * A validator for a JSON Schema from the Schema Registry. Unlike the inline schema, its
     * draft follows {@code $schema} (draft-04 to 2020-12; without one draft-07, the default of
     * Confluent's JSON Schema serializer), and it may load no other schema: a {@code $ref} to a
     * remote or classpath URI fails here, when the schema is compiled, instead of fetching anything.
     *
     * @throws IllegalArgumentException if the schema is not one JSON document, names an unknown
     *                                  {@code $schema}, or cannot be compiled
     */
    static JsonSchemaValidator forRegistrySchema(String schemaText) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode schemaNode = parseSchemaDocument(mapper, schemaText);
        SpecVersion.VersionFlag dialect = registryDialect(schemaNode);
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(dialect,
                builder -> builder.schemaLoaders(loaders -> loaders.add(DisallowSchemaLoader.getInstance())));
        try {
            JsonSchema compiled = factory.getSchema(schemaNode);
            compiled.initializeValidators();
            return new JsonSchemaValidator(compiled, mapper);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON Schema: " + e.getMessage(), e);
        }
    }

    private static SpecVersion.VersionFlag registryDialect(JsonNode schemaNode) {
        JsonNode dialect = schemaNode.isObject() ? schemaNode.get("$schema") : null;
        if (dialect == null) {
            return SpecVersion.VersionFlag.V7;
        }
        if (!dialect.isTextual()) {
            throw new IllegalArgumentException("Invalid JSON Schema: $schema must be a string, got: " + dialect);
        }
        String id = canonicalDialectId(dialect.asText());
        for (SpecVersion.VersionFlag flag : SpecVersion.VersionFlag.values()) {
            if (canonicalDialectId(flag.getId()).equals(id)) {
                return flag;
            }
        }
        throw new IllegalArgumentException("Invalid JSON Schema: unsupported $schema '" + dialect.asText()
                + "'; supported are draft-04, draft-06, draft-07, 2019-09 and 2020-12");
    }

    /**
     * The meta-schema URI without the scheme and an empty fragment, so that the forms found in practice,
     * such as {@code https://json-schema.org/draft-07/schema} for {@code http://json-schema.org/draft-07/schema#},
     * name the same draft.
     */
    private static String canonicalDialectId(String uri) {
        String id = uri.trim();
        if (id.endsWith("#")) {
            id = id.substring(0, id.length() - 1);
        }
        return id.replaceFirst("^https?://", "");
    }

    /**
     * Parses the schema text as exactly one JSON document, without compiling it. Used at start-up as
     * well: compiling could fetch a remote {@code $ref}, which a deployment must not do.
     */
    static JsonNode parseSchemaDocument(ObjectMapper mapper, String jsonSchemaString) {
        try (JsonParser parser = mapper.getFactory().createParser(jsonSchemaString)) {
            JsonNode schemaNode = mapper.readTree(parser);
            if (parser.nextToken() != null) {
                throw new IllegalArgumentException(
                    "Invalid JSON Schema: found content after the first JSON value. "
                    + "Only a single JSON Schema document is supported - combine multiple "
                    + "schemas using 'oneOf', 'anyOf', or 'allOf' instead of concatenating "
                    + "them with a comma.");
            }
            return schemaNode;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON Schema: " + e.getMessage(), e);
        }
    }

    /**
     * Validates a JSON string against the schema.
     *
     * @param json the JSON string to validate
     * @return null if valid, or an error message string describing all violations
     */
    public String validate(String json) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        try {
            JsonNode jsonNode = objectMapper.readTree(json);
            Set<ValidationMessage> errors = schema.validate(jsonNode);
            if (errors.isEmpty()) {
                return null;
            }
            StringBuilder sb = new StringBuilder("JSON Schema validation failed: ");
            String delimiter = "";
            for (ValidationMessage error : errors) {
                sb.append(delimiter).append(error.getMessage());
                delimiter = "; ";
            }
            return sb.toString();
        } catch (Exception e) {
            return "JSON parsing failed: " + e.getMessage();
        }
    }
}
