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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
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
        try {
            JsonNode schemaNode = objectMapper.readTree(jsonSchemaString);
            this.schema = factory.getSchema(schemaNode);
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] JSON Schema validator compiled successfully");
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
