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

/**
 * A record value as the sender reads it, with the schema it was written with, if known.
 */
final class DecodedValue {

    private static final DecodedValue NULL = new DecodedValue(null, null, null, null);

    private final String payload;
    private final Integer schemaId;
    private final String schemaType;
    private final JsonSchemaValidator registryValidator;

    DecodedValue(String payload, Integer schemaId, String schemaType, JsonSchemaValidator registryValidator) {
        this.payload = payload;
        this.schemaId = schemaId;
        this.schemaType = schemaType;
        this.registryValidator = registryValidator;
    }

    /** A value read without a schema. */
    static DecodedValue of(String payload) {
        return payload == null ? NULL : new DecodedValue(payload, null, null, null);
    }

    String payload() { return payload; }
    /** The ID from the Confluent header, or {@code null}. */
    Integer schemaId() { return schemaId; }
    /** The registry's {@code schemaType}, or {@code null} if the registry was not asked. */
    String schemaType() { return schemaType; }
    /** The registry JSON Schema to validate the payload against, or {@code null}. */
    JsonSchemaValidator registryValidator() { return registryValidator; }
}
