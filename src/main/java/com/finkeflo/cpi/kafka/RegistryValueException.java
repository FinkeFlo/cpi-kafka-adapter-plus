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
 * A value that cannot be read with the schema the registry has for its ID: the payload does not fit
 * the schema type, or the type or schema is not supported. The record is at fault, not the registry,
 * so this is never transient.
 */
final class RegistryValueException extends RuntimeException {

    private final int schemaId;
    private final String schemaType;

    RegistryValueException(int schemaId, String schemaType, String reason) {
        this(schemaId, schemaType, reason, null);
    }

    RegistryValueException(int schemaId, String schemaType, String reason, Throwable cause) {
        super("Schema Registry value (schema ID " + schemaId + ", type " + schemaType + ") cannot be read: "
                + reason, cause);
        this.schemaId = schemaId;
        this.schemaType = schemaType;
    }

    int getSchemaId() { return schemaId; }
    String getSchemaType() { return schemaType; }
}
