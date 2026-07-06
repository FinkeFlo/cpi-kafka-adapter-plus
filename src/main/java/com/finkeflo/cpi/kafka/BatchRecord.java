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
 * Value object representing a single record in a producer batch.
 * Used by {@link BatchParser} to pass parsed records to {@link ProducerBatchHelper}.
 */
public final class BatchRecord {

    private final String key;
    private final String value;

    public BatchRecord(String key, String value) {
        this.key = key;
        this.value = value;
    }

    /** Record key, may be null (Kafka round-robin partitioning). */
    public String getKey() { return key; }

    /** Record value, may be null (Kafka tombstone). */
    public String getValue() { return value; }
}
