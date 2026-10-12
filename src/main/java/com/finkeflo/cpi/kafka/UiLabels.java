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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The labels the CPI channel editor shows for the adapter's parameters, so that a deployment error
 * names a field the way the user finds it (#255). It holds every parameter a channel of metadata
 * version 1.4.0 offers, with the label of the direction that offers it; a parameter both directions
 * offer has the same label in both. {@code UiLabelsTest} compares them with the metadata, so a
 * renamed label cannot leave the messages behind.
 *
 * <p>A start check rejects a value as {@code field(param) + " must ..., got: " + value}, with a text
 * value in {@link #quote quotes}. Package-private.
 */
final class UiLabels {

    private static final Map<String, String> LABELS = new HashMap<>();

    static {
        // Both directions
        put("bootstrapServers", "Bootstrap Servers");
        put("topic", "Topic");
        put("securityProtocol", "Security Protocol");
        put("saslMechanism", "SASL Mechanism");
        put("credentialAlias", "Credential Alias");
        put("sslKeystoreAlias", "SSL Keystore Alias");
        put("jsonSchemaValidation", "Enable JSON Schema Validation");
        put("jsonSchema", "JSON Schema");
        put("jsonSchemaReportError", "Report Validation Failures in CPI Monitoring");
        put("schemaRegistryEnabled", "Enable Schema Registry");
        put("schemaRegistryUrl", "Schema Registry URL");
        put("schemaRegistryCredentialAlias", "Schema Registry Credential Alias");
        put("writeMplErrorAttachment", "Write MPL Error Attachment");
        // Sender
        put("groupId", "Consumer Group ID");
        put("consumptionMode", "Consumption Mode");
        put("autoOffsetReset", "Auto Offset Reset");
        put("commitStrategy", "Offset Commit Strategy");
        put("pollingIntervalSeconds", "Polling Interval (Seconds)");
        put("maxPollRecords", "Max Poll Records");
        put("batchTimeout", "Poll Timeout (ms)");
        put("maxPartitionFetchSizeKb", "Max Fetch Size per Partition (KB)");
        put("fetchMinBytes", "Fetch Min Bytes");
        put("fetchMaxWaitMs", "Fetch Max Wait (ms)");
        put("drainEnabled", "Drain Backlog");
        put("minBacklogToDrain", "Min Backlog to Drain");
        put("batchMode", "Batch Mode");
        put("batchSize", "Max Records per IFlow Run (MPL)");
        put("batchOutputFormat", "Batch Output Format");
        put("avroOutputFormat", "Avro Output Format");
        put("avroValueDeserialization", "Deserialize Values as Avro");
        put("errorHandling", "Error Handling");
        put("retryDelaySeconds", "Retry Delay (Seconds)");
        put("dlqEnabled", "Enable Dead Letter Queue");
        put("dlqTopic", "Dead Letter Topic");
        put("dlqMaxRetries", "Retries before DLQ (additional attempts)");
        put("dlqCredentialAlias", "DLQ SASL Credential Alias");
        put("retryOnlyTransientErrors", "Retry Transient Errors Only");
        put("autoPauseEnabled", "Enable Auto-Pause");
        put("autoPauseErrorThreshold", "Consecutive Error Threshold");
        put("autoPauseCooldownSeconds", "Initial Pause Duration (sec)");
        // Receiver
        put("producerBatchMode", "Batch Send Mode");
        put("acks", "Acknowledgments");
        put("enableIdempotence", "Enable Idempotent Producer");
        put("deliveryTimeoutSeconds", "Delivery Timeout (Seconds)");
        put("allowedHeaders", "Allowed Headers");
        put("enableTransactions", "Enable Transactions");
        put("transactionalIdPrefix", "Transactional ID Prefix");
        put("maxConcurrentTransactions", "Max Concurrent Transactions");
        put("compressionType", "Compression Type");
        put("maxRequestSizeKb", "Max Request Size (KB)");
        put("producerBatchSizeKb", "Producer Batch Size (KB)");
        put("bufferMemoryKb", "Buffer Memory (KB)");
        put("autoRegisterSchemas", "Auto Register Schemas");
        put("subjectNameStrategy", "Subject Name Strategy");
        put("avroValueSerialization", "Serialize Values as Avro");
        put("producerRetryMaxAttempts", "Send Attempts (total, 1 = off)");
        put("producerRetryDelaySeconds", "Delay Between Attempts (Seconds)");
        put("producerRetryOnlyTransientErrors", "Retry Transient Errors Only");
        put("producerRetryTotalBudgetSeconds", "Total Retry Budget (Seconds)");
        put("diagnosticsLevel", "Diagnostics Level");
    }

    private UiLabels() {
    }

    private static void put(String param, String label) {
        LABELS.put(param, label);
    }

    /** The label of {@code param}, or the parameter name itself for one no channel offers. */
    static String label(String param) {
        String label = LABELS.get(param);
        return label != null ? label : param;
    }

    /** {@code 'Max Poll Records' (maxPollRecords)}: the field as the UI shows it, then its parameter. */
    static String field(String param) {
        return "'" + label(param) + "' (" + param + ")";
    }

    /** A text value in quotes, so that an empty value and spaces around it show; null is empty. */
    static String quote(String value) {
        return "'" + (value != null ? value : "") + "'";
    }

    /** Every parameter with its label. */
    static Map<String, String> all() {
        return Collections.unmodifiableMap(LABELS);
    }
}
