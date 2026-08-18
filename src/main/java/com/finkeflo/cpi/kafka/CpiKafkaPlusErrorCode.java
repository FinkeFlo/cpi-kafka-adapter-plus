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

import org.apache.kafka.common.errors.*;

public enum CpiKafkaPlusErrorCode {
    KP_PROD_001("KP-PROD-001", "producer.init.failed", "Producer init failed."),
    KP_PROD_002("KP-PROD-002", "producer.send.timeout", "Send timed out."),
    KP_PROD_003("KP-PROD-003", "producer.send.record_too_large", "Record too large."),
    KP_PROD_004("KP-PROD-004", "producer.send.serialization_failed", "Serialization failed."),
    KP_TXN_001("KP-TXN-001", "txn.fenced", "Producer fenced."),
    KP_TXN_002("KP-TXN-002", "txn.failed", "Transaction failed."),
    KP_SEC_001("KP-SEC-001", "security.authentication_failed", "Authentication failed."),
    KP_SEC_002("KP-SEC-002", "security.authorization_denied", "Authorization denied."),
    KP_CFG_001("KP-CFG-001", "config.invalid_topic", "Invalid topic."),
    KP_CFG_002("KP-CFG-002", "config.json_schema_validation_failed", "JSON schema validation failed."),
    KP_META_001("KP-META-001", "metadata.fetch_timeout", "Metadata fetch timed out."),
    KP_META_002("KP-META-002", "metadata.monitor_state_fault", "KAFKA-10902 monitor fault."),
    KP_DLQ_001("KP-DLQ-001", "dlq.delivery_failed", "DLQ delivery failed."),
    KP_SR_001("KP-SR-001", "schema_registry.failed", "Schema Registry failed."),
    KP_GEN_001("KP-GEN-001", "unclassified", "Unclassified error.");

    private final String code, operation, description;
    CpiKafkaPlusErrorCode(String c, String o, String d) { code=c; operation=o; description=d; }
    public String code() { return code; }
    public String operation() { return operation; }
    public String description() { return description; }
    @Override public String toString() { return code; }

    public static CpiKafkaPlusErrorCode fromThrowable(Throwable t) {
        if (t == null) return KP_GEN_001;
        if (KafkaErrorHelper.isMetadataMonitorFault(t)) return KP_META_002;
        for (Throwable c = t; c != null; c = (c.getCause() == c) ? null : c.getCause()) {
            if (c instanceof ProducerFencedException) return KP_TXN_001;
            if (c instanceof OutOfOrderSequenceException) return KP_TXN_002;
            if (c instanceof AuthenticationException) return KP_SEC_001;
            if (c instanceof AuthorizationException) return KP_SEC_002;
            if (c instanceof RecordTooLargeException) return KP_PROD_003;
            if (c instanceof InvalidTopicException) return KP_CFG_001;
            if (c instanceof SerializationException) return KP_PROD_004;
            if (c instanceof TimeoutException) {
                String m = c.getMessage();
                return (m != null && m.toLowerCase().contains("metadata")) ? KP_META_001 : KP_PROD_002;
            }
        }
        return KP_GEN_001;
    }
    public static CpiKafkaPlusErrorCode fromSendFailure(Throwable t, boolean dlq) { return dlq ? KP_DLQ_001 : fromThrowable(t); }
    public static CpiKafkaPlusErrorCode fromProducerInitFailure(Throwable t) { CpiKafkaPlusErrorCode c = fromThrowable(t); return c != KP_GEN_001 ? c : KP_PROD_001; }
    public static CpiKafkaPlusErrorCode fromJsonSchemaValidationFailure(Throwable t) { return KP_CFG_002; }
}
