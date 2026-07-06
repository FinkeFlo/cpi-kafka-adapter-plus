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

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages an internal KafkaProducer that sends failed records to a Dead Letter Queue topic.
 * Preserves the original record key, value, headers and adds error metadata as Kafka headers.
 */
public final class DlqProducerHelper implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DlqProducerHelper.class);

    private final String dlqTopic;
    private final KafkaProducer<byte[], byte[]> producer;

    public DlqProducerHelper(CpiKafkaPlusEndpoint endpoint) {
        this.dlqTopic = endpoint.getDlqTopic();
        LOG.info("Creating DLQ producer for topic '{}'", dlqTopic);

        try {
            Properties props = buildProducerProperties(endpoint);
            this.producer = BundleBackedClassLoader.withBundleClassLoader(DlqProducerHelper.class,
                    () -> new KafkaProducer<byte[], byte[]>(props,
                            new ByteArraySerializer(), new ByteArraySerializer()));
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] DLQ producer created OK for topic='{}'", dlqTopic);
        } catch (Exception e) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] DLQ producer creation FAILED for topic='{}': {}", dlqTopic, e.getMessage(), e);
            throw (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
        }
    }

    /**
     * Sends a failed consumer record to the DLQ topic with error metadata headers.
     * Original record headers are preserved.
     *
     * @param record     the original consumer record that failed processing
     * @param error      the exception that caused the failure
     * @param retryCount the number of retries attempted before sending to DLQ
     */
    public void sendToDlq(ConsumerRecord<byte[], byte[]> record, Exception error, int retryCount) {
        sendToDlq(record, error, retryCount, null);
    }

    /**
     * Sends a failed consumer record to the DLQ topic with error metadata headers.
     * Original record headers are preserved.
     *
     * @param record     the original consumer record that failed processing
     * @param error      the exception that caused the failure
     * @param retryCount the number of retries attempted before sending to DLQ
     * @param errorType  optional error classification ("PERMANENT" or "TRANSIENT"), null to omit
     */
    public void sendToDlq(ConsumerRecord<byte[], byte[]> record, Exception error,
                           int retryCount, String errorType) {
        ProducerRecord<byte[], byte[]> dlqRecord = new ProducerRecord<byte[], byte[]>(
                dlqTopic, null, record.key(), record.value());

        Headers headers = dlqRecord.headers();

        // Preserve original Kafka record headers
        if (record.headers() != null) {
            for (Header originalHeader : record.headers()) {
                headers.add(originalHeader);
            }
        }

        // Add DLQ error metadata headers
        addHeader(headers, "CpiKafkaPlusDlqError",
                error.getMessage() != null ? error.getMessage() : error.getClass().getName());
        addHeader(headers, "CpiKafkaPlusDlqOriginalTopic", record.topic());
        addHeader(headers, "CpiKafkaPlusDlqOriginalPartition", String.valueOf(record.partition()));
        addHeader(headers, "CpiKafkaPlusDlqOriginalOffset", String.valueOf(record.offset()));
        addHeader(headers, "CpiKafkaPlusDlqTimestamp", formatIsoTimestamp());
        addHeader(headers, "CpiKafkaPlusDlqRetryCount", String.valueOf(retryCount));
        if (errorType != null) {
            addHeader(headers, "CpiKafkaPlusDlqErrorType", errorType);
        }

        LOG.info("Sending failed record to DLQ topic '{}' (originalTopic='{}', partition={}, offset={}, retries={}, error='{}')",
                dlqTopic, record.topic(), record.partition(), record.offset(), retryCount, error.getMessage());

        try {
            producer.send(dlqRecord).get();
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] DLQ send OK for offset={} partition={}",
                    record.offset(), record.partition());
        } catch (Exception e) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] DLQ send FAILED for offset={} partition={}: {}",
                    record.offset(), record.partition(), e.getMessage(), e);
            throw new RuntimeException("Failed to send record to DLQ topic '" + dlqTopic + "'", e);
        }
    }

    /**
     * Sends a record that failed deserialization (poison-pill) to the DLQ topic.
     * Uses the raw key/value bytes from {@link org.apache.kafka.common.errors.RecordDeserializationException}
     * (KIP-1036) so the original payload is preserved even though it could not be deserialized.
     *
     * @param tp              the source topic-partition of the failed record
     * @param offset          the offset of the failed record
     * @param key             raw key bytes (may be null)
     * @param value           raw value bytes (may be null)
     * @param originalHeaders headers of the failed record (may be null)
     * @param timestamp       record timestamp (or {@link org.apache.kafka.clients.consumer.ConsumerRecord#NO_TIMESTAMP})
     * @param cause           the deserialization exception
     */
    public void sendDeserializationFailure(TopicPartition tp, long offset,
                                           byte[] key, byte[] value,
                                           Headers originalHeaders, long timestamp,
                                           Throwable cause) {
        Long ts = (timestamp >= 0L) ? Long.valueOf(timestamp) : null;
        ProducerRecord<byte[], byte[]> dlqRecord = new ProducerRecord<byte[], byte[]>(
                dlqTopic, null, ts, key, value);

        Headers headers = dlqRecord.headers();
        if (originalHeaders != null) {
            for (Header originalHeader : originalHeaders) {
                headers.add(originalHeader);
            }
        }

        String causeClass = cause != null ? cause.getClass().getName() : "unknown";
        String causeMsg = (cause != null && cause.getMessage() != null)
                ? cause.getMessage() : causeClass;
        Throwable root = cause;
        while (root != null && root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String rootClass = (root != null) ? root.getClass().getName() : causeClass;
        String rootMsg = (root != null && root.getMessage() != null) ? root.getMessage() : "";

        addHeader(headers, "CpiKafkaPlusDlqError", causeMsg);
        addHeader(headers, "CpiKafkaPlusDlqErrorClass", causeClass);
        addHeader(headers, "CpiKafkaPlusDlqCauseClass", rootClass);
        addHeader(headers, "CpiKafkaPlusDlqCauseMessage", rootMsg);
        addHeader(headers, "CpiKafkaPlusDlqOriginalTopic", tp.topic());
        addHeader(headers, "CpiKafkaPlusDlqOriginalPartition", String.valueOf(tp.partition()));
        addHeader(headers, "CpiKafkaPlusDlqOriginalOffset", String.valueOf(offset));
        addHeader(headers, "CpiKafkaPlusDlqTimestamp", formatIsoTimestamp());
        addHeader(headers, "CpiKafkaPlusDlqErrorType", "DESERIALIZATION");

        LOG.info("Sending poison-pill record to DLQ topic '{}' (originalTopic='{}', partition={}, offset={}, error='{}')",
                dlqTopic, tp.topic(), tp.partition(), offset, causeMsg);

        try {
            producer.send(dlqRecord).get();
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] DLQ deser-failure send OK for offset={} partition={}",
                    offset, tp.partition());
        } catch (Exception e) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] DLQ deser-failure send FAILED for offset={} partition={}: {}",
                    offset, tp.partition(), e.getMessage(), e);
            throw new RuntimeException("Failed to send poison-pill record to DLQ topic '" + dlqTopic + "'", e);
        }
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing DLQ producer for topic '{}'", dlqTopic);
        try {
            producer.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            LOG.warn("Error closing DLQ producer: {}", e.getMessage(), e);
        }
    }

    private static void addHeader(Headers headers, String key, String value) {
        if (value != null) {
            headers.add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String formatIsoTimestamp() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(new Date());
    }

    private static Properties buildProducerProperties(CpiKafkaPlusEndpoint endpoint) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, endpoint.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        // Unique client.id per consumer group to avoid metric collision on the broker
        String clientId = "cpi-kafka-plus-dlq";
        if (endpoint.getGroupId() != null && !endpoint.getGroupId().isEmpty()) {
            clientId = clientId + "-" + endpoint.getGroupId();
        }
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        // Use DLQ-specific credentials if configured, otherwise reuse consumer credentials
        SecurityConfigHelper.configureSecurityProperties(props, endpoint);
        String dlqAlias = endpoint.getDlqCredentialAlias();
        if (dlqAlias != null && !dlqAlias.trim().isEmpty()) {
            SecurityConfigHelper.overrideSaslCredentials(props, endpoint.getSaslMechanism(), dlqAlias);
        }

        return props;
    }
}
