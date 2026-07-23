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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.support.DefaultProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Producer - sends messages from CPI IFlow to Kafka.
 * Receiver direction in CPI terminology (CPI receives from IFlow, sends to external system).
 */
public class CpiKafkaPlusProducer extends DefaultProducer {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusProducer.class);
    private static final int MAX_CONSECUTIVE_SEND_FAILURES = 3;
    private static final Duration TXN_PRODUCER_CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private final CpiKafkaPlusEndpoint endpoint;
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private AvroSerializerHelper avroHelper;
    private AdapterTracingHelper tracingHelper;
    private JsonSchemaValidator jsonSchemaValidator;

    private java.util.concurrent.Semaphore txnSlotSemaphore;
    private boolean[] txnSlotInUse;
    private String resolvedMemberSuffix;

    private volatile boolean initialized = false;
    private volatile boolean helpersInitialized = false;
    private volatile int consecutiveSendFailures = 0;
    private volatile int consecutiveTxnSendFailures = 0;
    private volatile int consecutiveInitFailures = 0;
    private volatile Throwable lastInitException = null;

    public CpiKafkaPlusProducer(CpiKafkaPlusEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    @Override
    protected void doStart() throws Exception {
        // Fail-fast: validate shared configuration (Schema Registry, JSON Schema, SASL)
        endpoint.validateConfiguration();

        super.doStart();
        LOG.info("[CPI-KAFKA-PLUS-DIAG] Starting CPI Kafka Producer for topic '{}' (lazy init — Kafka resources created on first send)",
                endpoint.getEffectiveTopic());

        tracingHelper = new AdapterTracingHelper(endpoint);

        if (endpoint.isEnableTransactions()) {
            if (endpoint.getTransactionalIdPrefix() == null || endpoint.getTransactionalIdPrefix().trim().isEmpty()) {
                throw new IllegalArgumentException("transactionalIdPrefix is required when enableTransactions is true");
            }
            if (!endpoint.isEnableIdempotence()) {
                throw new IllegalArgumentException(
                        "enableTransactions requires enableIdempotence=true "
                        + "(Kafka transactional producers cannot disable idempotence)");
            }
            int slots = endpoint.getMaxConcurrentTransactions();
            if (slots < 1) {
                throw new IllegalArgumentException(
                        "maxConcurrentTransactions must be >= 1 when enableTransactions is true (was: " + slots + ")");
            }
            // Resolved once at startup and reused for every transactional.id below — avoids
            // re-reading env vars per exchange and prevents a literal "null" segment in the
            // transactional.id, which would cause fencing collisions between worker nodes that both
            // fail to resolve a suffix. If neither CF_INSTANCE_INDEX nor HOSTNAME is available, fall
            // back to a random-but-unique suffix rather than hard-failing startup: this keeps the
            // adapter usable in environments without those env vars, at the cost of the
            // transactional.id no longer being stable across restarts on the same node (so a
            // crashed producer's old transactional.id will not be actively fenced by the new one —
            // it will simply expire via transaction.timeout.ms).
            resolvedMemberSuffix = CpiKafkaPlusConsumer.resolveStaticMemberSuffix();
            if (resolvedMemberSuffix == null) {
                resolvedMemberSuffix = "r" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 12);
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] Neither CF_INSTANCE_INDEX nor HOSTNAME is set; using a random "
                        + "per-startup member suffix '{}' for transactional.id. This avoids fencing collisions "
                        + "between nodes but means the transactional.id will change on every restart.",
                        resolvedMemberSuffix);
            }
            txnSlotSemaphore = new java.util.concurrent.Semaphore(slots, true);
            txnSlotInUse = new boolean[slots];
            LOG.info("[CPI-KAFKA-PLUS-DIAG] Transactional batching enabled with max {} concurrent transactions. "
                    + "Prefix: {}, memberSuffix: {}",
                    slots, endpoint.getTransactionalIdPrefix(), resolvedMemberSuffix);
        }
    }

    @Override
    protected void doStop() throws Exception {
        LOG.info("[CPI-KAFKA-PLUS-DIAG] Stopping CPI Kafka Producer for topic '{}'", endpoint.getEffectiveTopic());
        initialized = false;
        helpersInitialized = false;
        consecutiveSendFailures = 0;
        consecutiveTxnSendFailures = 0;
        consecutiveInitFailures = 0;
        closeProducerQuietly();
        jsonSchemaValidator = null;
        if (tracingHelper != null) {
            tracingHelper.publishConnectionStatus(false, null);
        }
        super.doStop();
    }

    private void ensureInitialized() {
        if (initialized) {
            return;
        }
        synchronized (this) {
            if (initialized) {
                return;
            }

            LOG.info("[CPI-KAFKA-PLUS-DIAG] ensureInitialized: creating Kafka resources for topic='{}'",
                    endpoint.getEffectiveTopic());

            if (!createKafkaProducer()) {
                return;
            }
            if (!ensureHelpersInitializedLocked()) {
                return;
            }

            consecutiveInitFailures = 0;
            lastInitException = null;
            initialized = true;
            tracingHelper.publishConnectionStatus(true, null);
            LOG.info("[CPI-KAFKA-PLUS-DIAG] ensureInitialized: Producer READY for topic='{}'",
                    endpoint.getEffectiveTopic());
        }
    }

    /**
     * Initializes the serialization/validation helpers (Avro, JSON Schema) independently of the
     * shared (non-transactional) KafkaProducer. This lets a purely transactional producer
     * (enableTransactions=true with a batch mode) become usable without ever creating the unused
     * shared producer connection.
     */
    private void ensureHelpersInitialized() {
        if (helpersInitialized) {
            return;
        }
        synchronized (this) {
            ensureHelpersInitializedLocked();
        }
    }

    /** Must be called while holding the {@code this} monitor. @return true if helpers are ready */
    private boolean ensureHelpersInitializedLocked() {
        if (helpersInitialized) {
            return true;
        }
        if (!createProducerHelpers()) {
            return false;
        }
        helpersInitialized = true;
        return true;
    }

    /** @return true if the KafkaProducer was created successfully */
    private boolean createKafkaProducer() {
        try {
            Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] ensureInitialized: producer properties built, security={}, sasl={}",
                    endpoint.getSecurityProtocol(), endpoint.getSaslMechanism());
            kafkaProducer = BundleBackedClassLoader.withBundleClassLoader(getClass(),
                    () -> new KafkaProducer<>(props,
                            new ByteArraySerializer(), new ByteArraySerializer()));
            return true;
        } catch (Throwable e) {
            logInitFailure("KafkaProducer", e);
            closeProducerQuietly();
            lastInitException = e;
            return false;
        }
    }

    /** @return true if all helpers were created successfully */
    private boolean createProducerHelpers() {
        try {
            if (endpoint.isSchemaRegistryEnabled()
                    && endpoint.isAvroValueSerialization()) {
                avroHelper = BundleBackedClassLoader.withBundleClassLoader(getClass(),
                        () -> new AvroSerializerHelper(endpoint));
            }
            if (endpoint.isJsonSchemaValidation()) {
                jsonSchemaValidator = new JsonSchemaValidator(endpoint.getJsonSchema());
                LOG.info("[CPI-KAFKA-PLUS-DIAG] JSON Schema validation enabled for outbound messages");
            }
            return true;
        } catch (Throwable e) {
            logInitFailure("helpers", e);
            if (avroHelper != null) {
                try { avroHelper.close(); } catch (Exception ignored) { }
                avroHelper = null;
            }
            jsonSchemaValidator = null;
            closeProducerQuietly();
            lastInitException = e;
            return false;
        }
    }

    private void logInitFailure(String component, Throwable e) {
        consecutiveInitFailures++;
        if (consecutiveInitFailures >= KafkaErrorHelper.INIT_FAILURE_ESCALATION_THRESHOLD) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] ensureInitialized: FAILED to create {} ({} consecutive failures): {}",
                    component, consecutiveInitFailures, e.getMessage(), e);
        } else {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] ensureInitialized: FAILED to create {} (attempt {}): {}",
                    component, consecutiveInitFailures, e.getMessage(), e);
        }
        tracingHelper.publishConnectionStatus(false, KafkaErrorHelper.wrapIfError(e));
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String batchMode = endpoint.getProducerBatchMode();
        // When every exchange goes through the transactional path (enableTransactions with a batch
        // mode), the shared non-transactional KafkaProducer is never used and must not gate
        // processing — only the serialization/validation helpers are needed here.
        boolean transactionalOnlyPath = endpoint.isEnableTransactions() && !"NONE".equalsIgnoreCase(batchMode);

        if (transactionalOnlyPath) {
            ensureHelpersInitialized();
        } else {
            ensureInitialized();
            if (kafkaProducer == null) {
                String msg = "Kafka producer not initialized — init failed, will retry on next exchange";
                if (lastInitException != null) {
                    StringBuilder chain = new StringBuilder();
                    Throwable t = lastInitException;
                    while (t != null) {
                        if (chain.length() > 0) {
                            chain.append(" -> ");
                        }
                        chain.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage());
                        t = t.getCause();
                    }
                    msg += ". Cause: " + chain;
                }
                throw new IllegalStateException(msg, lastInitException);
            }
        }

        Message in = exchange.getIn();

        // Determine topic - header overrides config
        String topic = resolveTopic(exchange, in.getHeader("CamelKafkaTopic", String.class));
        if (topic == null || topic.isEmpty()) {
            topic = resolveTopic(exchange, endpoint.getEffectiveTopic());
        }

        if (!"NONE".equalsIgnoreCase(batchMode)) {
            processBatch(exchange, in, topic, batchMode);
        } else {
            processSingle(exchange, in, topic);
        }
    }

    private void processBatch(Exchange exchange, Message in, String topic,
                               String batchMode) throws Exception {
        if (jsonSchemaValidator != null) {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] JSON Schema validation is skipped in batch mode "
                    + "(producerBatchMode={})", batchMode);
        }

        java.util.List<BatchRecord> records = parseBatchRecords(in, batchMode);

        String fallbackKey = in.getHeader("kafka.KEY", String.class);
        Integer partition = parsePartitionHeader(in);
        Long timestamp = in.getHeader("kafka.OVERRIDE_TIMESTAMP", Long.class);

        LOG.info("[CPI-KAFKA-PLUS-DIAG] Batch send: {} records to topic '{}' (mode={})",
                records.size(), topic, batchMode);

        ProducerBatchHelper.ByteSerializer valueSerializer = buildBatchValueSerializer();

        if (endpoint.isEnableTransactions()) {
            sendTransactionalBatch(in, topic, batchMode, records, fallbackKey, partition, timestamp, valueSerializer);
        } else {
            try {
                ProducerBatchHelper.BatchSendResult result = ProducerBatchHelper.sendBatch(
                        kafkaProducer, records, topic, fallbackKey, partition, timestamp,
                        in, this::addRecordHeaders, valueSerializer, null);

                ProducerBatchHelper.setResponseHeadersAndBody(in, topic, batchMode, result);
                recordSendSuccess();
            } catch (Exception e) {
                handleSendFailure(e);
                throw new RuntimeException(
                        "Failed to send batch to Kafka topic '" + topic + "': " + e.getMessage(), e);
            }
        }
    }

    private void sendTransactionalBatch(Message in, String topic, String batchMode,
                                        java.util.List<BatchRecord> records, String fallbackKey,
                                        Integer partition, Long timestamp,
                                        ProducerBatchHelper.ByteSerializer valueSerializer) throws Exception {
        int slotId = -1;
        try {
            txnSlotSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for a transaction slot", e);
        }

        KafkaProducer<byte[], byte[]> txnProducer = null;
        try {
            synchronized (txnSlotInUse) {
                for (int i = 0; i < txnSlotInUse.length; i++) {
                    if (!txnSlotInUse[i]) {
                        txnSlotInUse[i] = true;
                        slotId = i;
                        break;
                    }
                }
            }
            if (slotId == -1) {
                throw new IllegalStateException("Acquired semaphore but no slot was free. This is a bug.");
            }

            // resolvedMemberSuffix is resolved once (fail-fast) in doStart() — never null here.
            String transactionalId = endpoint.getTransactionalIdPrefix() + "-" + resolvedMemberSuffix + "-" + slotId;

            java.util.Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);
            props.put(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

            txnProducer = BundleBackedClassLoader.withBundleClassLoader(getClass(),
                    () -> new KafkaProducer<>(props,
                            new org.apache.kafka.common.serialization.ByteArraySerializer(),
                            new org.apache.kafka.common.serialization.ByteArraySerializer()));

            txnProducer.initTransactions();
            txnProducer.beginTransaction();

            ProducerBatchHelper.BatchSendResult result = ProducerBatchHelper.sendBatch(
                    txnProducer, records, topic, fallbackKey, partition, timestamp,
                    in, this::addRecordHeaders, valueSerializer, null);

            txnProducer.commitTransaction();

            ProducerBatchHelper.setResponseHeadersAndBody(in, topic, batchMode, result);
            recordTxnSendSuccess();

        } catch (Exception e) {
            if (txnProducer != null) {
                try {
                    txnProducer.abortTransaction();
                } catch (Exception abortEx) {
                    LOG.warn("[CPI-KAFKA-PLUS-DIAG] Failed to abort transaction for slot {}: {}", slotId, abortEx.getMessage(), abortEx);
                    e.addSuppressed(abortEx);
                }
            }
            handleTxnSendFailure(e);
            throw new RuntimeException("Failed to send transactional batch to Kafka topic '" + topic + "': " + e.getMessage(), e);
        } finally {
            if (txnProducer != null) {
                try {
                    // Bounded close — an unreachable broker must not hang here indefinitely, since
                    // the transaction slot below is only released once close() returns. Without a
                    // timeout, a broker outage could permanently exhaust all txn slots (deadlock).
                    txnProducer.close(TXN_PRODUCER_CLOSE_TIMEOUT);
                } catch (Exception closeEx) {
                    LOG.warn("[CPI-KAFKA-PLUS-DIAG] Failed to close transactional producer for slot {}: {}", slotId, closeEx.getMessage(), closeEx);
                }
            }
            if (slotId != -1) {
                synchronized (txnSlotInUse) {
                    txnSlotInUse[slotId] = false;
                }
            }
            txnSlotSemaphore.release();
        }
    }

    private java.util.List<BatchRecord> parseBatchRecords(Message in, String batchMode) {
        String body = in.getBody(String.class);
        if ("JSON_ARRAY".equalsIgnoreCase(batchMode)) {
            return BatchParser.parseJson(body);
        } else if ("XML_LIST".equalsIgnoreCase(batchMode)) {
            return BatchParser.parseXml(body);
        }
        throw new IllegalArgumentException(
                "Unknown producerBatchMode: " + batchMode
                + ". Supported: NONE, JSON_ARRAY, XML_LIST");
    }

    private ProducerBatchHelper.ByteSerializer buildBatchValueSerializer() {
        if (avroHelper != null && endpoint.isAvroValueSerialization()) {
            final AvroSerializerHelper helper = avroHelper;
            return new ProducerBatchHelper.ByteSerializer() {
                @Override
                public byte[] serialize(String t, String data) {
                    return helper.serialize(t, data);
                }
            };
        }
        return null;
    }

    private void processSingle(Exchange exchange, Message in, String topic) throws Exception {
        // Determine key from kafka.KEY header
        String keyStr = in.getHeader("kafka.KEY", String.class);

        // JSON Schema validation of outbound message (if enabled)
        if (jsonSchemaValidator != null) {
            String bodyStr = in.getBody(String.class);
            if (bodyStr != null) {
                String validationError = jsonSchemaValidator.validate(bodyStr);
                if (validationError != null) {
                    if (endpoint.isJsonSchemaReportError()) {
                        tracingHelper.traceOutbound(exchange,
                                bodyStr.getBytes(StandardCharsets.UTF_8));
                    }
                    throw new RuntimeException("Outbound " + validationError);
                }
            }
        }

        // Serialize value
        byte[] value = serializeValue(topic, in);

        // Serialize key
        byte[] key = serializeKey(topic, keyStr);

        // Determine partition (optional)
        Integer partition = parsePartitionHeader(in);

        // Determine timestamp (optional)
        Long timestamp = in.getHeader("kafka.OVERRIDE_TIMESTAMP", Long.class);

        // Build ProducerRecord
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, partition, timestamp, key, value);

        // Map exchange headers to Kafka record headers
        addRecordHeaders(record, in);

        // Trace outbound
        if (value != null) {
            tracingHelper.traceOutbound(exchange, value);
        }

        // Send synchronously to ensure delivery before IFlow continues
        try {
            Future<RecordMetadata> future = kafkaProducer.send(record);
            RecordMetadata metadata = future.get();

            in.setHeader("SAP_Receiver", metadata.topic());
            in.setHeader("CamelKafkaTopic", metadata.topic());
            in.setHeader("CamelKafkaPartition", metadata.partition());
            in.setHeader("CamelKafkaOffset", metadata.offset());
            in.setHeader("CamelKafkaTimestamp", metadata.timestamp());

            recordSendSuccess();
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] Message sent to topic '{}' partition {} offset {}",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            handleSendFailure(e);
            throw new RuntimeException(
                    "Failed to send message to Kafka topic '" + topic + "': " + e.getMessage(), e);
        }
    }

    private Integer parsePartitionHeader(Message in) {
        String partitionHeader = in.getHeader("kafka.PARTITION_KEY", String.class);
        if (partitionHeader != null && !partitionHeader.isEmpty()) {
            try {
                return Integer.parseInt(partitionHeader);
            } catch (NumberFormatException e) {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] Invalid partition header value '{}', using default partitioning: {}",
                        partitionHeader, e.getMessage());
            }
        }
        return null;
    }

    static String resolveTopic(Exchange exchange, String topicCandidate) {
        if (topicCandidate == null) {
            return null;
        }

        String trimmed = topicCandidate.trim();
        if (trimmed.isEmpty() || !trimmed.contains("${")) {
            return trimmed;
        }

        String normalized = trimmed
                .replace("${property.", "${exchangeProperty.")
                .replace("${property[", "${exchangeProperty[");
        String resolved = exchange.getContext()
                .resolveLanguage("simple")
                .createExpression(normalized)
                .evaluate(exchange, String.class);

        if (resolved == null || resolved.trim().isEmpty() || resolved.contains("${")) {
            throw new IllegalArgumentException(
                    "Topic expression '" + topicCandidate + "' could not be resolved to a valid topic name.");
        }
        return resolved.trim();
    }

    private byte[] serializeValue(String topic, Message message) {
        byte[] bodyBytes = message.getBody(byte[].class);
        if (bodyBytes == null) {
            String bodyStr = message.getBody(String.class);
            if (bodyStr != null) {
                bodyBytes = bodyStr.getBytes(StandardCharsets.UTF_8);
            }
        }

        if (bodyBytes == null) {
            return null;
        }

        if (endpoint.isSchemaRegistryEnabled() && endpoint.isAvroValueSerialization() && avroHelper != null) {
            String jsonData = new String(bodyBytes, StandardCharsets.UTF_8);
            return avroHelper.serialize(topic, jsonData);
        }

        return bodyBytes;
    }

    private byte[] serializeKey(String topic, String keyStr) {
        if (keyStr == null || keyStr.isEmpty()) {
            return null;
        }

        return keyStr.getBytes(StandardCharsets.UTF_8);
    }

    private void addRecordHeaders(ProducerRecord<byte[], byte[]> record, Message message) {
        Map<String, Object> headers = message.getHeaders();
        String allowedHeadersPattern = endpoint.getAllowedHeaders();
        
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            String name = entry.getKey();
            // Skip internal Camel headers and our kafka.* control headers
            if (name.startsWith("Camel") || name.startsWith("org.apache.camel")
                    || name.startsWith("kafka.") || name.startsWith("CpiKafkaPlus")) {
                continue;
            }
            
            if (!HeaderFilterStrategy.isHeaderAllowed(name, allowedHeadersPattern)) {
                continue;
            }
            
            Object val = entry.getValue();
            if (val != null) {
                record.headers().add(name, val.toString().getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private void recordSendSuccess() {
        boolean wasRecovering = consecutiveSendFailures > 0;
        consecutiveSendFailures = 0;
        if (wasRecovering) {
            tracingHelper.publishConnectionStatus(true, null);
            LOG.info("[CPI-KAFKA-PLUS-DIAG] Send recovered after previous failures for topic='{}'",
                    endpoint.getEffectiveTopic());
        }
    }

    private void handleSendFailure(Exception e) {
        consecutiveSendFailures++;
        tracingHelper.publishConnectionStatus(false, e);

        Throwable cause = KafkaErrorHelper.extractKafkaCause(e);
        if (KafkaErrorHelper.isFatalKafkaException(cause)) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] Fatal Kafka exception, triggering reconnect: {}",
                    cause.getClass().getSimpleName());
            triggerReconnect();
        } else if (consecutiveSendFailures >= MAX_CONSECUTIVE_SEND_FAILURES) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] {} consecutive send failures, triggering reconnect",
                    consecutiveSendFailures);
            triggerReconnect();
        }
    }

    /**
     * Success/failure tracking for the transactional batch path, kept entirely separate from
     * {@link #recordSendSuccess()}/{@link #handleSendFailure(Exception)}. Each transactional batch
     * uses its own short-lived KafkaProducer (created and closed per call), so there is nothing to
     * "reconnect" here — and, critically, a failure in this path must never close or reset the
     * shared, independent {@link #kafkaProducer} used by the non-transactional send path (and vice
     * versa a transactional success must not mask a degraded shared producer by resetting its
     * failure counter).
     */
    private void recordTxnSendSuccess() {
        boolean wasRecovering = consecutiveTxnSendFailures > 0;
        consecutiveTxnSendFailures = 0;
        if (wasRecovering) {
            LOG.info("[CPI-KAFKA-PLUS-DIAG] Transactional send recovered after previous failures for topic='{}'",
                    endpoint.getEffectiveTopic());
        }
    }

    private void handleTxnSendFailure(Exception e) {
        consecutiveTxnSendFailures++;
        Throwable cause = KafkaErrorHelper.extractKafkaCause(e);
        LOG.error("[CPI-KAFKA-PLUS-DIAG] Transactional send failure ({} consecutive) for topic='{}': {}",
                consecutiveTxnSendFailures, endpoint.getEffectiveTopic(),
                cause != null ? cause.getClass().getSimpleName() : e.getMessage());
    }

    private void triggerReconnect() {
        initialized = false;
        closeProducerQuietly();
    }

    private void closeProducerQuietly() {
        if (kafkaProducer != null) {
            try {
                kafkaProducer.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] Error closing Kafka producer: {}", e.getMessage());
            }
            kafkaProducer = null;
        }
        if (avroHelper != null) {
            try {
                avroHelper.close();
            } catch (Exception e) {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] Error closing Avro helper: {}", e.getMessage());
            }
            avroHelper = null;
        }
    }

}
