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
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.support.DefaultProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
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
    /**
     * Upper bound for the AdminClient topic-existence probe. A metadata describe is a single round
     * trip, so this only ever comes into play when the broker is unreachable — in which case the
     * send that follows is going to fail anyway and there is nothing to gain from waiting longer.
     * Kept deliberately short because the probe also runs at startup, where it would otherwise add
     * to deployment time once per endpoint.
     */
    private static final int TOPIC_CHECK_TIMEOUT_MS = 5_000;
    /**
     * Safety cap for {@link #verifiedTopics}. Topics normally number in the single digits per
     * endpoint, but the topic can come from a header or a Simple expression, so an unbounded set
     * could grow without limit on a long-running tenant.
     */
    private static final int VERIFIED_TOPICS_CACHE_LIMIT = 256;
    /**
     * How often a reported-missing topic is re-checked before the send is failed, and how long to
     * wait between attempts. {@code createTopics} returns as soon as the controller has accepted the
     * request, so a topic can legitimately be absent from broker metadata for a moment afterwards —
     * without this, "create the topic, then send" would race against metadata propagation. Only ever
     * paid when the topic really does look missing.
     */
    private static final int TOPIC_RECHECK_ATTEMPTS = 2;
    private static final long TOPIC_RECHECK_DELAY_MS = 500L;

    private final CpiKafkaPlusEndpoint endpoint;
    /**
     * Bounds every wait for a send result so a sender thread that died — the classic case is a TLS
     * mismatch — cannot block a CPI worker thread indefinitely and surface as {@code Node Crashed}.
     */
    private final ProducerSendGuard sendGuard;
    /**
     * Topics already confirmed to exist. Only ever holds <em>positive</em> results: a topic that
     * the broker reported as missing must never be cached, otherwise creating the topic would not
     * take effect until the IFlow is redeployed.
     */
    private final java.util.Set<String> verifiedTopics =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<String, Boolean>());
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
    /**
     * Why the last topic probe could not reach the broker, kept so that the send failure which
     * follows can name a cause instead of reporting a bare metadata timeout. Only set for
     * inconclusive <em>connectivity</em> failures; cleared as soon as a probe succeeds.
     */
    private volatile Throwable lastProbeFailure = null;

    public CpiKafkaPlusProducer(CpiKafkaPlusEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
        this.sendGuard = ProducerSendGuard.forEndpoint(endpoint);
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
                throw new IllegalArgumentException(
                        "Please configure transactionalIdPrefix — it is required whenever "
                        + "transactional batching is switched on");
            }
            if (!endpoint.isEnableIdempotence()) {
                throw new IllegalArgumentException(
                        "Please leave idempotence switched on — it cannot be turned off "
                        + "while transactional batching is enabled");
            }
            int slots = endpoint.getMaxConcurrentTransactions();
            if (slots < 1) {
                throw new IllegalArgumentException(
                        "Please set maxConcurrentTransactions to 1 or higher (configured value: " + slots
                        + ") — with transactional batching enabled, a value below 1 would leave the "
                        + "adapter unable to send anything");
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
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] Could not determine a stable identifier for this worker node, "
                        + "so a random one ('{}') will be used instead. This is safe, but note that it will "
                        + "change on every restart.",
                        resolvedMemberSuffix);
            }
            txnSlotSemaphore = new java.util.concurrent.Semaphore(slots, true);
            txnSlotInUse = new boolean[slots];
            LOG.info("[CPI-KAFKA-PLUS-DIAG] Transactional batching enabled with max {} concurrent transactions. "
                    + "Prefix: {}, memberSuffix: {}",
                    slots, endpoint.getTransactionalIdPrefix(), resolvedMemberSuffix);
        }

        // Surface a missing topic in the deployment log right away, and warm the cache when it is
        // present. Warning only, off the deployment thread — never blocks or delays route startup
        // (see the method javadoc).
        startTopicCheckInBackground();
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
                    msg += ". Root cause: " + KafkaErrorHelper.describeChain(lastInitException);
                }
                // Do not pass lastInitException as the cause: CPI's HTTP adapter surfaces
                // getCause().getMessage() rather than this exception's own message, which would
                // swallow the descriptive chain we just built. The full stack trace is already
                // logged by logInitFailure(); we only need a readable message here.
                throw new IllegalStateException(msg);
            }
        }

        Message in = exchange.getIn();

        // Determine topic - header overrides config
        String topic = resolveTopic(exchange, in.getHeader("CamelKafkaTopic", String.class));
        if (topic == null || topic.isEmpty()) {
            topic = resolveTopic(exchange, endpoint.getEffectiveTopic());
        }

        // Fail-fast for both send paths: a topic that does not exist would otherwise make the
        // producer block for max.block.ms and then report only a metadata timeout, hiding the actual
        // cause. Cached per topic, so this costs one AdminClient round trip per topic, not per
        // message.
        assertTopicExists(topic);

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
                        in, this::addRecordHeaders, valueSerializer, null, sendGuard);

                ProducerBatchHelper.setResponseHeadersAndBody(in, topic, batchMode, result);
                recordSendSuccess();
            } catch (Exception e) {
                handleSendFailure(e);
                throw sendFailure("Failed to send batch to", topic, e);
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
                    in, this::addRecordHeaders, valueSerializer, null, sendGuard);

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
            throw sendFailure("Failed to send transactional batch to", topic, e);
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
            RecordMetadata metadata = sendGuard.await(future, sendGuard.newDeadline(),
                    "Send to topic '" + topic + "'");

            in.setHeader("SAP_Receiver", metadata.topic());
            // Adapter-native headers, consistent with the batch producer and consumer.
            // Note: the previous CamelKafkaTopic/Partition/Offset/Timestamp headers are
            // intentionally no longer set here (#84) - iFlows must use the CpiKafkaPlus*
            // headers below instead.
            in.setHeader("CpiKafkaPlusTopic", metadata.topic());
            in.setHeader("CpiKafkaPlusPartition", metadata.partition());
            in.setHeader("CpiKafkaPlusOffset", metadata.offset());
            in.setHeader("CpiKafkaPlusTimestamp", metadata.timestamp());
            in.setHeader("CpiKafkaPlusStatus", "OK");

            recordSendSuccess();
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] Message sent to topic '{}' partition {} offset {}",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            handleSendFailure(e);
            throw sendFailure("Failed to send message to", topic, e);
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

    /**
     * Builds the exception for a failed send, folding in everything the adapter already knows about
     * why the broker might be unreachable.
     *
     * <p>Kafka reports an unreachable broker as {@code Topic ... not present in metadata after N ms}
     * and nothing else — the same message it uses for a topic that does not exist, and with no trace
     * of a refused connection, a rejected handshake or a wrong security protocol. Where the topic
     * probe learned more, that cause is appended here and attached as a suppressed exception, so the
     * operator sees it in the Message Processing Log.
     *
     * @param prefix what failed, e.g. {@code "Failed to send message to"}
     */
    private RuntimeException sendFailure(String prefix, String topic, Exception e) {
        StringBuilder msg = new StringBuilder(prefix)
                .append(" Kafka topic '").append(topic).append("': ").append(e.getMessage());

        Throwable probeFailure = lastProbeFailure;
        if (probeFailure != null) {
            msg.append(" The broker could not be reached during the pre-send check either (")
                    .append(KafkaErrorHelper.describeChain(probeFailure)).append(").");
            String hint = KafkaErrorHelper.tlsMismatchHint(endpoint.getSecurityProtocol());
            if (hint != null) {
                msg.append(' ').append(hint);
            }
        }

        RuntimeException failure = new RuntimeException(msg.toString(), e);
        if (probeFailure != null && probeFailure != e) {
            failure.addSuppressed(probeFailure);
        }
        return failure;
    }

    /** Outcome of the AdminClient topic probe. */
    private enum TopicCheck {
        /** Broker confirmed the topic exists. */
        EXISTS,
        /** Broker definitively reported the topic as absent. */
        MISSING,
        /** No usable answer (no Describe permission, broker unreachable, unsupported API, ...). */
        INCONCLUSIVE
    }

    /**
     * Outcome of the topic probe together with the exception that produced it. The cause matters:
     * an inconclusive probe caused by a rejected TLS handshake is a configuration error worth
     * reporting straight away, while the same verdict caused by a timeout may just be a hiccup.
     */
    private static final class TopicCheckResult {

        private static final TopicCheckResult EXISTS = new TopicCheckResult(TopicCheck.EXISTS, null);
        private static final TopicCheckResult MISSING = new TopicCheckResult(TopicCheck.MISSING, null);

        private final TopicCheck state;
        /** Only set for {@link TopicCheck#INCONCLUSIVE}; may still be {@code null} there. */
        private final Throwable cause;

        private TopicCheckResult(TopicCheck state, Throwable cause) {
            this.state = state;
            this.cause = cause;
        }

        static TopicCheckResult inconclusive(Throwable cause) {
            return new TopicCheckResult(TopicCheck.INCONCLUSIVE, cause);
        }
    }

    /**
     * Runtime guard, called on every send path before the record leaves the adapter: fails
     * immediately with a clear message if the broker reports the target topic as missing, or if the
     * probe hit a rejected authentication or a failed TLS handshake. Either way the alternative is
     * letting the producer block for {@code max.block.ms} and report only a metadata timeout that
     * names no cause at all.
     *
     * <p>A confirmed topic is remembered in {@link #verifiedTopics}, so the AdminClient round trip
     * is paid once per topic rather than once per message. An inconclusive result is deliberately
     * <em>not</em> cached: it means we learned nothing, and the next send should try again.
     *
     * <p>An inconclusive probe that merely timed out does <em>not</em> fail the exchange — a short
     * broker hiccup must not turn into a message failure when the send itself may still succeed
     * within {@code max.block.ms}. Its cause is remembered in {@link #lastProbeFailure} instead and
     * folded into the send error if the send does go on to fail.
     *
     * <p>A single {@code MISSING} answer is likewise not treated as proof: {@code createTopics}
     * returns once the controller has accepted the request, and the topic can be absent from broker
     * metadata for a moment afterwards. Failing on the first answer would break the entirely normal
     * "create the topic, then send" sequence, so the probe is repeated briefly before giving up.
     */
    private void assertTopicExists(String topic) {
        if (topic == null || topic.isEmpty() || verifiedTopics.contains(topic)) {
            return;
        }
        TopicCheckResult result = checkTopicExists(topic);
        for (int attempt = 0; result.state == TopicCheck.MISSING
                && attempt < TOPIC_RECHECK_ATTEMPTS; attempt++) {
            if (!sleepQuietly(TOPIC_RECHECK_DELAY_MS)) {
                break;
            }
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] Topic '{}' reported as missing, re-checking ({}/{})",
                    topic, attempt + 1, TOPIC_RECHECK_ATTEMPTS);
            result = checkTopicExists(topic);
        }
        if (result.state == TopicCheck.MISSING) {
            throw new IllegalArgumentException(
                    "Kafka topic '" + topic + "' does not exist on the broker. "
                    + "Please create the topic before sending to it "
                    + "(auto-create may be disabled on this cluster).");
        }
        if (result.state == TopicCheck.EXISTS) {
            lastProbeFailure = null;
            if (verifiedTopics.size() < VERIFIED_TOPICS_CACHE_LIMIT) {
                verifiedTopics.add(topic);
            }
            return;
        }

        if (KafkaErrorHelper.isDefinitiveAuthOrTlsFailure(result.cause)) {
            throw new IllegalStateException(
                    "Cannot connect to Kafka broker '" + endpoint.getBootstrapServers() + "' for topic '"
                    + topic + "': " + KafkaErrorHelper.describeChain(result.cause)
                    + ". Please check Security Protocol, Credential Alias and SSL Keystore Alias.",
                    result.cause);
        }
        lastProbeFailure = result.cause;
    }

    /**
     * Deployment-time probe, called from {@link #doStart()}: reports a missing topic as a warning
     * in the deployment log so the misconfiguration is visible immediately, without ever blocking
     * the route from starting. A hard failure here would tie the IFlow's ability to start to broker
     * availability — and routes are restarted not only by operators but also by node moves and
     * tenant maintenance, so an outage at the wrong moment would leave the IFlow in an error state
     * requiring manual intervention. It also would break the common "deploy the IFlow, create the
     * topic afterwards" ordering.
     *
     * <p>A confirmed topic is cached, so this doubles as a warm-up for the first send.
     *
     * <p>Runs on a short-lived daemon thread rather than on the deployment thread: the result is
     * purely diagnostic, and an unreachable broker would otherwise add the probe timeout to route
     * startup for every Kafka endpoint in the IFlow. The runtime check on the send path is the one
     * that actually enforces anything, so nothing is lost by not waiting here.
     */
    private void startTopicCheckInBackground() {
        final String topic = endpoint.getEffectiveTopic() == null
                ? null : endpoint.getEffectiveTopic().trim();
        if (topic == null || topic.isEmpty()) {
            // Topic supplied per message via the CamelKafkaTopic header — nothing to check yet.
            return;
        }
        if (topic.contains("${")) {
            LOG.info("[CPI-KAFKA-PLUS-DIAG] Topic '{}' is an expression and can only be resolved per "
                    + "message, so its existence is checked on first send instead of at startup.", topic);
            return;
        }
        Thread probe = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    warnIfTopicMissing(topic);
                } catch (Throwable t) {
                    // A diagnostic must never take anything else down with it.
                    LOG.warn("[CPI-KAFKA-PLUS-DIAG] Startup topic check for '{}' failed unexpectedly: {}",
                            topic, t.getMessage());
                }
            }
        }, "cpi-kafka-plus-topic-check-" + topic);
        probe.setDaemon(true);
        probe.start();
    }

    private void warnIfTopicMissing(String topic) {
        TopicCheckResult result = checkTopicExists(topic);
        if (result.state == TopicCheck.MISSING) {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] Kafka topic '{}' does not exist on the broker. Sending to "
                    + "this endpoint will fail until the topic is created (auto-create may be disabled "
                    + "on this cluster). The route is started anyway so the topic can still be created "
                    + "without a redeployment.", topic);
            return;
        }
        if (result.state == TopicCheck.EXISTS) {
            lastProbeFailure = null;
            if (verifiedTopics.size() < VERIFIED_TOPICS_CACHE_LIMIT) {
                verifiedTopics.add(topic);
            }
            LOG.info("[CPI-KAFKA-PLUS-DIAG] Kafka topic '{}' confirmed to exist at startup", topic);
            return;
        }
        // Inconclusive: the topic is unknown, but an unreachable broker is worth reporting at
        // deployment time — this is where a wrong Security Protocol becomes visible before the
        // first message is ever sent, rather than as a metadata timeout hours later.
        if (result.cause != null) {
            String hint = KafkaErrorHelper.tlsMismatchHint(endpoint.getSecurityProtocol());
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] Could not reach Kafka broker '{}' at startup to verify topic "
                    + "'{}': {}.{} The route is started anyway; sending will report the same cause.",
                    endpoint.getBootstrapServers(), topic,
                    KafkaErrorHelper.describeChain(result.cause),
                    hint == null ? "" : " " + hint);
        }
    }

    /**
     * Asks the broker whether {@code topic} exists, using a short-lived AdminClient bounded by
     * {@link #TOPIC_CHECK_TIMEOUT_MS}. Never throws: anything other than a definitive answer is
     * reported as {@link TopicCheck#INCONCLUSIVE} so a restricted service account (no Describe
     * permission) or a transient network problem cannot become a new single point of failure.
     *
     * <p>Every inconclusive verdict carries the exception that caused it. Discarding it here was the
     * reason a wrong Security Protocol surfaced as nothing but {@code Topic ... not present in
     * metadata}: the probe had already learned why the broker was unreachable, but the caller never
     * got to see it.
     */
    private TopicCheckResult checkTopicExists(String topic) {
        final Properties adminProps;
        try {
            adminProps = buildTopicCheckProperties();
        } catch (Exception e) {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] Could not build AdminClient configuration to check topic "
                    + "'{}' ({}). Skipping the check.", topic, KafkaErrorHelper.describeChain(e));
            return TopicCheckResult.inconclusive(e);
        }

        try (AdminClient admin = BundleBackedClassLoader.withBundleClassLoader(getClass(),
                () -> AdminClient.create(adminProps))) {

            DescribeTopicsResult result = admin.describeTopics(Collections.singletonList(topic));
            KafkaFuture<TopicDescription> future = result.topicNameValues().get(topic);
            try {
                // Bounded: the AdminClient completes this future from its own network thread, so an
                // unbounded get() would hang for good if that thread dies.
                future.get(TOPIC_CHECK_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
                LOG.debug("[CPI-KAFKA-PLUS-DIAG] Topic '{}' confirmed to exist", topic);
                return TopicCheckResult.EXISTS;
            } catch (java.util.concurrent.TimeoutException te) {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] Timed out after {} ms while checking whether topic "
                        + "'{}' exists. Skipping the check.", TOPIC_CHECK_TIMEOUT_MS, topic);
                return TopicCheckResult.inconclusive(te);
            } catch (java.util.concurrent.ExecutionException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof UnknownTopicOrPartitionException) {
                    return TopicCheckResult.MISSING;
                }
                if (cause instanceof org.apache.kafka.common.errors.TopicAuthorizationException
                        || cause instanceof org.apache.kafka.common.errors.ClusterAuthorizationException) {
                    // No Describe permission: we can neither confirm nor deny the topic's existence.
                    // The send proceeds and the producer will surface an authorization error itself.
                    // Deliberately not reported as a probe failure — it says nothing about whether
                    // producing to the topic is permitted.
                    LOG.warn("[CPI-KAFKA-PLUS-DIAG] No permission to describe topic '{}' ({}). "
                            + "Skipping the existence check.",
                            topic, cause.getClass().getSimpleName());
                    return TopicCheckResult.inconclusive(null);
                }
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] Could not verify existence of topic '{}' "
                        + "(AdminClient error: {}). Skipping the check.",
                        topic, KafkaErrorHelper.describeChain(cause != null ? cause : ex));
                return TopicCheckResult.inconclusive(cause != null ? cause : ex);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] Interrupted while checking topic '{}' existence.", topic);
                return TopicCheckResult.inconclusive(null);
            }
        } catch (Exception e) {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] AdminClient creation failed while checking topic '{}' "
                    + "existence ({}). Skipping the check.", topic, KafkaErrorHelper.describeChain(e));
            return TopicCheckResult.inconclusive(e);
        }
    }

    /**
     * Sleeps without throwing. Returns {@code false} if the thread was interrupted, so the caller can
     * stop retrying instead of swallowing the interrupt.
     */
    private static boolean sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** Bootstrap and security settings for the topic probe, derived from the producer config. */
    private Properties buildTopicCheckProperties() {
        Properties producerProps = ProducerConfigFactory.buildProducerProperties(endpoint);
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        for (String key : producerProps.stringPropertyNames()) {
            if (key.startsWith("ssl.") || key.startsWith("sasl.") || key.equals("security.protocol")) {
                adminProps.put(key, producerProps.getProperty(key));
            }
        }
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TOPIC_CHECK_TIMEOUT_MS);
        // Leave room for one retry inside the API timeout rather than making them equal.
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 2 * TOPIC_CHECK_TIMEOUT_MS);
        return adminProps;
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
