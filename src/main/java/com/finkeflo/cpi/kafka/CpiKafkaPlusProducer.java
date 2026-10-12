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
import org.apache.camel.support.ExchangeHelper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.finkeflo.cpi.kafka.KafkaErrorHelper.Classification;
import com.finkeflo.cpi.kafka.ProducerBatchHelper.ProducerPath;

/**
 * Kafka Producer - sends messages from CPI IFlow to Kafka.
 * Receiver direction in CPI terminology (CPI receives from IFlow, sends to external system).
 */
public class CpiKafkaPlusProducer extends DefaultProducer {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusProducer.class);

    /** Rate limit for {@code send.bundle-wiring-invalid}: every failing exchange hits it until the iFlow is redeployed. */
    private static final long CLASS_SPACE_GONE_LOG_INTERVAL_MS = 60_000L;
    private volatile long lastClassSpaceGoneLogMs;

    private final CpiKafkaPlusEndpoint endpoint;
    /**
     * Bounds every wait for a send result so a sender thread that died — the classic case is a TLS
     * mismatch — cannot block a CPI worker thread indefinitely and surface as {@code Node Crashed}.
     */
    private final ProducerSendGuard sendGuard;
    /**
     * The shared producer. Volatile, and replaced or cleared only while holding the {@code this}
     * monitor, so a reconnect cannot clear a producer that another thread has just built.
     */
    private volatile KafkaProducer<byte[], byte[]> kafkaProducer;
    /**
     * Creates the throw-away producer of a transactional attempt. A field rather than a direct
     * {@code new}, because it is the only seam through which a test can fail a specific phase
     * (SEND vs. COMMIT) with a specific exception class while still talking to a real broker —
     * and the shared Testcontainers Kafka instance rules out stopping the broker itself.
     */
    java.util.function.Function<Properties, KafkaProducer<byte[], byte[]>> txnProducerFactory =
            TransactionalProducerPool::createTransactionalProducer;
    /**
     * Creates the shared producer. A field for the same reason as {@link #txnProducerFactory}: it is
     * the seam through which a test hands out the producer that a send after a rebuild builds,
     * without a broker.
     */
    java.util.function.Function<Properties, KafkaProducer<byte[], byte[]>> sharedProducerFactory =
            props -> new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
    private AvroSerializerHelper avroHelper;
    private AdapterTracingHelper tracingHelper;
    private JsonSchemaValidator jsonSchemaValidator;

    /**
     * The transaction slots and the {@code transactional.id} they stand for, built in
     * {@code doStart()} when transactional batching is on. Package-private so a test can supply
     * one without a start.
     */
    TransactionalProducerPool txnPool;

    private volatile boolean initialized = false;
    private volatile boolean helpersInitialized = false;
    /**
     * Set by {@code doStop()}, cleared by {@code doStart()}. A send that is still running when the
     * route stops, or its retry, must not build a new shared producer: nothing would close it.
     */
    private volatile boolean stopped = false;
    private volatile int consecutiveSendFailures = 0;
    private volatile int consecutiveTxnSendFailures = 0;
    private volatile int consecutiveInitFailures = 0;
    private volatile Throwable lastInitException = null;

    /**
     * Kafka client.id captured after the producer is built, for correlation with broker-side logs.
     * Stable across restarts as long as the endpoint adapterInstanceID is stable.
     */
    private volatile String resolvedClientId;

    // ─────────────────────────────────────────────────────────────────────────────
    // c7: Success heartbeat state — matches consumer's logEmitCycleHeartbeat pattern
    // ─────────────────────────────────────────────────────────────────────────────
    /** Heartbeat interval: one line per 5 minutes, emitted immediately on state change. */
    private static final long HEARTBEAT_INTERVAL_MS = 5 * 60 * 1000L;
    private volatile long lastHeartbeatMs = 0L;
    private volatile boolean lastHeartbeatHealthy = false;
    private volatile long sendsSinceLastHeartbeat = 0L;

    // ─────────────────────────────────────────────────────────────────────────────
    // e4: Rebuild storm guard — prevents rebuilding on every record of a failing batch.
    // If a rebuild fails or the producer re-fails immediately, back off before retrying.
    // ─────────────────────────────────────────────────────────────────────────────
    /** Minimum interval between producer rebuild attempts. */
    private static final long REBUILD_BACKOFF_MS = 30_000L;
    private volatile long lastRebuildAttemptMs = 0L;
    private volatile int rebuildAttemptsSinceSuccess = 0;
    /** Tracks whether a rebuild is pending effect confirmation from the next send. */
    private volatile boolean pendingRebuildEffect = false;

    /** e4b: Node-level fault escalation (same fault N times in T minutes despite mitigation). */
    private final NodeFaultEscalation nodeFaultEscalation;
    /**
     * The topic probe and the metadata pre-warm, with the topics they remember. Package-private so
     * a test can script the probe.
     */
    final TopicMetadataChecker topicChecker;

    public CpiKafkaPlusProducer(CpiKafkaPlusEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
        this.sendGuard = ProducerSendGuard.forEndpoint(endpoint);
        this.nodeFaultEscalation = new NodeFaultEscalation(endpoint::getEffectiveTopic,
                endpoint::isDiagnosticsLevelFull, () -> resolvedClientId, System::currentTimeMillis);
        this.topicChecker = new TopicMetadataChecker(endpoint::getEffectiveTopic,
                endpoint::getBootstrapServers, endpoint::getSecurityProtocol,
                () -> ProducerConfigFactory.buildProducerProperties(endpoint),
                () -> kafkaProducer, () -> resolvedClientId);
    }

    @Override
    protected void doStart() throws Exception {
        try {
            doStartInternal();
        } catch (Exception e) {
            // Previously the only lines in the adapter carrying a second, competing
            // marker — and among the most valuable ones there are.
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.start.failed")
                    .with("topic", endpoint.getEffectiveTopic())
                    .with("bootstrapServers", endpoint.getBootstrapServers())
                    .with("securityProtocol", endpoint.getSecurityProtocol()), e);
            throw e;
        }
    }

    private void doStartInternal() throws Exception {
        stopped = false;
        // Fail-fast: validate shared configuration (Schema Registry, JSON Schema, SASL)
        endpoint.validateConfiguration(appliesJsonSchema());
        ReceiverConfigValidator.validateBeforeStart(endpoint);

        super.doStart();
        BundleClassWarmup.ensureStarted(CpiKafkaPlusProducer.class, "producer.start");
        LOG.info("[CPI-KAFKA-PLUS-DIAG] Starting CPI Kafka Producer for topic '{}' (lazy init — Kafka resources created on first send) {}",
                endpoint.getEffectiveTopic(), OsgiBundleInfo.describeClassSpace(CpiKafkaPlusProducer.class));

        tracingHelper = new AdapterTracingHelper(endpoint);

        ReceiverConfigValidator.validateTransactional(endpoint);
        if (endpoint.isTransactionalBatching()) {
            txnPool = TransactionalProducerPool.start(endpoint::getTransactionalIdPrefix,
                    endpoint.getEffectiveTopic(), NodeIdentity::memberSuffix, endpoint.getMaxConcurrentTransactions());
        }

        ReceiverConfigValidator.validateRetry(endpoint);

        // Surface a missing topic in the deployment log right away, and warm the cache when it is
        // present. Warning only, off the deployment thread — never blocks or delays route startup
        // (see the method javadoc).
        topicChecker.startTopicCheckInBackground();
    }

    @Override
    protected void doStop() throws Exception {
        LOG.info("[CPI-KAFKA-PLUS-DIAG] Stopping CPI Kafka Producer for topic '{}'", endpoint.getEffectiveTopic());
        // Before the producer is closed, so that a send which finds it closed cannot build a new one.
        stopped = true;
        initialized = false;
        helpersInitialized = false;
        consecutiveSendFailures = 0;
        consecutiveTxnSendFailures = 0;
        consecutiveInitFailures = 0;
        closeProducerQuietly();
        closeAvroHelperQuietly();
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

            // Helpers first: they need no connection, and a helper that cannot be built (a Schema
            // Registry credential alias that does not resolve, say) used to cost a complete
            // KafkaProducer, built and closed again under this monitor, on every message.
            if (!ensureHelpersInitializedLocked()) {
                return;
            }
            if (!createKafkaProducer()) {
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
     *
     * @return true if helpers are ready
     */
    private boolean ensureHelpersInitialized() {
        if (helpersInitialized) {
            return true;
        }
        synchronized (this) {
            return ensureHelpersInitializedLocked();
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
            // Before any Kafka client exists: a plaintext protocol against a TLS-only broker makes
            // Kafka allocate the broker's TLS alert as a 352 MB frame until jvmkill takes the node
            // down. That cannot be caught afterwards, so it must not be started.
            TlsListenerProbe.assertNoTlsListener(endpoint.getBootstrapServers(),
                    endpoint.getSecurityProtocol());
            Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] ensureInitialized: producer properties built, security={}, sasl={}",
                    endpoint.getSecurityProtocol(), endpoint.getSaslMechanism());
            kafkaProducer = BundleBackedClassLoader.withBundleClassLoader(getClass(),
                    () -> sharedProducerFactory.apply(props));

            // c3: Capture the resolved client.id for diagnostic correlation with broker-side logs.
            // This is stable across producer rebuilds as long as the endpoint adapterInstanceID
            // is stable, which is the case in CPI — the ID is assigned at iFlow deployment time.
            resolvedClientId = (String) props.get(org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG);

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
            if (appliesJsonSchema()) {
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
            lastInitException = e;
            return false;
        }
    }

    /**
     * The JSON Schema applies to single messages only. A batch is sent without validation, so with a
     * batch send mode the schema is neither checked at start nor compiled (#263);
     * {@link ReceiverConfigValidator#validateBeforeStart} logs once that it has no effect.
     */
    private boolean appliesJsonSchema() {
        return endpoint.isJsonSchemaValidation() && "NONE".equalsIgnoreCase(endpoint.getProducerBatchMode());
    }

    private IllegalStateException initFailure(String component) {
        String msg = component + " not initialized — init failed, will retry on next exchange";
        if (lastInitException != null) {
            msg += ". Root cause: " + KafkaErrorHelper.describeChain(lastInitException);
        }
        // Do not pass lastInitException as the cause: CPI's HTTP adapter surfaces
        // getCause().getMessage() rather than this exception's own message, which would
        // swallow the descriptive chain we just built. The full stack trace is already
        // logged by logInitFailure(); we only need a readable message here.
        return new IllegalStateException(msg);
    }

    private void logInitFailure(String component, Throwable e) {
        consecutiveInitFailures++;
        // Always ERROR. The first nine attempts used to be WARN, which does not reach the CPI
        // tenant trace file — so the most common real failure class (credentials, TLS, unreachable
        // broker) produced nothing visible until the tenth attempt, if it ever got there.
        AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.init.failed")
                .with("component", component)
                .with("consecutiveFailures", consecutiveInitFailures)
                .with("bootstrapServers", endpoint.getBootstrapServers())
                .with("securityProtocol", endpoint.getSecurityProtocol())
                .with("thread", Thread.currentThread().getName()), e);
        tracingHelper.publishConnectionStatus(false, KafkaErrorHelper.wrapIfError(e));
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        try {
            doProcess(exchange);
        } catch (Throwable t) {
            if (ClassSpaceFaults.isOnStaleClassSpace(t, getClass())) {
                throw classSpaceGone(t);
            }
            throw t;
        }
    }

    /**
     * Same break as the consumer's {@code poll.bundle-wiring-invalid} (issue #148), seen from the
     * send side: the adapter was updated while this integration flow kept running on the old bundle
     * revision, and a first-time class/native-library load (typically the compression codec of the
     * first batch) failed over the dead loader. Nothing inside the adapter can repair that; say so
     * once per minute in the trace and give every failing exchange the actionable message instead
     * of a generic send failure.
     */
    private IllegalStateException classSpaceGone(Throwable failure) {
        long now = System.currentTimeMillis();
        if (now - lastClassSpaceGoneLogMs >= CLASS_SPACE_GONE_LOG_INTERVAL_MS) {
            lastClassSpaceGoneLogMs = now;
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("send.bundle-wiring-invalid")
                    .with("topic", endpoint.getEffectiveTopic())
                    .with("classSpace", OsgiBundleInfo.describeClassSpace(getClass()))
                    .with("action", "redeploy the integration flow"), failure);
        }
        return new IllegalStateException(
                "The adapter bundle was updated while this integration flow was running; the class space of "
                + "this route (topic='" + endpoint.getEffectiveTopic() + "') is gone and cannot be recovered "
                + "from inside the adapter. Redeploy the integration flow to bind it to the new adapter version. "
                + "Root cause: " + KafkaErrorHelper.describeChain(failure), failure);
    }

    /** Visible for tests: the unguarded send path behind {@link #process(Exchange)}. */
    void doProcess(Exchange exchange) throws Exception {
        String batchMode = endpoint.getProducerBatchMode();
        // When every exchange goes through the transactional path (enableTransactions with a batch
        // mode), the shared non-transactional KafkaProducer is never used and must not gate
        // processing — only the serialization/validation helpers are needed here.
        boolean transactionalOnlyPath = endpoint.isTransactionalBatching();

        if (transactionalOnlyPath) {
            // Without the Avro helper the batch would be committed as raw JSON.
            if (!ensureHelpersInitialized()) {
                throw initFailure("Serialization helpers");
            }
        } else {
            // Built here, before the topic checks, so the pre-warm has a client to warm. The send
            // reads the producer again: a rebuild may replace it in between.
            sharedProducer();
        }

        Message in = exchange.getIn();

        String topic = targetTopic(exchange);

        // Fail-fast for both send paths: a topic that does not exist would otherwise make the
        // producer block for max.block.ms and then report only a metadata timeout, hiding the actual
        // cause. Cached per topic, so this costs one AdminClient round trip per topic, not per
        // message.
        topicChecker.assertTopicExists(topic);
        topicChecker.prewarmTopicMetadata(topic);

        if (!"NONE".equalsIgnoreCase(batchMode)) {
            processBatch(exchange, in, topic, batchMode);
        } else {
            processSingle(exchange, in, topic);
        }
    }

    /**
     * The shared producer for one send attempt, built when there is none. Callers read it once into
     * a local and keep using that: a rebuild after another send's failure clears the field at any
     * moment, and reading the field again in the middle of a send turned that race into a
     * {@code NullPointerException} (#246). After the stop no producer is built.
     */
    private KafkaProducer<byte[], byte[]> sharedProducer() {
        KafkaProducer<byte[], byte[]> producer = kafkaProducer;
        if (producer != null) {
            return producer;
        }
        // Under the monitor that closeProducerQuietly() detaches under, so a rebuild cannot clear the
        // producer between building and reading it, and the stop closes any producer built before it.
        synchronized (this) {
            if (stopped) {
                throw new IllegalStateException("The receiver for Kafka topic '" + endpoint.getEffectiveTopic()
                        + "' was stopped (the integration flow is stopping or was undeployed), so no new Kafka "
                        + "producer is built and the message is not sent.");
            }
            ensureInitialized();
            producer = kafkaProducer;
            if (producer == null) {
                throw initFailure("Kafka producer");
            }
            return producer;
        }
    }

    private void processBatch(Exchange exchange, Message in, String topic,
                               String batchMode) throws Exception {
        // Read the body once: an unconverted InputStream body cannot be read a second time, and
        // the trace below must show the same content that was parsed.
        String body = in.getBody(String.class);
        java.util.List<BatchRecord> records = parseBatchRecords(body, batchMode);

        // Trace before the send, as processSingle does: setResponseHeadersAndBody replaces the
        // body with the send summary afterwards. Once per message, outside the transactional
        // retry loop, so a retried transaction does not write the payload again. The String
        // overload encodes only while trace is active, sparing every other batch a full copy.
        tracingHelper.traceOutbound(exchange, body);

        String fallbackKey = in.getHeader("kafka.KEY", String.class);
        Integer partition = parsePartitionHeader(in);
        Long timestamp = in.getHeader("kafka.OVERRIDE_TIMESTAMP", Long.class);

        LOG.info("[CPI-KAFKA-PLUS-DIAG] Batch send: {} records to topic '{}' (mode={})",
                records.size(), topic, batchMode);

        ProducerBatchHelper.ByteSerializer valueSerializer = buildBatchValueSerializer();

        if (endpoint.isTransactionalBatching()) {
            sendTransactionalBatch(in, topic, batchMode, records, fallbackKey, partition, timestamp, valueSerializer);
        } else {
            KafkaProducer<byte[], byte[]> producer = sharedProducer();
            try {
                ProducerBatchHelper.BatchSendResult result = ProducerBatchHelper.sendBatch(
                        producer, records, topic, fallbackKey, partition, timestamp,
                        in, this::addRecordHeaders, valueSerializer, null, sendGuard,
                        ProducerPath.SHARED, resolvedClientId);

                ProducerBatchHelper.setResponseHeadersAndBody(in, topic, batchMode, result);
                recordSendSuccess();
            } catch (Exception e) {
                Exception failure = reclassifyIfProducerReplaced(e, producer);
                Map<String, String> ctx = new java.util.LinkedHashMap<>();
                ctx.put("topic", topic);
                ctx.put("batchMode", batchMode);
                ctx.put("recordCount", String.valueOf(records.size()));
                CorrelationHelper.addTo(ctx, exchange);
                tracingHelper.traceError(exchange, failure, ctx);
                handleSendFailure(failure, "producer.batch.send", ctx);
                throw sendFailure("Failed to send batch to", topic, failure);
            }
        }
    }

    private void sendTransactionalBatch(Message in, String topic, String batchMode,
                                        java.util.List<BatchRecord> records, String fallbackKey,
                                        Integer partition, Long timestamp,
                                        ProducerBatchHelper.ByteSerializer valueSerializer) throws Exception {
        // The transactional producer is created outside ensureInitialized(), so it needs the same
        // protection against a plaintext protocol on a TLS-only broker. A conclusive probe is cached
        // per bootstrap/security config (an unreachable broker is probed again), and it runs before
        // acquiring a transaction slot so a timeout on a silent endpoint cannot hold scarce slots.
        TlsListenerProbe.assertNoTlsListener(endpoint.getBootstrapServers(),
                endpoint.getSecurityProtocol());

        int slotId = -1;
        String transactionalId = null;
        txnPool.acquire();

        try {
            slotId = txnPool.claimFreeSlot();

            // The id is computed once, outside the retry loop, so every attempt of this message uses
            // the same transactional.id. That is what lets the successor producer fence the epoch of
            // the failed one and have the broker clean up its dangling transaction.
            transactionalId = txnPool.transactionalId(slotId);

            java.util.Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);
            props.put(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

            runTransactionalAttempts(in, topic, batchMode, records, fallbackKey, partition, timestamp,
                    valueSerializer, props, transactionalId, slotId);
        } finally {
            // Read from the field again, not from a local: after a stop/start the slot goes back to
            // the new pool. Issue #204 is about changing that; ProducerTransactionSlotTest pins it.
            txnPool.release(slotId);
        }
    }

    /**
     * Runs up to {@code producerRetryMaxAttempts} transactional sends, each with its own throw-away
     * producer, and reports the outcome exactly once.
     *
     * <p>Deliberately nested inside the transaction slot held by the caller: every attempt must use
     * the same {@code transactional.id}, which is derived from the slot. The cost is that a retry
     * holds a scarce slot for its whole duration — bounded by
     * {@code producerRetryTotalBudgetSeconds} and validated against the slot count at start-up.
     *
     * <p>{@code abortTransaction()} is deliberately <em>not</em> called between attempts. After a
     * network disconnect it throws itself — it is precisely the state this feature routes around.
     * {@code close(Duration)} aborts an ongoing transaction that is not already completing
     * (kafka-clients 4.3.1, {@code KafkaProducer.java:1366-1372}) and the successor's
     * {@code initTransactions()} aborts anything the previous instance left in progress
     * ({@code KafkaProducer.java:637-643}); two independent nets for the same case.
     */
    private void runTransactionalAttempts(Message in, String topic, String batchMode,
                                          java.util.List<BatchRecord> records, String fallbackKey,
                                          Integer partition, Long timestamp,
                                          ProducerBatchHelper.ByteSerializer valueSerializer,
                                          java.util.Properties props, String transactionalId,
                                          int slotId) throws Exception {
        int maxAttempts = endpoint.getProducerRetryMaxAttempts();
        long delayMs = endpoint.getProducerRetryDelaySeconds() * 1000L;
        long startedAtMs = System.currentTimeMillis();
        long budgetDeadlineMs = startedAtMs + endpoint.getProducerRetryTotalBudgetSeconds() * 1000L;

        Exception lastError = null;
        ProducerRetryPolicy.TxnPhase phase = ProducerRetryPolicy.TxnPhase.CREATE;
        ProducerRetryPolicy.StopReason stopReason = ProducerRetryPolicy.StopReason.RETRY_DISABLED;
        int attempt = 0;

        for (attempt = 1; attempt <= maxAttempts; attempt++) {
            phase = ProducerRetryPolicy.TxnPhase.CREATE;
            KafkaProducer<byte[], byte[]> txnProducer = null;
            try {
                txnProducer = txnProducerFactory.apply(props);

                phase = ProducerRetryPolicy.TxnPhase.INIT;
                txnProducer.initTransactions();

                phase = ProducerRetryPolicy.TxnPhase.BEGIN;
                txnProducer.beginTransaction();

                phase = ProducerRetryPolicy.TxnPhase.SEND;
                ProducerBatchHelper.BatchSendResult result = ProducerBatchHelper.sendBatch(
                        txnProducer, records, topic, fallbackKey, partition, timestamp,
                        in, this::addRecordHeaders, valueSerializer, null, sendGuard,
                        ProducerPath.TRANSACTIONAL, transactionalId);

                phase = ProducerRetryPolicy.TxnPhase.COMMIT;
                txnProducer.commitTransaction();
                phase = ProducerRetryPolicy.TxnPhase.COMMITTED;

                ProducerBatchHelper.setResponseHeadersAndBody(in, topic, batchMode, result);
                reportRetryRecovery(in, topic, attempt, System.currentTimeMillis() - startedAtMs,
                        ProducerPath.TRANSACTIONAL);
                recordTxnSendSuccess();
                return;
            } catch (Exception e) {
                lastError = e;
                ProducerRetryPolicy.Decision decision = ProducerRetryPolicy.decideTransactional(
                        e, phase, attempt, maxAttempts, delayMs, budgetDeadlineMs,
                        System.currentTimeMillis(),
                        endpoint.isProducerRetryOnlyTransientErrors());
                if (!decision.isRetry()) {
                    stopReason = decision.stopReason();
                    break;
                }
                logRetryAttempt(attempt, maxAttempts, phase, e, topic, transactionalId, slotId,
                        budgetDeadlineMs);
            } finally {
                TransactionalProducerPool.closeTxnProducerQuietly(txnProducer, slotId);
            }

            if (!sleepBetweenAttempts(delayMs)) {
                stopReason = ProducerRetryPolicy.StopReason.INTERRUPTED;
                break;
            }
        }

        // All attempts consumed, or the failure was not retry-eligible: report once. Feeding the
        // failure handlers per attempt would let the auto-pause counter and the node-fault
        // escalation count up to five failures for a single message and trip early.
        int attemptsMade = Math.min(attempt, maxAttempts);
        if (lastError == null) {
            // Only reachable if the loop never ran, which the start-up range check rules out.
            lastError = new IllegalStateException(
                    "Transactional send made no attempt (producerRetryMaxAttempts=" + maxAttempts + ")");
        }
        java.util.Map<String, String> ctx = new java.util.LinkedHashMap<>();
        ctx.put("topic", topic);
        ctx.put("batchMode", batchMode);
        ctx.put("transactionalId", transactionalId != null ? transactionalId : "(not yet assigned)");
        ctx.put("slotId", String.valueOf(slotId));
        String topicHash = txnPool.topicHash();
        ctx.put("topicHash", topicHash != null ? topicHash : "(not yet computed)");
        ctx.put("retryAttempts", String.valueOf(attemptsMade));
        ctx.put("txnPhase", String.valueOf(phase));
        ctx.put("stopReason", String.valueOf(stopReason));
        ctx.put("totalElapsedMs", String.valueOf(System.currentTimeMillis() - startedAtMs));
        CorrelationHelper.addTo(ctx, in.getExchange());

        logRetryOutcome(attemptsMade, maxAttempts, stopReason, phase, lastError, topic,
                System.currentTimeMillis() - startedAtMs, ProducerPath.TRANSACTIONAL);

        tracingHelper.traceError(in.getExchange(), lastError, ctx);
        handleTxnSendFailure(lastError, ctx);
        throw sendFailure("Failed to send transactional batch to", topic, lastError);
    }

    /** @return false if the wait was interrupted, in which case no further attempt may be made */
    private boolean sleepBetweenAttempts(long delayMs) {
        if (delayMs <= 0) {
            return true;
        }
        try {
            Thread.sleep(delayMs);
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Reports one failed attempt that will be repeated.
     *
     * <p>ERROR, like every other retry event, because the CPI tenant trace file receives only
     * ERROR — a retry logged at WARN or INFO would be invisible in exactly the production incident
     * it exists to explain. This is bounded (at most {@code producerRetryMaxAttempts} lines per
     * message, capped at 5, and additionally capped by the budget) and only occurs during a real
     * fault, so it reports an event rather than a pulse.
     */
    private void logRetryAttempt(int attempt, int maxAttempts, ProducerRetryPolicy.TxnPhase phase,
                                 Exception e, String topic, String transactionalId, int slotId,
                                 long budgetDeadlineMs) {
        AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.retry.attempt")
                .with("attempt", attempt)
                .with("maxAttempts", maxAttempts)
                .with("txnPhase", phase)
                .with("classification", KafkaErrorHelper.classify(e))
                .with("delaySeconds", endpoint.getProducerRetryDelaySeconds())
                .with("remainingBudgetMs", Math.max(0L, budgetDeadlineMs - System.currentTimeMillis()))
                .with("topic", topic)
                .withOptional("transactionalId", transactionalId)
                .with("slotId", slotId)
                .with("producerPath", ProducerPath.TRANSACTIONAL), e);
    }

    /**
     * Reports the end of a retry run that did not succeed.
     *
     * <p>Two shapes, both carrying {@code attempts} and {@code recovered} under the same names as
     * {@code producer.retry.effect}, so "how often does the retry actually rescue a message" is a
     * ratio of two greppable lines rather than a manual correlation:
     * {@code producer.retry.exhausted} when every attempt was used up, {@code producer.retry.skipped}
     * when the failure was not retry-eligible in the first place.
     *
     * <p>Nothing is written when the feature is off ({@code producerRetryMaxAttempts=1}) — every
     * ordinary failure would otherwise gain a second, contentless line.
     */
    private void logRetryOutcome(int attempts, int maxAttempts,
                                 ProducerRetryPolicy.StopReason stopReason,
                                 ProducerRetryPolicy.TxnPhase phase, Exception lastError,
                                 String topic, long totalElapsedMs, ProducerPath producerPath) {
        if (maxAttempts <= 1) {
            return;
        }
        boolean exhausted = stopReason == ProducerRetryPolicy.StopReason.ATTEMPTS_EXHAUSTED
                || stopReason == ProducerRetryPolicy.StopReason.BUDGET_EXHAUSTED;
        AdapterDiagnostics.Event event = AdapterDiagnostics
                .event(exhausted ? "producer.retry.exhausted" : "producer.retry.skipped")
                .with("attempts", attempts)
                .with("stopReason", stopReason)
                .with("txnPhase", phase)
                .with("classification", KafkaErrorHelper.classify(lastError))
                .with("totalElapsedMs", totalElapsedMs)
                .with("recovered", false)
                .with("topic", topic)
                .with("producerPath", producerPath);
        AdapterDiagnostics.error(LOG, event, lastError);
    }

    /**
     * Records that a send only succeeded because of a retry: as a diagnostic event, as a response
     * header for the iFlow, and in the Message Processing Log so the message is findable in
     * monitoring without reading the tenant trace.
     */
    private void reportRetryRecovery(Message in, String topic, int attempts, long totalElapsedMs,
                                     ProducerPath producerPath) {
        if (attempts <= 1) {
            return;
        }
        in.setHeader("CamelKafkaPlusRetryAttempts", attempts);
        AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.retry.effect")
                .with("attempts", attempts)
                .with("totalElapsedMs", totalElapsedMs)
                .with("recovered", true)
                .with("topic", topic)
                .with("producerPath", producerPath));
        tracingHelper.annotateRetrySuccess(in.getExchange(), attempts);
    }

    private java.util.List<BatchRecord> parseBatchRecords(String body, String batchMode) {
        if ("JSON_ARRAY".equalsIgnoreCase(batchMode)) {
            return BatchParser.parseJson(body);
        } else if ("XML_LIST".equalsIgnoreCase(batchMode)) {
            return BatchParser.parseXml(body);
        }
        throw ReceiverConfigValidator.unknownBatchMode(batchMode);
    }

    private ProducerBatchHelper.ByteSerializer buildBatchValueSerializer() {
        final AvroSerializerHelper helper = avroHelperIfConfigured();
        if (helper != null) {
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

        // Read the body once. A stream body that is not cached can be read only once: validating it
        // as a String and then reading it again for the send used to send it empty (#246).
        byte[] bodyBytes = readBody(in);

        // JSON Schema validation of outbound message (if enabled), on the bytes that are sent,
        // decoded with the exchange charset as Camel's String conversion of the body does
        if (jsonSchemaValidator != null && bodyBytes != null) {
            String validationError = jsonSchemaValidator.validate(
                    new String(bodyBytes, ExchangeHelper.getCharsetName(exchange)));
            if (validationError != null) {
                if (endpoint.isJsonSchemaReportError()) {
                    tracingHelper.traceOutbound(exchange, bodyBytes);
                }
                throw new RuntimeException("Outbound " + validationError);
            }
        }

        // Serialize value
        byte[] value = serializeValue(topic, bodyBytes);

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

        // Send synchronously to ensure delivery before IFlow continues.
        //
        // Two retry layers are stacked here on purpose. MonitorFaultRetry below catches the
        // KAFKA-10902 JVM defect in the millisecond range on the send() call itself; the outer loop
        // catches broker outages in the second range. Both are covered by the same total budget.
        //
        // The shared producer is reused across attempts, and an attempt is only repeated when the
        // record provably never reached the producer's buffer (send() threw, or returned Kafka's
        // FutureFailure). A failure after buffering may already be on the broker, and a second send()
        // gets a new sequence number the broker does not deduplicate — see ProducerRetryPolicy.
        // Each attempt reads the shared producer anew, so an attempt after a rebuild uses the new one.
        int maxAttempts = endpoint.getProducerRetryMaxAttempts();
        long delayMs = endpoint.getProducerRetryDelaySeconds() * 1000L;
        long startedAtMs = System.currentTimeMillis();
        long budgetDeadlineMs = startedAtMs + endpoint.getProducerRetryTotalBudgetSeconds() * 1000L;
        Exception lastError = null;
        ProducerRetryPolicy.StopReason stopReason = ProducerRetryPolicy.StopReason.RETRY_DISABLED;
        int attempt = 0;

        for (attempt = 1; attempt <= maxAttempts; attempt++) {
            // Outside the try: a producer that cannot be built, or is not built after the stop, fails
            // the exchange as it does before the first attempt, not as a send failure.
            KafkaProducer<byte[], byte[]> producer = sharedProducer();
            // Stays true when send() itself throws: then nothing was buffered.
            boolean failedBeforeBuffering = true;
            try {
                long deadlineMs = sendGuard.newDeadline();
                Future<RecordMetadata> future = MonitorFaultRetry.execute(
                        () -> producer.send(record),
                        new MonitorFaultRetry.Budget(),
                        deadlineMs,
                        topic,
                        0);
                failedBeforeBuffering = ProducerRetryPolicy.failedBeforeBuffering(future);
                RecordMetadata metadata = sendGuard.await(future, deadlineMs,
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

                reportRetryRecovery(in, topic, attempt, System.currentTimeMillis() - startedAtMs,
                        ProducerPath.SHARED);
                recordSendSuccess();
                LOG.debug("[CPI-KAFKA-PLUS-DIAG] Message sent to topic '{}' partition {} offset {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
                return;
            } catch (Exception e) {
                lastError = reclassifyIfProducerReplaced(e, producer);
                ProducerRetryPolicy.Decision decision = ProducerRetryPolicy.decideSingle(
                        lastError, failedBeforeBuffering, attempt, maxAttempts, delayMs, budgetDeadlineMs,
                        System.currentTimeMillis(), endpoint.isEnableIdempotence());
                if (!decision.isRetry()) {
                    stopReason = decision.stopReason();
                    break;
                }
                logSingleRetryAttempt(attempt, maxAttempts, lastError, topic, budgetDeadlineMs);
            }

            if (!sleepBetweenAttempts(delayMs)) {
                stopReason = ProducerRetryPolicy.StopReason.INTERRUPTED;
                break;
            }
        }

        int attemptsMade = Math.min(attempt, maxAttempts);
        long totalElapsedMs = System.currentTimeMillis() - startedAtMs;
        if (lastError == null) {
            // Only reachable if the loop never ran, which the start-up range check rules out.
            lastError = new IllegalStateException(
                    "Send made no attempt (producerRetryMaxAttempts=" + maxAttempts + ")");
        }
        Map<String, String> ctx = new java.util.LinkedHashMap<>();
        ctx.put("topic", topic);
        ctx.put("sendMode", "SINGLE");
        ctx.put("retryAttempts", String.valueOf(attemptsMade));
        ctx.put("stopReason", String.valueOf(stopReason));
        ctx.put("totalElapsedMs", String.valueOf(totalElapsedMs));
        CorrelationHelper.addTo(ctx, exchange);

        // Exactly once, with the last error — feeding the failure handler per attempt would inflate
        // the consecutive-failure counter that drives the producer rebuild and the node-fault
        // escalation.
        logRetryOutcome(attemptsMade, maxAttempts, stopReason, null, lastError, topic,
                totalElapsedMs, ProducerPath.SHARED);
        tracingHelper.traceError(exchange, lastError, ctx);
        handleSendFailure(lastError, "producer.single.send", ctx);
        throw sendFailure("Failed to send message to", topic, lastError);
    }

    /** Single-path counterpart of {@link #logRetryAttempt}; there is no transaction phase here. */
    private void logSingleRetryAttempt(int attempt, int maxAttempts, Exception e, String topic,
                                       long budgetDeadlineMs) {
        AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.retry.attempt")
                .with("attempt", attempt)
                .with("maxAttempts", maxAttempts)
                .with("txnPhase", "N/A")
                .with("classification", KafkaErrorHelper.classify(e))
                .with("delaySeconds", endpoint.getProducerRetryDelaySeconds())
                .with("remainingBudgetMs", Math.max(0L, budgetDeadlineMs - System.currentTimeMillis()))
                .with("topic", topic)
                .with("producerPath", ProducerPath.SHARED), e);
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

    /**
     * The topic to send to: a {@code CamelKafkaTopic} header overrides the configured one — unless it
     * is still the value a batch receiver earlier in the same exchange left as its response. That
     * response used to redirect the next receiver to the previous receiver's topic (#185). A receiver
     * without a topic of its own keeps following the header, as before.
     */
    // Package-private for ProducerTopicHeaderTest.
    String targetTopic(Exchange exchange) {
        String header = exchange.getIn().getHeader("CamelKafkaTopic", String.class);
        String configured = endpoint.getEffectiveTopic();
        boolean ownResponse = header != null && header.equals(
                exchange.getProperty(ProducerBatchHelper.RESPONSE_TOPIC_PROPERTY, String.class));
        if (ownResponse && configured != null && !configured.trim().isEmpty()) {
            header = null;
        }
        String topic = resolveTopic(exchange, header);
        if (topic == null || topic.isEmpty()) {
            topic = resolveTopic(exchange, configured);
        }
        return topic;
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

    /** The body as bytes; read it only once, see {@link #processSingle}. */
    private static byte[] readBody(Message message) {
        byte[] bodyBytes = message.getBody(byte[].class);
        if (bodyBytes == null) {
            String bodyStr = message.getBody(String.class);
            if (bodyStr != null) {
                bodyBytes = bodyStr.getBytes(StandardCharsets.UTF_8);
            }
        }
        return bodyBytes;
    }

    private byte[] serializeValue(String topic, byte[] bodyBytes) {
        if (bodyBytes == null) {
            return null;
        }

        AvroSerializerHelper helper = avroHelperIfConfigured();
        if (helper != null) {
            String jsonData = new String(bodyBytes, StandardCharsets.UTF_8);
            return helper.serialize(topic, jsonData);
        }

        return bodyBytes;
    }

    /**
     * The Avro helper when Avro value serialization is configured, otherwise {@code null}.
     *
     * <p>Throws instead of returning {@code null} when Avro is configured but the helper is missing:
     * falling back to the raw body would send plain JSON to consumers that expect Confluent Avro,
     * and report status OK.
     */
    private AvroSerializerHelper avroHelperIfConfigured() {
        if (!endpoint.isSchemaRegistryEnabled() || !endpoint.isAvroValueSerialization()) {
            return null;
        }
        AvroSerializerHelper helper = avroHelper;
        if (helper == null) {
            throw new IllegalStateException("Avro value serialization is configured, but the Avro "
                    + "serializer is not initialized; refusing to send the unserialized body.");
        }
        return helper;
    }

    private byte[] serializeKey(String topic, String keyStr) {
        if (keyStr == null || keyStr.isEmpty()) {
            return null;
        }

        return keyStr.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Copies the exchange headers that {@code allowedHeaders} lets through to the record. A
     * {@code byte[]} value is sent as it is, any other value as the UTF-8 bytes of its text.
     */
    void addRecordHeaders(ProducerRecord<byte[], byte[]> record, Message message) {
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
            if (val instanceof byte[]) {
                record.headers().add(name, (byte[]) val);
            } else if (val != null) {
                record.headers().add(name, val.toString().getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    void recordSendSuccess() {
        boolean wasRecovering = consecutiveSendFailures > 0;
        consecutiveSendFailures = 0;

        // e4: Emit rebuild effect if this is the first success after a rebuild
        if (pendingRebuildEffect) {
            pendingRebuildEffect = false;
            // Guard topic access on what is essentially a success path that follows a failure
            String topic;
            try {
                topic = endpoint.getEffectiveTopic();
            } catch (Exception e) {
                topic = "<unavailable>";
            }
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.rebuild.effect")
                    .with("outcome", "RECOVERED")
                    .with("producerPath", ProducerPath.SHARED)
                    .withOptional("clientId", resolvedClientId)
                    .withOptional("topic", topic)
                    .with("thread", Thread.currentThread().getName()), null);
        }

        if (wasRecovering) {
            tracingHelper.publishConnectionStatus(true, null);
            // Guard topic access — recovery logging is on a path that follows failures
            String topic;
            try {
                topic = endpoint.getEffectiveTopic();
            } catch (Exception e) {
                topic = "<unavailable>";
            }
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.send.recovered")
                    .with("producerPath", ProducerPath.SHARED)
                    .withOptional("clientId", resolvedClientId)
                    .withOptional("topic", topic)
                    .with("thread", Thread.currentThread().getName()), null);
        }
        // e4: Reset rebuild tracking on success
        rebuildAttemptsSinceSuccess = 0;

        // c7: Track for the periodic success heartbeat
        sendsSinceLastHeartbeat++;
        maybeLogSuccessHeartbeat();
    }

    /**
     * c7: Periodic success heartbeat for the SHARED producer path.
     *
     * <p>Without a baseline, "no errors" and "nothing ran" look identical in the trace. This
     * method emits a low-volume success line so a healthy producer is provably alive. It matches
     * the throttling approach used for the consumer's emit-cycle heartbeat: one line per 5 minutes,
     * emitted immediately on a state change (healthy→unhealthy or first success).
     *
     * <p>Called from {@link #recordSendSuccess()}, so only successful sends trigger it.
     */
    private void maybeLogSuccessHeartbeat() {
        long now = System.currentTimeMillis();
        boolean healthy = true;
        boolean stateChanged = !lastHeartbeatHealthy && healthy;
        boolean intervalElapsed = (now - lastHeartbeatMs) >= HEARTBEAT_INTERVAL_MS;

        if (stateChanged || intervalElapsed) {
            // Guard topic access even on success path for consistency
            String topic;
            try {
                topic = endpoint.getEffectiveTopic();
            } catch (Exception e) {
                topic = "<unavailable>";
            }
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.heartbeat")
                    .with("status", "HEALTHY")
                    .with("producerPath", ProducerPath.SHARED)
                    .withOptional("clientId", resolvedClientId)
                    .with("sendsSinceLastHeartbeat", sendsSinceLastHeartbeat)
                    .withOptional("topic", topic)
                    .with("thread", Thread.currentThread().getName()), null);
            lastHeartbeatMs = now;
            lastHeartbeatHealthy = healthy;
            sendsSinceLastHeartbeat = 0;
        }
    }

    /**
     * A send whose producer was closed under it, by the rebuild after another send's failure or by
     * the route stopping, fails with "Cannot perform operation after producer has been closed",
     * "Producer is closed forcefully" or the like. Unrecognised, that is {@code UNKNOWN_FATAL}: a
     * permanent failure that asks for one more rebuild and counts towards the node-fault escalation
     * (#246). It says nothing about the broker or the record, only that the send lost a race against
     * the replacement of the client, so it is reported as transient instead. Recognised failures
     * keep their classification, and so does the KAFKA-10902 monitor fault, which the escalation
     * exists for.
     *
     * @param used the producer the failed send ran on
     */
    private Exception reclassifyIfProducerReplaced(Exception failure, KafkaProducer<byte[], byte[]> used) {
        if (used == kafkaProducer
                || KafkaErrorHelper.classify(failure) != Classification.UNKNOWN_FATAL
                || KafkaErrorHelper.isMetadataMonitorFault(failure)) {
            return failure;
        }
        return new ProducerReplacedException(failure);
    }

    /**
     * Records a failure of the shared, non-transactional producer.
     *
     * <p>The failure is <b>always</b> logged at ERROR. It previously was not: logging happened only
     * when the cause matched a three-entry "fatal" allow-list or after a number of <i>consecutive</i>
     * failures. A failure that was neither on the allow-list nor consecutive — because successful
     * sends in between reset the counter via {@link #recordSendSuccess()} — produced no log line at
     * all, which is how a production incident could occur repeatedly while this adapter stayed
     * completely silent about it.
     *
     * <p>Whether {@link #triggerReconnect()} rebuilds the producer is decided by the classification of
     * the failure and a backoff between rebuilds, not by a failure count. Both decisions are reported
     * as fields so the log states why it did or did not rebuild.
     *
     * @param operation stable, greppable name of the failing activity
     * @param context   the same key/value context handed to the Message Processing Log, so trace and
     *                  MPL cannot drift apart
     */
    // Package-private so the regression test can prove that a single, first, unclassified failure
    // still produces exactly one ERROR line.
    void handleSendFailure(Exception e, String operation, Map<String, String> context) {
        consecutiveSendFailures++;
        tracingHelper.publishConnectionStatus(false, e);

        // e4: Use classification API instead of the deprecated isFatalKafkaException
        Classification classification = KafkaErrorHelper.classify(e);
        CpiKafkaPlusErrorCode errorCode = CpiKafkaPlusErrorCode.fromThrowable(e);

        // e4: Rebuild decision driven by classification:
        // - FATAL_PRODUCER_UNUSABLE / UNKNOWN_FATAL justify a rebuild
        // - RETRIABLE / FATAL_DATA_ERROR do not (a record that is too large will be exactly
        //   as too large on a fresh producer, and rebuilding on it would be a self-inflicted
        //   outage under a poison message)
        boolean classificationJustifiesRebuild =
                classification == Classification.FATAL_PRODUCER_UNUSABLE
                || classification == Classification.UNKNOWN_FATAL;

        // e4: Guard against rebuild storm - don't rebuild on every record of a failing batch
        long now = System.currentTimeMillis();
        boolean rebuildBackoffElapsed = (now - lastRebuildAttemptMs) >= REBUILD_BACKOFF_MS;
        boolean shouldRebuild = classificationJustifiesRebuild && rebuildBackoffElapsed;

        AdapterDiagnostics.Event event = AdapterDiagnostics.event(operation)
                .with("producerPath", ProducerPath.SHARED)
                .withOptional("clientId", resolvedClientId)
                .with("classification", classification)
                .with("errorCode", errorCode.code())
                .with("consecutiveFailures", consecutiveSendFailures)
                .with("rebuildJustified", classificationJustifiesRebuild)
                .with("rebuildBackoffElapsed", rebuildBackoffElapsed)
                .with("rebuildTriggered", shouldRebuild)
                .with("thread", Thread.currentThread().getName());
        if (context != null) {
            context.forEach(event::with);
        }
        AdapterDiagnostics.error(LOG, event, e);

        // e4b: Check for node-level fault escalation
        nodeFaultEscalation.checkNodeFaultEscalation(classification, errorCode, e);

        if (shouldRebuild) {
            triggerReconnectWithMeasurement();
        }
    }

    /**
     * e4: Rebuilds the producer and reports what happened.
     *
     * <p>The rebuild is lazy: the producer is closed and discarded here, and the next send builds a
     * new one. The event therefore reports whether there was a producer to discard
     * ({@code producerDiscarded}), not whether a new one exists; the former
     * {@code producerRecreated} read the field right after it had been cleared and was always
     * {@code false} (#246). Whether the rebuild <i>helped</i> is determined by the next send: if that
     * succeeds, {@link #recordSendSuccess()} emits {@code producer.rebuild.effect} with
     * {@code outcome=RECOVERED}; if the new producer cannot be built, {@code producer.init.failed}
     * says why; if it is built but the send fails, the failure line follows the rebuild line.
     */
    private void triggerReconnectWithMeasurement() {
        lastRebuildAttemptMs = System.currentTimeMillis();
        rebuildAttemptsSinceSuccess++;
        pendingRebuildEffect = true; // Track whether next send shows recovery

        long rebuildStartMs = System.currentTimeMillis();
        boolean producerDiscarded = triggerReconnect();
        long rebuildDurationMs = System.currentTimeMillis() - rebuildStartMs;

        AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.rebuild.outcome")
                .with("producerDiscarded", producerDiscarded)
                .with("consequence", "the next send builds a new producer")
                .with("durationMs", rebuildDurationMs)
                .with("attemptsSinceLastSuccess", rebuildAttemptsSinceSuccess)
                .with("producerPath", ProducerPath.SHARED)
                .withOptional("clientId", resolvedClientId)
                .with("thread", Thread.currentThread().getName()), null);
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
            // Recovery after failure is a low-volume, high-value event that justifies ERROR.
            // INFO does not reach the CPI tenant trace file, so recovery would be invisible.
            String topic;
            try {
                topic = endpoint.getEffectiveTopic();
            } catch (Exception e) {
                topic = "<unavailable>";
            }
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.transactional.send.recovered")
                    .with("producerPath", ProducerPath.TRANSACTIONAL)
                    .withOptional("topic", topic)
                    .with("thread", Thread.currentThread().getName()), null);
        }
    }

    /**
     * Records a failure of the transactional path. Logs the {@code Throwable} itself rather than
     * only {@code getClass().getSimpleName()}, which discarded the message, the cause chain and the
     * stack trace — the three things needed to tell apart a broker problem, a fencing problem and a
     * client-side defect.
     */
    void handleTxnSendFailure(Exception e, Map<String, String> context) {
        consecutiveTxnSendFailures++;
        Classification classification = KafkaErrorHelper.classify(e);
        CpiKafkaPlusErrorCode errorCode = CpiKafkaPlusErrorCode.fromThrowable(e);

        AdapterDiagnostics.Event event = AdapterDiagnostics.event("producer.transactional.batch.send")
                .with("producerPath", ProducerPath.TRANSACTIONAL)
                .with("classification", classification)
                .with("errorCode", errorCode.code())
                .with("consecutiveFailures", consecutiveTxnSendFailures)
                .with("thread", Thread.currentThread().getName());
        if (context != null) {
            context.forEach(event::with);
        }
        AdapterDiagnostics.error(LOG, event, e);
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

        Throwable probeFailure = topicChecker.lastProbeFailure(topic);
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

    /** Bootstrap and security settings for the topic probe, as this producer wires them. */
    // Package-private for ProducerTopicProbeConfigTest.
    Properties buildTopicCheckProperties() {
        return topicChecker.buildTopicCheckProperties();
    }

    /**
     * Replaces only the producer. The serialization helpers do not depend on the connection and stay
     * as they are: clearing the Avro helper here, while {@code helpersInitialized} stayed true, made
     * every later message go out as raw JSON.
     *
     * @return whether there was a producer to discard
     */
    private boolean triggerReconnect() {
        return closeProducerQuietly();
    }

    /**
     * Detaches the producer under the monitor that {@link #ensureInitialized()} builds under, then
     * closes it outside. Clearing the field only after the close used to wipe out a producer another
     * thread had built in the meantime, leaving {@code initialized=true} without a producer. Closing
     * outside the monitor keeps the 5 s close from blocking every other sender.
     *
     * @return whether there was a producer to discard
     */
    private boolean closeProducerQuietly() {
        KafkaProducer<byte[], byte[]> detached;
        synchronized (this) {
            initialized = false;
            detached = kafkaProducer;
            kafkaProducer = null;
            if (detached != null) {
                // A replacement client starts with an empty metadata cache, so the pre-warm has to
                // happen again for every topic.
                topicChecker.onProducerReplaced();
            }
        }
        if (detached != null) {
            try {
                detached.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] Error closing Kafka producer: {}", e.getMessage());
            }
        }
        return detached != null;
    }

    private void closeAvroHelperQuietly() {
        if (avroHelper != null) {
            try {
                avroHelper.close();
            } catch (Exception e) {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] Error closing Avro helper: {}", e.getMessage());
            }
            avroHelper = null;
        }
    }

    /**
     * A send failure caused by the shared producer being closed while the send was using it. A
     * Kafka {@link org.apache.kafka.common.errors.RetriableException}, so that every classification
     * of the failure (the retry decision, the rebuild decision, the node-fault escalation) reads it
     * as transient; the original failure is the cause.
     */
    static final class ProducerReplacedException extends org.apache.kafka.common.errors.RetriableException {

        private static final long serialVersionUID = 1L;

        ProducerReplacedException(Exception cause) {
            super("The shared Kafka producer was closed while this send was using it (another send's "
                    + "failure had it rebuilt, or the route was stopping); the next send uses a new "
                    + "producer. " + KafkaErrorHelper.describeChain(cause), cause);
        }
    }
}
