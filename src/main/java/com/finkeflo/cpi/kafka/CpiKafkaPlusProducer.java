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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import com.finkeflo.cpi.kafka.KafkaErrorHelper.Classification;
import com.finkeflo.cpi.kafka.ProducerBatchHelper.ProducerPath;

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
     * Window in which a failed metadata pre-warm may be retried. Short on purpose: the KAFKA-10902
     * monitor fault throws immediately and is worth another attempt, whereas a genuine metadata
     * timeout has already spent {@code max.block.ms} and must not be multiplied.
     */
    private static final long PREWARM_RETRY_WINDOW_MS = 5_000L;
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
    /**
     * Topics whose metadata this producer instance has already tried to fetch up front. Cleared
     * whenever the producer is replaced, because a new client starts with an empty metadata cache.
     */
    private final java.util.Set<String> prewarmedTopics =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<String, Boolean>());
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private AvroSerializerHelper avroHelper;
    private AdapterTracingHelper tracingHelper;
    private JsonSchemaValidator jsonSchemaValidator;

    private java.util.concurrent.Semaphore txnSlotSemaphore;
    private boolean[] txnSlotInUse;
    private String resolvedMemberSuffix;
    /**
     * Short (8 hex-char) SHA-256 digest of the target topic name. Included in every
     * {@code transactional.id} so that two producers with the same
     * {@code transactionalIdPrefix} but different target topics never share an id —
     * which would cause them to fence each other on every call.
     */
    private String topicHash;

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
    // c4: Slow metadata warn threshold — avoids log noise on the success path while
    // still surfacing pathological fetches. 1 second is well above normal (<50ms)
    // but below the timeout that would cause a hard failure (max.block.ms).
    // ─────────────────────────────────────────────────────────────────────────────
    private static final long SLOW_METADATA_THRESHOLD_MS = 1000L;

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

    // ─────────────────────────────────────────────────────────────────────────────
    // e4b: Node-level fault escalation — if the same fault (keyed by exception class +
    // error code) recurs more than N times within T minutes despite mitigation, escalate
    // to make the node problem visible.
    // Thresholds: 5 occurrences in 20 minutes (observed incident was 5 in 18 min).
    // ─────────────────────────────────────────────────────────────────────────────
    private static final int NODE_FAULT_ESCALATION_COUNT = 5;
    private static final long NODE_FAULT_ESCALATION_WINDOW_MS = 20 * 60 * 1000L;
    // c6b: Thread dump caps — AdapterDiagnostics truncates at 8K chars; stay well under.
    // Prefer BLOCKED/WAITING threads (informative for monitor issues) over RUNNABLE.
    private static final int THREAD_DUMP_MAX_THREADS = 20;
    private static final int THREAD_DUMP_MAX_FRAMES = 10;
    private static final int THREAD_DUMP_MAX_CHARS = 5000;
    /** Lock for atomic escalation decisions. The incident involved 5 threads — volatile++ would race. */
    private final Object nodeFaultLock = new Object();
    /** Fault identity = exceptionClass + ":" + errorCode. Keyed to make "same fault" claim true. */
    private String nodeFaultIdentity = null;
    private long nodeFaultWindowStartMs = 0L;
    private int nodeFaultCountInWindow = 0;
    private boolean nodeFaultEscalated = false;

    public CpiKafkaPlusProducer(CpiKafkaPlusEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
        this.sendGuard = ProducerSendGuard.forEndpoint(endpoint);
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

            // A short hash of the target topic is included in every transactional.id so that two
            // producers configured with the same transactionalIdPrefix but different target topics
            // (a common copy-paste pattern) never share an id. Shared ids cause constant mutual
            // fencing: each new producer bumps the epoch and invalidates all concurrent ones.
            topicHash = computeTopicHash(endpoint.getEffectiveTopic());

            // Validate that the resulting id fits within a safe length budget before any producer
            // is created. Prefix + hash + memberSuffix + slotId + separators must stay short enough
            // to be handled by all Kafka broker versions without truncation.
            String exampleId = endpoint.getTransactionalIdPrefix() + "-" + topicHash
                    + "-" + resolvedMemberSuffix + "-0";
            if (exampleId.length() > 249) {
                throw new IllegalArgumentException(
                        "The computed transactional.id '" + exampleId + "' is " + exampleId.length()
                        + " characters long. Please shorten transactionalIdPrefix to keep it under 249 characters.");
            }

            txnSlotSemaphore = new java.util.concurrent.Semaphore(slots, true);
            txnSlotInUse = new boolean[slots];
            LOG.info("[CPI-KAFKA-PLUS-DIAG] Transactional batching enabled with max {} concurrent transactions. "
                    + "Prefix: {}, topicHash: {}, memberSuffix: {}, transactionV2: {}, example transactional.id: {}",
                    slots, endpoint.getTransactionalIdPrefix(), topicHash, resolvedMemberSuffix,
                    endpoint.isTransactionV2Enabled(), exampleId);
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
            // Before any Kafka client exists: a plaintext protocol against a TLS-only broker makes
            // Kafka allocate the broker's TLS alert as a 352 MB frame until jvmkill takes the node
            // down. That cannot be caught afterwards, so it must not be started.
            TlsListenerProbe.assertNoTlsListener(endpoint.getBootstrapServers(),
                    endpoint.getSecurityProtocol());
            Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] ensureInitialized: producer properties built, security={}, sasl={}",
                    endpoint.getSecurityProtocol(), endpoint.getSaslMechanism());
            kafkaProducer = BundleBackedClassLoader.withBundleClassLoader(getClass(),
                    () -> new KafkaProducer<>(props,
                            new ByteArraySerializer(), new ByteArraySerializer()));

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
        prewarmTopicMetadata(topic);

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
                        in, this::addRecordHeaders, valueSerializer, null, sendGuard,
                        ProducerPath.SHARED, resolvedClientId);

                ProducerBatchHelper.setResponseHeadersAndBody(in, topic, batchMode, result);
                recordSendSuccess();
            } catch (Exception e) {
                Map<String, String> ctx = new java.util.LinkedHashMap<>();
                ctx.put("topic", topic);
                ctx.put("batchMode", batchMode);
                ctx.put("recordCount", String.valueOf(records.size()));
                CorrelationHelper.addTo(ctx, exchange);
                tracingHelper.traceError(exchange, e, ctx);
                handleSendFailure(e, "producer.batch.send", ctx);
                throw sendFailure("Failed to send batch to", topic, e);
            }
        }
    }

    private void sendTransactionalBatch(Message in, String topic, String batchMode,
                                        java.util.List<BatchRecord> records, String fallbackKey,
                                        Integer partition, Long timestamp,
                                        ProducerBatchHelper.ByteSerializer valueSerializer) throws Exception {
        // The transactional producer is created outside ensureInitialized(), so it needs the same
        // protection against a plaintext protocol on a TLS-only broker. The probe is cached per
        // bootstrap/security config, and it runs before acquiring a transaction slot so a first
        // inconclusive timeout on a silent endpoint cannot hold scarce slots.
        TlsListenerProbe.assertNoTlsListener(endpoint.getBootstrapServers(),
                endpoint.getSecurityProtocol());

        int slotId = -1;
        String transactionalId = null;
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

            // resolvedMemberSuffix and topicHash are resolved once (fail-fast) in doStart() — never null here.
            transactionalId = endpoint.getTransactionalIdPrefix() + "-" + topicHash
                    + "-" + resolvedMemberSuffix + "-" + slotId;

            java.util.Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);
            props.put(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

            // TODO [tech-debt] The UI option "transactionV2Enabled" and its mapping to
            //   transaction.two.phase.commit.enable are retained for backward compatibility
            //   with existing iFlow channel configurations.
            //   1. WHY: The option name misleadingly references "V2", which sounds like KIP-890
            //      (Transaction Protocol V2, broker-side, negotiated automatically). In fact,
            //      transaction.two.phase.commit.enable controls KIP-939 (Two-Phase Commit),
            //      a client-side opt-in for distributed transactions. Verified against
            //      kafka-clients 4.3.1 javap output and KIP-939 documentation.
            //   2. TRIGGERS: Any iFlow with transactionV2Enabled set (default=true since the
            //      option was added) will pass that value straight through to the producer.
            //   3. REMOVE: After a major version bump with a migration guide, or when the
            //      repository owner's in-flight branch (fix/kafka-kip890-non-transactional)
            //      lands and changes the default.
            //
            // Empirical note: setting transactionV2Enabled=false (i.e., config=false) was an
            // effective workaround for IllegalMonitorStateException observed in certain Kafka
            // 4.x client versions, apparently due to a bug in the TransactionManager under
            // the 2PC protocol path. This observation is kept for diagnostic context.
            props.put(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG,
                    endpoint.isTransactionV2Enabled());

            txnProducer = BundleBackedClassLoader.withBundleClassLoader(getClass(),
                    () -> new KafkaProducer<>(props,
                            new org.apache.kafka.common.serialization.ByteArraySerializer(),
                            new org.apache.kafka.common.serialization.ByteArraySerializer()));

            txnProducer.initTransactions();
            txnProducer.beginTransaction();

            ProducerBatchHelper.BatchSendResult result = ProducerBatchHelper.sendBatch(
                    txnProducer, records, topic, fallbackKey, partition, timestamp,
                    in, this::addRecordHeaders, valueSerializer, null, sendGuard,
                    ProducerPath.TRANSACTIONAL, transactionalId);

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
            java.util.Map<String, String> ctx = new java.util.LinkedHashMap<>();
            ctx.put("topic", topic);
            ctx.put("batchMode", batchMode);
            ctx.put("transactionalId", transactionalId != null ? transactionalId : "(not yet assigned)");
            ctx.put("slotId", String.valueOf(slotId));
            ctx.put("topicHash", topicHash != null ? topicHash : "(not yet computed)");
            ctx.put("transactionV2Enabled", String.valueOf(endpoint.isTransactionV2Enabled()));
            CorrelationHelper.addTo(ctx, in.getExchange());
            tracingHelper.traceError(in.getExchange(), e, ctx);
            handleTxnSendFailure(e, ctx);
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
            Map<String, String> ctx = new java.util.LinkedHashMap<>();
            ctx.put("topic", topic);
            ctx.put("sendMode", "SINGLE");
            CorrelationHelper.addTo(ctx, exchange);
            tracingHelper.traceError(exchange, e, ctx);
            handleSendFailure(e, "producer.single.send", ctx);
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

    /**
     * Fetches the metadata for a topic once per producer instance, before any record is submitted.
     *
     * <p>This is the preventive half of the KAFKA-10902 mitigation. {@code waitOnMetadata()} only
     * blocks in {@code ProducerMetadata.awaitUpdate()} — the method carrying the monitor defect —
     * when the topic's partition count is not in the client's cache. Fetching it here means the
     * subsequent {@code send(...)} finds it present and never enters that path. Together with the
     * raised {@code metadata.max.idle.ms} (see {@link ProducerConfigFactory}), which stops an idle
     * topic from being evicted again, the vulnerable construct is left out of the hot path entirely
     * rather than merely being retried when it fails.
     *
     * <p>Doing it here rather than at producer creation covers dynamically resolved topics as well,
     * and it is done before any record exists, so a failure costs nothing and a retry is free.
     *
     * <p>Never fails the exchange. If the fetch does not succeed, the send path performs it anyway
     * and reports a far more precise error than this method could.
     */
    private void prewarmTopicMetadata(String topic) {
        if (kafkaProducer == null || topic == null || topic.isEmpty()) {
            return;
        }
        // Bounded like verifiedTopics, so a flow with unbounded dynamic topics cannot grow this set
        // without limit. Beyond the limit the pre-warm is simply skipped; the send path still works.
        if (prewarmedTopics.size() >= VERIFIED_TOPICS_CACHE_LIMIT || !prewarmedTopics.add(topic)) {
            return;
        }

        // c4: Measure the metadata fetch duration. This is the exact place the KAFKA-10902 incident
        // happened (IllegalMonitorStateException inside ProducerMetadata.awaitUpdate). Tracking
        // the duration makes a slow or blocking metadata fetch visible before it turns into a hard
        // failure. On failure we always emit; on success we only emit if the wait exceeded the
        // threshold (respects the log volume budget while still surfacing pathological fetches).
        long metadataStartMs = System.currentTimeMillis();
        try {
            // A short retry deadline on purpose: the monitor fault throws immediately and is worth
            // retrying, while a genuine metadata timeout has already consumed max.block.ms and must
            // not be multiplied by retrying it.
            MonitorFaultRetry.execute(() -> kafkaProducer.partitionsFor(topic),
                    new MonitorFaultRetry.Budget(),
                    System.currentTimeMillis() + PREWARM_RETRY_WINDOW_MS,
                    topic, -1);

            long metadataWaitMs = System.currentTimeMillis() - metadataStartMs;
            // Only log slow pre-warms to avoid log noise on the success path.
            if (metadataWaitMs > SLOW_METADATA_THRESHOLD_MS) {
                LOG.error("[CPI-KAFKA-PLUS-DIAG] producer.metadata.prewarm: outcome=SLOW, "
                        + "topic={}, metadataWaitMs={}, clientId={}, producerPath=SHARED, thread={}",
                        topic, metadataWaitMs, resolvedClientId, Thread.currentThread().getName());
            }
        } catch (Exception e) {
            long metadataWaitMs = System.currentTimeMillis() - metadataStartMs;
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.metadata.prewarm")
                    .with("outcome", "FAILED")
                    .with("topic", topic)
                    .with("metadataWaitMs", metadataWaitMs)
                    .with("producerPath", ProducerPath.SHARED)
                    .withOptional("clientId", resolvedClientId)
                    .with("thread", Thread.currentThread().getName()), e);
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
     * Records a failure of the shared, non-transactional producer.
     *
     * <p>The failure is <b>always</b> logged at ERROR. It previously was not: logging happened only
     * when the cause matched a three-entry "fatal" allow-list or after
     * {@code MAX_CONSECUTIVE_SEND_FAILURES} <i>consecutive</i> failures. A failure that was neither
     * on the allow-list nor consecutive — because successful sends in between reset the counter via
     * {@link #recordSendSuccess()} — produced no log line at all, which is how a production incident
     * could occur repeatedly while this adapter stayed completely silent about it.
     *
     * <p>The threshold now governs {@link #triggerReconnect()} only, which is what it was actually
     * for. Both decisions are reported as fields so the log states why it did or did not reconnect.
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
        checkNodeFaultEscalation(classification, errorCode, e);

        if (shouldRebuild) {
            triggerReconnectWithMeasurement();
        }
    }

    /**
     * e4b: Escalate loudly when the same fault recurs more than N times within T minutes
     * on this JVM despite mitigation. This indicates a node-level problem that requires
     * operator attention.
     *
     * <p>Fault identity is keyed by exception class + error code, so the escalation message
     * "the same fault has recurred" is actually true. Five different faults will not escalate;
     * five occurrences of the same fault will.
     *
     * <p>Thresholds: 5 occurrences in 20 minutes. The observed incident was 5 in 18 minutes
     * across a whole worker node. We use 5 and 20 rather than 5 and 18 to avoid escalating
     * on a single retryable blip that happens to cross a window boundary.
     *
     * <p>The window reset, increment and escalation decision are synchronized because the
     * incident involved 5 concurrent threads — volatile++ races under exactly that load.
     */
    private void checkNodeFaultEscalation(Classification classification, CpiKafkaPlusErrorCode errorCode,
                                           Throwable cause) {
        // Only track producer-unusable or unknown faults (not retriable or data errors)
        if (classification != Classification.FATAL_PRODUCER_UNUSABLE
                && classification != Classification.UNKNOWN_FATAL) {
            return;
        }

        // Fault identity = exceptionClass + errorCode — so "same fault" claim is true
        String exceptionClass = (cause != null) ? cause.getClass().getName() : "unknown";
        String currentFaultIdentity = exceptionClass + ":" + errorCode.code();

        boolean shouldEscalate = false;
        int count = 0;

        synchronized (nodeFaultLock) {
            long now = System.currentTimeMillis();

            // Reset window if expired OR if a different fault is seen (start fresh tracking)
            boolean windowExpired = (now - nodeFaultWindowStartMs) > NODE_FAULT_ESCALATION_WINDOW_MS;
            boolean differentFault = nodeFaultIdentity != null && !nodeFaultIdentity.equals(currentFaultIdentity);

            if (windowExpired || differentFault) {
                nodeFaultWindowStartMs = now;
                nodeFaultCountInWindow = 0;
                nodeFaultEscalated = false;
                nodeFaultIdentity = currentFaultIdentity;
            }

            nodeFaultCountInWindow++;
            count = nodeFaultCountInWindow;

            if (count >= NODE_FAULT_ESCALATION_COUNT && !nodeFaultEscalated) {
                nodeFaultEscalated = true;
                shouldEscalate = true;
            }
        }

        if (shouldEscalate) {
            emitNodeFaultEscalation(exceptionClass, errorCode, classification, count);
        }
    }

    /**
     * Emits the node-fault escalation diagnostic with JVM state (c6) and optionally a thread
     * dump (c6b) when diagnosticsLevel=FULL.
     *
     * <p>The JVM state is cheap and always-on. The thread dump is expensive and opt-in.
     */
    private void emitNodeFaultEscalation(String exceptionClass, CpiKafkaPlusErrorCode errorCode,
                                          Classification classification, int count) {
        // Guard against endpoint.getEffectiveTopic() throwing on failure path
        String topic;
        try {
            topic = endpoint.getEffectiveTopic();
        } catch (Exception e) {
            topic = "<unavailable>";
        }

        AdapterDiagnostics.Event event = AdapterDiagnostics.event("producer.node.fault.escalation")
                .with("faultClass", exceptionClass)
                .with("errorCode", errorCode.code())
                .with("classification", classification)
                .with("countInWindow", count)
                .with("windowMinutes", NODE_FAULT_ESCALATION_WINDOW_MS / 60_000)
                .with("producerPath", ProducerPath.SHARED)
                .withOptional("clientId", resolvedClientId)
                .withOptional("topic", topic)
                .with("thread", Thread.currentThread().getName())
                .with("advice", "The same fault has recurred " + count + " times within "
                        + (NODE_FAULT_ESCALATION_WINDOW_MS / 60_000) + " minutes on this JVM despite "
                        + "mitigation. This indicates a node-level problem. The node address is available "
                        + "in the trace record's second-to-last #-separated field.");

        // c6: Lightweight JVM state — always-on, cheap
        addJvmState(event);

        // c6b: Thread dump — expensive, opt-in at FULL level only
        boolean diagnosticsFull = false;
        try {
            diagnosticsFull = endpoint.isDiagnosticsLevelFull();
        } catch (Exception e) {
            // Ignore - just don't emit dump
        }

        if (diagnosticsFull) {
            addThreadDump(event);
        }

        AdapterDiagnostics.error(LOG, event, null);
    }

    /**
     * c6: Adds lightweight JVM state to the escalation event.
     *
     * <p>Uses java.lang.management APIs which are available in Java 11 without extra dependencies.
     * Fully guarded so a failure here can never displace the diagnostic it is attached to.
     */
    private void addJvmState(AdapterDiagnostics.Event event) {
        try {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            ThreadMXBean threads = ManagementFactory.getThreadMXBean();
            MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
            MemoryUsage heap = memory.getHeapMemoryUsage();

            event.with("jvmUptimeMs", runtime.getUptime())
                 .with("jvmThreadCount", threads.getThreadCount())
                 .with("jvmPeakThreadCount", threads.getPeakThreadCount())
                 .with("jvmHeapUsedMB", heap.getUsed() / (1024 * 1024))
                 .with("jvmHeapMaxMB", heap.getMax() / (1024 * 1024))
                 .with("jvmAvailableProcessors", Runtime.getRuntime().availableProcessors());
        } catch (Exception e) {
            // Management APIs can throw or be restricted in locked-down containers
            event.with("jvmStateUnavailable", true)
                 .with("jvmStateError", e.getClass().getSimpleName());
        }
    }

    /**
     * c6b: Adds a bounded thread dump to the escalation event.
     *
     * <p>Only called when diagnosticsLevel=FULL. The dump includes lock and monitor information
     * (the entire point — this is what settles monitor-ownership questions). Output is bounded
     * hard: max 20 threads, max 10 frames per thread, max 5000 chars total. BLOCKED and WAITING
     * threads are preferred over RUNNABLE (they are the informative ones for deadlock/contention).
     *
     * <p>Fully guarded so a failure here can never displace the diagnostic it is attached to.
     */
    private void addThreadDump(AdapterDiagnostics.Event event) {
        try {
            ThreadMXBean threads = ManagementFactory.getThreadMXBean();

            // Check if monitor/synchronizer info is supported before requesting it
            boolean monitorSupported = threads.isObjectMonitorUsageSupported();
            boolean syncSupported = threads.isSynchronizerUsageSupported();

            ThreadInfo[] allThreads = threads.dumpAllThreads(monitorSupported, syncSupported);
            if (allThreads == null || allThreads.length == 0) {
                event.with("threadDumpUnavailable", true);
                return;
            }

            // Sort: BLOCKED first, then WAITING, then others — these are the informative ones
            java.util.List<ThreadInfo> sorted = new java.util.ArrayList<>(java.util.Arrays.asList(allThreads));
            sorted.sort((a, b) -> {
                int aPriority = threadStatePriority(a.getThreadState());
                int bPriority = threadStatePriority(b.getThreadState());
                return Integer.compare(aPriority, bPriority);
            });

            StringBuilder dump = new StringBuilder();
            int threadCount = 0;
            boolean truncated = false;

            for (ThreadInfo ti : sorted) {
                if (threadCount >= THREAD_DUMP_MAX_THREADS) {
                    truncated = true;
                    break;
                }
                if (dump.length() >= THREAD_DUMP_MAX_CHARS) {
                    truncated = true;
                    break;
                }

                dump.append("\n[").append(ti.getThreadName()).append("] ")
                    .append(ti.getThreadState());

                if (ti.getLockName() != null) {
                    dump.append(" on ").append(ti.getLockName());
                }
                if (ti.getLockOwnerName() != null) {
                    dump.append(" owned by [").append(ti.getLockOwnerName()).append("]");
                }

                StackTraceElement[] stack = ti.getStackTrace();
                int frameCount = Math.min(stack.length, THREAD_DUMP_MAX_FRAMES);
                for (int i = 0; i < frameCount; i++) {
                    dump.append("\n  at ").append(stack[i]);
                    if (dump.length() >= THREAD_DUMP_MAX_CHARS) {
                        truncated = true;
                        break;
                    }
                }
                if (stack.length > THREAD_DUMP_MAX_FRAMES) {
                    dump.append("\n  ... ").append(stack.length - THREAD_DUMP_MAX_FRAMES).append(" more");
                }

                threadCount++;
            }

            event.with("threadDump", dump.toString())
                 .with("threadDumpThreads", threadCount)
                 .with("threadDumpTruncated", truncated)
                 .with("threadDumpTotalThreads", allThreads.length)
                 .with("threadDumpMonitorInfoSupported", monitorSupported)
                 .with("threadDumpSyncInfoSupported", syncSupported);

        } catch (Exception e) {
            // Thread dump can fail in restricted environments
            event.with("threadDumpUnavailable", true)
                 .with("threadDumpError", e.getClass().getSimpleName());
        }
    }

    /** Returns priority for thread state sorting: lower = more informative for contention analysis. */
    private int threadStatePriority(Thread.State state) {
        switch (state) {
            case BLOCKED: return 0;      // Highest priority - waiting for monitor
            case WAITING: return 1;      // High - in Object.wait() or similar
            case TIMED_WAITING: return 2; // Medium - in sleep/wait with timeout
            default: return 3;           // RUNNABLE, NEW, TERMINATED
        }
    }

    /**
     * e4: Triggers a producer rebuild and measures/logs the outcome.
     *
     * <p>The reason this method exists (instead of just calling {@link #triggerReconnect()})
     * is that we currently rebuild and then have no idea whether it helped.
     *
     * <p>This method emits {@code producerRecreated=true/false} and the duration. Whether the
     * rebuild actually <i>helped</i> is determined by the next send: if that succeeds,
     * {@link #recordSendSuccess()} emits {@code producer.rebuild.effect} with {@code outcome=RECOVERED}.
     * If it fails, the diagnosis is that rebuilding did not help — which is visible from the
     * failure line immediately following the rebuild line.
     */
    private void triggerReconnectWithMeasurement() {
        lastRebuildAttemptMs = System.currentTimeMillis();
        rebuildAttemptsSinceSuccess++;
        pendingRebuildEffect = true; // Track whether next send shows recovery

        long rebuildStartMs = System.currentTimeMillis();
        boolean producerRecreated = false;
        Exception rebuildException = null;

        try {
            triggerReconnect();
            // A non-null producer only means it was constructed, not that it works
            producerRecreated = (kafkaProducer != null);
        } catch (Exception e) {
            rebuildException = e;
        }

        long rebuildDurationMs = System.currentTimeMillis() - rebuildStartMs;

        // Route both success and failure through AdapterDiagnostics for consistent format
        AdapterDiagnostics.Event event = AdapterDiagnostics.event("producer.rebuild.outcome")
                .with("producerRecreated", producerRecreated)
                .with("durationMs", rebuildDurationMs)
                .with("attemptsSinceLastSuccess", rebuildAttemptsSinceSuccess)
                .with("producerPath", ProducerPath.SHARED)
                .withOptional("clientId", resolvedClientId)
                .with("thread", Thread.currentThread().getName());

        if (rebuildException != null) {
            AdapterDiagnostics.error(LOG, event.with("rebuildException", rebuildException.getClass().getSimpleName()),
                    rebuildException);
        } else {
            // No throwable - just log the outcome. Next send will close the loop.
            AdapterDiagnostics.error(LOG, event, null);
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
     * Returns the first 8 hex characters of the SHA-256 digest of the given topic name.
     * Used as a stable, short segment in {@code transactional.id} to ensure that two producers
     * with the same {@code transactionalIdPrefix} but different target topics never share an id.
     *
     * <p>SHA-256 is used rather than {@link String#hashCode()} because the latter is not
     * guaranteed to be stable across JVM versions or implementations.
     */
    static String computeTopicHash(String topic) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(topic.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(8);
            for (int i = 0; i < 4; i++) {
                sb.append(String.format("%02x", hash[i]));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the Java SE spec — this branch is unreachable in practice.
            throw new IllegalStateException("SHA-256 not available", e);
        }
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
            // A replacement client starts with an empty metadata cache, so the pre-warm has to
            // happen again for every topic.
            prewarmedTopics.clear();
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
