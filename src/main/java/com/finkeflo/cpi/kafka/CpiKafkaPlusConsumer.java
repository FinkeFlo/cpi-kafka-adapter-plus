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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.support.ScheduledPollConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CpiKafkaPlusConsumer extends ScheduledPollConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusConsumer.class);
    private static final int MAX_CONSECUTIVE_POLL_FAILURES = 5;
    private static final long MAX_POLL_FAILURE_DURATION_MS = 60_000L;
    /** Camel-Scheduler-Takt zwischen zwei poll()-Aufrufen, wenn pollingIntervalSeconds groesser ist. */
    private static final long KEEP_ALIVE_INTERVAL_SECONDS = 60L;
    /** Poll-Timeout des Keep-Alive-Polls — kurz, treibt nur den Coordinator-Roundtrip. */
    private static final long KEEP_ALIVE_POLL_TIMEOUT_MS = 1000L;

    private final CpiKafkaPlusEndpoint endpoint;
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private int consecutivePollFailures = 0;
    private long firstPollFailureMs = 0L;
    private volatile boolean initialized = false;
    private AvroDeserializerHelper avroHelper;
    private AdapterTracingHelper tracingHelper;
    private JsonSchemaValidator jsonSchemaValidator;
    private volatile DlqProducerHelper dlqHelper;
    private volatile boolean shutdownRequested = false;
    private int consecutiveInitFailures = 0;
    /** Zeitpunkt des letzten Emit-Zyklus (ms). 0 = noch nie emittiert. */
    private long lastEmitTimeMs = 0L;
    private ConsumerCircuitBreaker circuitBreaker;
    private RecordProcessor recordProcessor;
    /**
     * Last CPI connection/consumption status published for this consumer. Drives transition-based
     * reporting so the monitor is neither stuck nor spammed:
     * <ul>
     *   <li>OK is published only when a {@code kafkaConsumer.poll()} returns without throwing —
     *       only then is the broker reachable AND the SASL auth + group/topic authorization
     *       accepted (an unauthorized consumer throws {@code GroupAuthorizationException}/
     *       {@code TopicAuthorizationException} out of poll()). Subscribing alone must NOT report
     *       OK: subscribe() is local and never contacts the broker.</li>
     *   <li>ERROR is published on the first failure (even from {@code UNKNOWN}, i.e. a consumer
     *       that never once connected) and then suppressed until the status changes again.</li>
     * </ul>
     */
    private enum ConnStatus { UNKNOWN, OK, ERROR }
    private volatile ConnStatus connStatus = ConnStatus.UNKNOWN;

    private final RecordProcessor.ConsumerCallback consumerCallback = new RecordProcessor.ConsumerCallback() {
        @Override
        public void processExchange(Exchange exchange) throws Exception {
            getProcessor().process(exchange);
            if (exchange.getException() != null) {
                throw exchange.getException();
            }
        }

        @Override
        public void handleException(String message, Exchange exchange, Exception e) {
            getExceptionHandler().handleException(message, exchange, e);
        }

        @Override
        public Exchange createExchange() {
            return endpoint.createExchange();
        }
    };

    public CpiKafkaPlusConsumer(CpiKafkaPlusEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
    }

    @Override
    protected void doStart() throws Exception {
        // Fail-fast: validate shared configuration (Schema Registry, JSON Schema, SASL)
        endpoint.validateConfiguration();

        // Consumer-specific validations
        if (endpoint.isDlqEnabled()) {
            String dlqTopic = endpoint.getDlqTopic();
            if (dlqTopic == null || dlqTopic.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "DLQ is enabled but no DLQ topic is configured. Please set the 'dlqTopic' property.");
            }
        }
        if (endpoint.isDrainEnabled() && "AUTO".equalsIgnoreCase(endpoint.getCommitStrategy())) {
            throw new IllegalArgumentException(
                    "Drain mode requires commitStrategy=BATCH_COMPLETE. "
                    + "AUTO commit cannot guarantee at-least-once delivery during drain loops.");
        }
        if (endpoint.getMinBacklogToDrain() < 0) {
            throw new IllegalArgumentException(
                    "minBacklogToDrain must be >= 0, got: " + endpoint.getMinBacklogToDrain());
        }
        if (endpoint.getMinBacklogToDrain() > endpoint.getMaxPollRecords()) {
            throw new IllegalArgumentException(
                    "minBacklogToDrain (" + endpoint.getMinBacklogToDrain()
                    + ") must be <= maxPollRecords (" + endpoint.getMaxPollRecords()
                    + "). A higher value would disable drain after every poll.");
        }
        if (endpoint.getMaxPartitionFetchSizeKb() < 1 || endpoint.getMaxPartitionFetchSizeKb() > 51200) {
            throw new IllegalArgumentException(
                    "maxPartitionFetchSizeKb must be between 1 and 51200 (50 MB).");
        }
        if (endpoint.getRetryDelaySeconds() < 0 || endpoint.getRetryDelaySeconds() > 300) {
            throw new IllegalArgumentException(
                    "retryDelaySeconds must be between 0 and 300, got: "
                    + endpoint.getRetryDelaySeconds());
        }
        validatePollingIntervalSeconds(endpoint.getPollingIntervalSeconds());

        super.doStart();
        LOG.error("[CPI-KAFKA-PLUS-DIAG] doStart called — Consumer for topic='{}' group='{}'",
                endpoint.getEffectiveTopic(), endpoint.getGroupId());
        LOG.info("Starting CPI Kafka Plus Consumer for topic '{}' with group '{}' (lazy init — Kafka resources created on first poll)",
                endpoint.getEffectiveTopic(), endpoint.getGroupId());

        tracingHelper = new AdapterTracingHelper(endpoint);
    }

    @Override
    protected void doStop() throws Exception {
        LOG.info("Stopping CPI Kafka Plus Consumer for topic '{}'", endpoint.getEffectiveTopic());
        shutdownRequested = true;

        // Step 1: Signal any in-flight poll() to return via WakeupException.
        //         The poll thread must exit the poll loop before we can safely close
        //         the KafkaConsumer (KafkaConsumer is NOT thread-safe).
        if (kafkaConsumer != null) {
            try {
                kafkaConsumer.wakeup();
            } catch (Exception e) {
                LOG.debug("[CPI-KAFKA-PLUS-DIAG] doStop: wakeup threw (consumer may already be closed): {}",
                        e.getMessage());
            }
        }

        // Step 2: Stop the Camel scheduler FIRST — awaits in-flight poll() invocations
        //         to return, so the KafkaConsumer is no longer in use afterwards.
        //         Previously super.doStop() was called LAST, causing close() below to
        //         race with the active poll thread and frequently skip LeaveGroup.
        //         Missing LeaveGroup = coordinator waits full session.timeout.ms
        //         (45s on Confluent Cloud by default) before marking member dead.
        super.doStop();

        // Step 3: Close the KafkaConsumer on a thread that now has exclusive access.
        //         Timeout 15s allows the client to flush any pending commit and send
        //         LeaveGroup to the coordinator (bounded by request.timeout.ms=30s).
        //         This is the single most important change for fast rebalance after
        //         undeploy/redeploy of the integration flow.
        if (kafkaConsumer != null) {
            final KafkaConsumer<byte[], byte[]> consumerRef = kafkaConsumer;
            try {
                BundleBackedClassLoader.runWithBundleClassLoader(getClass(),
                        () -> consumerRef.close(Duration.ofSeconds(15)));
            } catch (Exception e) {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] doStop: error closing KafkaConsumer: {}", e.getMessage());
            }
            kafkaConsumer = null;
        }

        // Step 4: Close helpers AFTER the consumer is closed — the poll thread's
        //         RecordProcessor may have held references to these up to now.
        if (dlqHelper != null) {
            try {
                dlqHelper.close();
            } catch (Exception e) {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] doStop: error closing DLQ helper: {}", e.getMessage());
            }
            dlqHelper = null;
        }
        if (avroHelper != null) {
            try {
                avroHelper.close();
            } catch (Exception e) {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] doStop: error closing Avro helper: {}", e.getMessage());
            }
            avroHelper = null;
        }

        // Step 5: Reset state.
        initialized = false;
        consecutivePollFailures = 0;
        firstPollFailureMs = 0L;
        consecutiveInitFailures = 0;
        if (circuitBreaker != null) {
            circuitBreaker.reset();
            circuitBreaker = null;
        }
        jsonSchemaValidator = null;
        recordProcessor = null;

        // Step 6: Signal tracing after everything is cleaned up. Reset the transition guard so a
        // later restart re-reports OK on its first successful poll.
        if (tracingHelper != null) {
            connStatus = ConnStatus.UNKNOWN;
            tracingHelper.publishConnectionStatus(false, null);
        }
    }

    /**
     * Lazily initialize all Kafka resources (consumer, Avro helper, JSON Schema validator, DLQ helper)
     * on the first poll. This ensures only the CPI node that holds the cluster lock and actually polls
     * creates a KafkaConsumer, preventing partition starvation from idle consumers in the same group.
     */
    private void ensureInitialized() {
        if (initialized) {
            return;
        }

        LOG.info("[CPI-KAFKA-PLUS-DIAG] ensureInitialized: creating Kafka resources for topic='{}' group='{}'",
                endpoint.getEffectiveTopic(), endpoint.getGroupId());

        if (!createKafkaConsumer()) {
            return;
        }
        if (!createConsumerHelpers()) {
            return;
        }

        consecutiveInitFailures = 0;
        if (circuitBreaker == null) {
            circuitBreaker = new ConsumerCircuitBreaker(endpoint, tracingHelper);
        }
        recordProcessor = new RecordProcessor(endpoint, tracingHelper,
                jsonSchemaValidator, dlqHelper, avroHelper, consumerCallback,
                new OffsetCommitTracker());
        initialized = true;
        // NOTE: do NOT publish OK here. subscribe() is local/lazy and proves nothing about broker
        // reachability or group/topic authorization. OK is reported only after the first poll()
        // returns without throwing — see reportConnectionHealthy().
        LOG.info("[CPI-KAFKA-PLUS-DIAG] ensureInitialized: Consumer READY for topic='{}' group='{}'",
                endpoint.getEffectiveTopic(), endpoint.getGroupId());
    }

    /** @return true if the KafkaConsumer was created and subscribed successfully */
    private boolean createKafkaConsumer() {
        try {
            Properties props = buildConsumerProperties();
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] ensureInitialized: consumer properties built, security={}, sasl={}, credentialAlias='{}'",
                    endpoint.getSecurityProtocol(), endpoint.getSaslMechanism(), endpoint.getCredentialAlias());
            kafkaConsumer = BundleBackedClassLoader.withBundleClassLoader(getClass(),
                    () -> new KafkaConsumer<>(props,
                            new ByteArrayDeserializer(), new ByteArrayDeserializer()));
            List<String> topics = parseTopics(endpoint.getEffectiveTopic());
            // NOTE: the rebalance listener emits [CPI-KAFKA-PLUS-DIAG] log lines at ERROR level
            //       whenever partitions are assigned/revoked/lost. The CPI tenant trace log
            //       only persists ERROR; using the listener is the only practical way to
            //       observe Kafka group dynamics from inside a deployed adapter (Issue #45).
            //       The commit hook additionally commits pending offsets while partitions are
            //       still owned (revoke) or drops them when lost, so a processed batch's offset
            //       is never left behind the processed position (phantom lag, Issue #49). The
            //       hook reads recordProcessor lazily — it is only invoked during poll(), by
            //       which time recordProcessor is initialized.
            RebalanceLogger.RebalanceCommitHook commitHook = new RebalanceLogger.RebalanceCommitHook() {
                @Override
                public void onRevoked(java.util.Collection<TopicPartition> partitions) {
                    if (recordProcessor != null && kafkaConsumer != null) {
                        recordProcessor.commitOnRevoke(
                                offsets -> kafkaConsumer.commitSync(
                                        offsets, RecordProcessor.REVOKE_COMMIT_TIMEOUT),
                                partitions);
                    }
                }

                @Override
                public void onLost(java.util.Collection<TopicPartition> partitions) {
                    if (recordProcessor != null) {
                        recordProcessor.dropLost(partitions);
                    }
                }
            };
            RebalanceLogger rebalanceListener = new RebalanceLogger(
                    endpoint.getGroupId(), endpoint.getEffectiveTopic(), commitHook);
            kafkaConsumer.subscribe(topics, rebalanceListener);
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] ensureInitialized: KafkaConsumer created and subscribed to {} topic(s): {}",
                    topics.size(), topics);
            return true;
        } catch (Throwable e) {
            logInitFailure("KafkaConsumer", e);
            closeConsumerQuietly();
            return false;
        }
    }

    /** @return true if all helpers (Avro, JSON Schema, DLQ) were created successfully */
    private boolean createConsumerHelpers() {
        try {
            if (endpoint.isSchemaRegistryEnabled()
                    && endpoint.isAvroValueDeserialization()) {
                avroHelper = BundleBackedClassLoader.withBundleClassLoader(getClass(),
                        () -> new AvroDeserializerHelper(endpoint));
            }
            if (endpoint.isJsonSchemaValidation()) {
                jsonSchemaValidator = new JsonSchemaValidator(endpoint.getJsonSchema());
                LOG.info("JSON Schema validation enabled for incoming messages");
            }
            if (endpoint.isDlqEnabled()) {
                dlqHelper = new DlqProducerHelper(endpoint);
                LOG.info("DLQ enabled: failed records will be routed to topic '{}' after {} retries",
                        endpoint.getDlqTopic(), endpoint.getDlqMaxRetries());
            }
            return true;
        } catch (Throwable e) {
            logInitFailure("helpers", e);
            if (avroHelper != null) {
                try { avroHelper.close(); } catch (Exception ignored) { }
                avroHelper = null;
            }
            jsonSchemaValidator = null;
            if (dlqHelper != null) {
                try { dlqHelper.close(); } catch (Exception ignored) { }
                dlqHelper = null;
            }
            closeConsumerQuietly();
            return false;
        }
    }

    private void logInitFailure(String component, Throwable e) {
        consecutiveInitFailures++;
        String topStack = describeTopStack(e, 6);
        if (consecutiveInitFailures >= KafkaErrorHelper.INIT_FAILURE_ESCALATION_THRESHOLD) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] ensureInitialized: FAILED to create {} ({} consecutive failures) exClass={} exMsg='{}' topStack={}",
                    component, consecutiveInitFailures, e.getClass().getName(), e.getMessage(), topStack);
        } else {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] ensureInitialized: FAILED to create {} (attempt {}) exClass={} exMsg='{}' topStack={}",
                    component, consecutiveInitFailures, e.getClass().getName(), e.getMessage(), topStack);
        }
        reportConnectionError(KafkaErrorHelper.wrapIfError(e));
    }

    @Override
    protected int poll() throws Exception {
        ensureInitialized();
        if (kafkaConsumer == null) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] poll: kafkaConsumer is null (init failed), skipping poll");
            return 0;
        }
        if (circuitBreaker != null && circuitBreaker.handlePausedState(kafkaConsumer)) {
            return 0;
        }

        long pollingIntervalMs = endpoint.getPollingIntervalSeconds() * 1000L;
        if (isEmitDue(lastEmitTimeMs, System.currentTimeMillis(), pollingIntervalMs)) {
            int processed = runEmitCycle();
            // lastEmitTimeMs nur setzen, wenn der Consumer als Group-Member
            // gejoint ist (nicht-leere Zuordnung). Gibt der erste Emit-Poll 0
            // zurueck, weil die Group noch joint, bleibt der naechste Tick ein
            // Emit-Tick — sonst laege echte Daten bis zu pollingIntervalSeconds
            // unverarbeitet. Siehe Design-Spec Abschnitt 5.3.
            if (!kafkaConsumer.assignment().isEmpty()) {
                // bewusst nach runEmitCycle() neu gelesen: Emit-ENDE-Zeitpunkt
                // (der Drain-Zyklus kann lange laufen) -- nicht mit dem now aus
                // isEmitDue zusammenfassen.
                lastEmitTimeMs = System.currentTimeMillis();
            }
            return processed;
        }
        keepAlivePoll();
        return 0;
    }

    /**
     * Fuehrt einen Emit-Zyklus aus: pollt Kafka und verarbeitet die Records
     * (Drain-Schleife). Aus {@code poll()} extrahiert, damit {@code poll()}
     * zwischen Emit-Zyklus und Keep-Alive-Poll dispatchen kann.
     */
    private int runEmitCycle() throws Exception {
        LOG.error("[CPI-KAFKA-PLUS-DIAG] runEmitCycle: emit cycle started for topic='{}' initialized={}",
                endpoint.getEffectiveTopic(), initialized);
        boolean isBatchComplete = "BATCH_COMPLETE".equalsIgnoreCase(endpoint.getCommitStrategy());
        // Re-commit any offsets whose commit was skipped in a previous cycle because a
        // rebalance was in progress. By now poll() (in the previous keepalive/emit call) has
        // driven the rebalance to completion, so the commit should succeed. Prevents the
        // committed offset from lagging behind the processed position (phantom lag, Issue #49).
        if (isBatchComplete && recordProcessor != null) {
            recordProcessor.recommitPending(kafkaConsumer);
        }
        boolean drainEnabled = endpoint.isDrainEnabled();
        int totalProcessed = 0;
        int totalRecordsFetched = 0;
        int drainCycle = 0;
        long drainStartTime = System.currentTimeMillis();

        while (!shutdownRequested) {
            drainCycle++;

            ConsumerRecords<byte[], byte[]> records = pollKafkaRecords();
            if (records == null) {
                break; // wakeup received
            }

            consecutivePollFailures = 0;
            firstPollFailureMs = 0L;

            if (records.isEmpty()) {
                if (drainCycle == 1) {
                    LOG.debug("[CPI-KAFKA-PLUS-DIAG] poll: 0 records from topic='{}' group='{}' timeout={}ms",
                            endpoint.getEffectiveTopic(), endpoint.getGroupId(), endpoint.getBatchTimeout());
                }
                break;
            }

            int recordCount = records.count();

            int minBacklog = endpoint.getMinBacklogToDrain();
            if (drainEnabled && drainCycle > 1 && minBacklog > 0 && recordCount < minBacklog) {
                seekBackToFirstOffsets(records);
                LOG.debug("[CPI-KAFKA-PLUS-DIAG] drain: extra-cycle poll returned {} records (< minBacklogToDrain {}), "
                        + "seeking back, skipping process, stopping drain",
                        recordCount, minBacklog);
                break;
            }

            totalRecordsFetched += recordCount;

            LOG.debug("[CPI-KAFKA-PLUS-DIAG] poll: drainCycle={} records={} totalFetched={} from topic='{}' group='{}'",
                    drainCycle, recordCount, totalRecordsFetched, endpoint.getEffectiveTopic(), endpoint.getGroupId());

            try {
                if (endpoint.isBatchMode() && !"SPLIT_EXCHANGES".equalsIgnoreCase(endpoint.getBatchOutputFormat())) {
                    totalProcessed += recordProcessor.processBatchRecords(kafkaConsumer, records, isBatchComplete);
                } else {
                    totalProcessed += recordProcessor.processSingleRecords(kafkaConsumer, records, isBatchComplete);
                }
                if (circuitBreaker != null) {
                    circuitBreaker.recordSuccess();
                }
            } catch (Throwable t) {
                LOG.error("[CPI-KAFKA-PLUS-DIAG] processRecords: FAILED topic='{}' group='{}' exClass={} exMsg='{}' topStack={}",
                        endpoint.getEffectiveTopic(), endpoint.getGroupId(),
                        t.getClass().getName(), t.getMessage(), describeTopStack(t, 6));
                if (circuitBreaker != null && circuitBreaker.recordFailure()) {
                    break; // auto-pause triggered
                }
                if (t instanceof Exception) {
                    throw (Exception) t;
                }
                throw new RuntimeException("Non-Exception Throwable from processRecords: " + t.getClass().getName(), t);
            }

            if (!drainEnabled) {
                break;
            }
            if (minBacklog > 0 && recordCount < minBacklog) {
                LOG.debug("[CPI-KAFKA-PLUS-DIAG] drain: records ({}) below minBacklogToDrain ({}), stopping drain",
                        recordCount, minBacklog);
                break;
            }
        }

        if (drainCycle > 1) {
            long elapsedMs = System.currentTimeMillis() - drainStartTime;
            LOG.info("[CPI-KAFKA-PLUS-DIAG] drain complete: topic='{}' drainCycles={} totalRecords={} processed={} elapsedMs={}",
                    endpoint.getEffectiveTopic(), drainCycle, totalRecordsFetched, totalProcessed, elapsedMs);
        }

        return totalProcessed;
    }

    /**
     * Seeks each partition back to the smallest offset present in the polled records.
     * Used by the drain loop to "un-poll" records that should not be processed in the
     * current cycle (e.g. when fewer records than {@code minBacklogToDrain} were returned
     * on an extra drain cycle). Without this, those records would be lost until the next
     * rebalance, because {@code consumer.position()} has already advanced past them.
     */
    private void seekBackToFirstOffsets(ConsumerRecords<byte[], byte[]> records) {
        Map<TopicPartition, Long> firstOffsets = new HashMap<>();
        for (ConsumerRecord<byte[], byte[]> rec : records) {
            TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
            Long current = firstOffsets.get(tp);
            if (current == null || rec.offset() < current) {
                firstOffsets.put(tp, rec.offset());
            }
        }
        try {
            BundleBackedClassLoader.withBundleClassLoader(getClass(), () -> {
                for (Map.Entry<TopicPartition, Long> e : firstOffsets.entrySet()) {
                    kafkaConsumer.seek(e.getKey(), e.getValue());
                }
                return null;
            });
        } catch (Exception e) {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] drain: failed to seek back to first offsets, "
                    + "records may be re-polled on next cycle anyway: exClass={} exMsg='{}'",
                    e.getClass().getName(), e.getMessage());
        }
    }

    /**
     * Keep-Alive-Poll: treibt nur das Kafka-Group-Protokoll (Rebalance-Abschluss,
     * Heartbeat), ohne Records zu verarbeiten. Die Partitionen werden vor dem
     * poll() pausiert, damit poll() garantiert keine Records liefert und die
     * Consumer-Position sich nicht bewegt — danach wird die aktuelle Zuordnung
     * wieder resumed (resume auf nie-pausierten Partitionen ist ein No-op).
     *
     * <p>Best-effort: ein fehlgeschlagener Keep-Alive-Poll wird nur geloggt und
     * eskaliert nicht in den Circuit Breaker; der naechste Tick versucht es erneut.
     */
    private void keepAlivePoll() {
        try {
            BundleBackedClassLoader.runWithBundleClassLoader(getClass(), () -> {
                Set<TopicPartition> assigned = kafkaConsumer.assignment();
                kafkaConsumer.pause(assigned);
                ConsumerRecords<byte[], byte[]> records =
                        kafkaConsumer.poll(Duration.ofMillis(KEEP_ALIVE_POLL_TIMEOUT_MS));
                if (records != null && !records.isEmpty()) {
                    // Defensive: eine Partition wurde mitten im poll() neu zugeteilt
                    // und war daher nicht pausiert -> Records zurueckspulen, damit
                    // der naechste Emit-Zyklus sie liest. Keep-Alive committet nie.
                    seekBackToFirstOffsets(records);
                }
                kafkaConsumer.resume(kafkaConsumer.assignment());
            });
            // A keep-alive poll that returns without throwing is also a genuine broker round-trip.
            reportConnectionHealthy();
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] keepAlivePoll: completed (group protocol driven, no records emitted)");
        } catch (WakeupException we) {
            LOG.info("[CPI-KAFKA-PLUS-DIAG] keepAlivePoll: received wakeup signal");
        } catch (Exception e) {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] keepAlivePoll: best-effort poll failed: {}", e.getMessage());
        }
    }

    /**
     * Publishes an OK connection/consumption status to CPI on the first successful poll, and again
     * on any later recovery. Idempotent while already OK, so a steadily-polling consumer does not
     * spam the integration-flow monitor.
     */
    private void reportConnectionHealthy() {
        if (connStatus != ConnStatus.OK) {
            connStatus = ConnStatus.OK;
            tracingHelper.publishConnectionStatus(true, null);
            LOG.info("[CPI-KAFKA-PLUS-DIAG] connection: OK — poll succeeded for topic='{}' group='{}'",
                    endpoint.getEffectiveTopic(), endpoint.getGroupId());
        }
    }

    /**
     * Publishes an ERROR connection/consumption status to CPI. Fires on the first failure (even
     * from the initial {@code UNKNOWN} state, i.e. a consumer that never connected) and is then
     * suppressed until the status changes again. {@code error} may be {@code null} (e.g. on stop).
     */
    private void reportConnectionError(Throwable error) {
        if (connStatus != ConnStatus.ERROR) {
            connStatus = ConnStatus.ERROR;
            tracingHelper.publishConnectionStatus(false, error);
        }
    }

    /**
     * Polls Kafka for records, handling WakeupException and poll failures with reconnect.
     *
     * @return the polled records, or null if a wakeup signal was received
     */
    private ConsumerRecords<byte[], byte[]> pollKafkaRecords() throws Exception {
        try {
            ConsumerRecords<byte[], byte[]> records = BundleBackedClassLoader.withBundleClassLoader(
                    getClass(), () -> kafkaConsumer.poll(Duration.ofMillis(endpoint.getBatchTimeout())));
            // A poll() that returns without throwing proves broker reachability + SASL auth +
            // group/topic authorization — the true "connected" signal (see reportConnectionHealthy()).
            reportConnectionHealthy();
            return records;
        } catch (WakeupException e) {
            LOG.info("[CPI-KAFKA-PLUS-DIAG] poll: received wakeup signal, stopping poll loop");
            return null;
        } catch (RecordDeserializationException rde) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] poll: RecordDeserializationException topic='{}' group='{}'"
                            + " partition={} offset={} exClass={} exMsg='{}'"
                            + " causeClass={} causeMsg='{}' topStack={}",
                    endpoint.getEffectiveTopic(), endpoint.getGroupId(),
                    rde.topicPartition(), rde.offset(),
                    rde.getClass().getName(), rde.getMessage(),
                    rde.getCause() != null ? rde.getCause().getClass().getName() : "null",
                    rde.getCause() != null ? rde.getCause().getMessage() : "null",
                    describeTopStack(rde, 6));

            if (dlqHelper != null) {
                try {
                    final TopicPartition tp = rde.topicPartition();
                    final long badOffset = rde.offset();
                    byte[] keyBytes = toByteArray(rde.keyBuffer());
                    byte[] valueBytes = toByteArray(rde.valueBuffer());
                    Headers originalHeaders = rde.headers();
                    long ts = rde.timestamp();
                    Throwable cause = rde.getCause() != null ? rde.getCause() : rde;

                    dlqHelper.sendDeserializationFailure(tp, badOffset,
                            keyBytes, valueBytes, originalHeaders, ts, cause);

                    BundleBackedClassLoader.withBundleClassLoader(getClass(), () -> {
                        kafkaConsumer.seek(tp, badOffset + 1L);
                        return null;
                    });

                    LOG.info("[CPI-KAFKA-PLUS-DIAG] poison-pill: routed to DLQ and advanced past offset={} on {}",
                            badOffset, tp);

                    consecutivePollFailures = 0;
                    firstPollFailureMs = 0L;
                    return ConsumerRecords.empty();
                } catch (Exception dlqError) {
                    LOG.error("[CPI-KAFKA-PLUS-DIAG] poison-pill: DLQ routing FAILED, falling back to reconnect: exClass={} exMsg='{}'",
                            dlqError.getClass().getName(), dlqError.getMessage(), dlqError);
                }
            }

            reportConnectionError(rde);
            maybeReconnectAfterPollFailure();
            throw rde;
        } catch (Throwable t) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] poll: FAILED topic='{}' group='{}' exClass={} exMsg='{}'"
                            + " causeClass={} causeMsg='{}' topStack={}",
                    endpoint.getEffectiveTopic(), endpoint.getGroupId(),
                    t.getClass().getName(), t.getMessage(),
                    t.getCause() != null ? t.getCause().getClass().getName() : "null",
                    t.getCause() != null ? t.getCause().getMessage() : "null",
                    describeTopStack(t, 6));
            reportConnectionError(t);
            maybeReconnectAfterPollFailure();
            if (t instanceof Exception) {
                throw (Exception) t;
            }
            throw new RuntimeException("Non-Exception Throwable from poll: " + t.getClass().getName(), t);
        }
    }

    /**
     * Increments the consecutive-failure counter and triggers a consumer close+reinit
     * if either the count threshold or the time-duration threshold is exceeded.
     * The duration check protects against long poll intervals where a few failures
     * would otherwise mean waiting tens of minutes before reconnect.
     */
    private void maybeReconnectAfterPollFailure() {
        long now = System.currentTimeMillis();
        consecutivePollFailures++;
        if (firstPollFailureMs == 0L) {
            firstPollFailureMs = now;
        }
        long durationMs = now - firstPollFailureMs;
        boolean countExceeded = consecutivePollFailures >= MAX_CONSECUTIVE_POLL_FAILURES;
        boolean durationExceeded = durationMs >= MAX_POLL_FAILURE_DURATION_MS;
        if (countExceeded || durationExceeded) {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] poll: closing consumer for reconnect (consecutiveFailures={} durationMs={} reason={})",
                    consecutivePollFailures, durationMs,
                    countExceeded ? (durationExceeded ? "count+duration" : "count") : "duration");
            initialized = false;
            closeConsumerQuietly();
        }
    }

    /**
     * Returns a compact one-line string representation of the top N stack frames of a throwable
     * plus its cause chain. Used because the CPI trace-log appender drops the exception object's
     * stack trace; we therefore encode it into the log message itself.
     */
    static String describeTopStack(Throwable t, int maxFrames) {
        if (t == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        Throwable cur = t;
        int depth = 0;
        while (cur != null && depth < 4) {
            if (depth > 0) {
                sb.append(" CAUSED_BY ");
            }
            sb.append(cur.getClass().getSimpleName());
            String msg = cur.getMessage();
            if (msg != null) {
                String trimmed = msg.length() > 200 ? msg.substring(0, 200) + "…" : msg;
                sb.append("('").append(trimmed.replace('\n', ' ')).append("')");
            }
            StackTraceElement[] frames = cur.getStackTrace();
            sb.append("[");
            int shown = Math.min(maxFrames, frames.length);
            for (int i = 0; i < shown; i++) {
                if (i > 0) {
                    sb.append(" <- ");
                }
                StackTraceElement f = frames[i];
                sb.append(f.getClassName()).append('.').append(f.getMethodName())
                        .append(':').append(f.getLineNumber());
            }
            if (frames.length > shown) {
                sb.append(" <- …(" ).append(frames.length - shown).append(" more)");
            }
            sb.append("]");
            cur = cur.getCause();
            depth++;
        }
        return sb.toString();
    }

    /**
     * Copies a {@link ByteBuffer} into a fresh {@code byte[]} without mutating the buffer's
     * position. Used for poison-pill DLQ routing where {@link RecordDeserializationException#keyBuffer()}
     * and {@link RecordDeserializationException#valueBuffer()} may return direct buffers.
     * Returns {@code null} if the input is {@code null}.
     */
    static byte[] toByteArray(ByteBuffer bb) {
        if (bb == null) {
            return null;
        }
        byte[] arr = new byte[bb.remaining()];
        bb.duplicate().get(arr);
        return arr;
    }

    /**
     * Parse comma-separated topic string into a trimmed list.
     */
    static List<String> parseTopics(String topicString) {
        List<String> topics = new ArrayList<>();
        if (topicString != null) {
            for (String t : topicString.split(",")) {
                String trimmed = t.trim();
                if (!trimmed.isEmpty()) {
                    topics.add(trimmed);
                }
            }
        }
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("At least one topic must be specified");
        }
        return topics;
    }

    /**
     * Computes the effective {@code max.poll.interval.ms} value based on the user-configured
     * {@code pollingIntervalSeconds}. The Kafka client's heartbeat thread (KIP-62) sends a
     * proactive {@code LeaveGroup} request when {@code poll()} has not been called within
     * {@code max.poll.interval.ms}; with a hardcoded 5-min default this triggered a self-leave
     * for IFlows configured with longer polling intervals (issue #45).
     *
     * <p>Bounds:
     * <ul>
     *   <li>Polling-based: {@code pollingMs + 600_000} (10 min buffer for processing,
     *       scheduling slack, and downstream-call bursts on large batches).</li>
     *   <li>Hard cap: 22_200_000 ms (6 h 10 min) — bounds stuck-consumer detection even at the
     *       maximum allowed {@code pollingIntervalSeconds=21600} (= 6 h, whole-hour aligned
     *       so operators can plan poll cycles around predictable clock times).</li>
     * </ul>
     *
     * <p>Note: with a 10-min buffer the Kafka default (300_000 ms / 5 min) is always exceeded
     * for any positive polling interval, so an explicit floor is no longer necessary.
     */
    static int computeMaxPollIntervalMs(long pollingIntervalSeconds) {
        long pollingMs = pollingIntervalSeconds * 1000L;
        long maxPollIntervalMs = pollingMs + 600_000L;
        maxPollIntervalMs = Math.min(maxPollIntervalMs, 22_200_000L);
        return (int) maxPollIntervalMs;
    }

    /**
     * Validates that {@code pollingIntervalSeconds} is in the supported range {@code (0, 21600]}.
     * Values above 21600 (6 h) would exceed the {@code max.poll.interval.ms} hard cap of
     * 6 h 10 min and cause the consumer to leave its group between polls (issue #45).
     * The 6-h limit is chosen as a whole-hour value so polls align with predictable clock
     * times (e.g. every 6 h: 00:00, 06:00, 12:00, 18:00).
     */
    static void validatePollingIntervalSeconds(long pollingIntervalSeconds) {
        if (pollingIntervalSeconds <= 0) {
            throw new IllegalArgumentException(
                    "pollingIntervalSeconds must be > 0, got: " + pollingIntervalSeconds);
        }
        if (pollingIntervalSeconds > 21600) {
            throw new IllegalArgumentException(
                    "pollingIntervalSeconds must be <= 21600 (6 h), got: "
                    + pollingIntervalSeconds
                    + ". Higher values exceed the max.poll.interval.ms hard cap (6 h 10 min) "
                    + "and would cause the consumer to leave the group between polls.");
        }
    }

    /**
     * Effektiver Camel-Scheduler-Takt: das Minimum aus dem konfigurierten
     * {@code pollingIntervalSeconds} und dem Keep-Alive-Takt. Bei
     * {@code pollingIntervalSeconds <= 60} tickt der Scheduler unveraendert wie
     * bisher; bei laengeren Intervallen tickt er alle 60 s, damit poll() das
     * Kafka-Group-Protokoll regelmaessig treiben kann.
     *
     * <p>Package-private und static fuer direkte Unit-Tests.
     */
    static long computeSchedulerDelaySeconds(long pollingIntervalSeconds) {
        return Math.min(pollingIntervalSeconds, KEEP_ALIVE_INTERVAL_SECONDS);
    }

    /**
     * Entscheidet, ob im aktuellen Tick ein Emit-Zyklus (Drain + Verarbeitung)
     * laufen soll. {@code lastEmitTimeMs == 0} bedeutet "noch nie emittiert" und
     * erzwingt einen Emit (erster Tick nach Start). Sonst ist ein Emit faellig,
     * wenn seit dem letzten Emit mindestens {@code pollingIntervalMs} vergangen
     * sind.
     *
     * <p>Package-private und static fuer direkte Unit-Tests.
     */
    static boolean isEmitDue(long lastEmitTimeMs, long nowMs, long pollingIntervalMs) {
        if (lastEmitTimeMs == 0L) {
            return true;
        }
        return (nowMs - lastEmitTimeMs) >= pollingIntervalMs;
    }

    Properties buildConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, endpoint.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, endpoint.getGroupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, endpoint.getAutoOffsetReset());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, endpoint.getMaxPollRecords());
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                endpoint.getMaxPartitionFetchSizeKb() * 1024);

        boolean autoCommit = "AUTO".equalsIgnoreCase(endpoint.getCommitStrategy());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        if (autoCommit) {
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
        }

        // max.poll.interval.ms must be >= the Camel polling tick. Otherwise the Java
        // client (heartbeat thread, KIP-62) sends a LeaveGroup request between poll()
        // calls and the consumer drops from the group (symptom: State=EMPTY, 0 active
        // consumers, duplicate processing on rejoin). See issue #45.
        int maxPollIntervalMs = computeMaxPollIntervalMs(endpoint.getPollingIntervalSeconds());
        String maxPollBranch;
        if (maxPollIntervalMs == 22_200_000) {
            maxPollBranch = "HARD_CAP_6H_10MIN";
        } else {
            maxPollBranch = "POLLING_PLUS_10MIN_BUFFER";
        }
        LOG.error("[CPI-KAFKA-PLUS-DIAG] max.poll.interval.ms={} ms (branch={}, pollingIntervalSeconds={}s)",
                maxPollIntervalMs, maxPollBranch, endpoint.getPollingIntervalSeconds());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);

        // Group-membership hardening — prevents rebalance storms when the bundle
        // runs on multiple CPI worker nodes with the same group.id. Typical failure
        // mode without these settings: N nodes join, only M<=partition-count get a
        // partition assigned, the remaining N-M are idle standby members. Any
        // transient disconnect of a standby triggers a full rebalance which, with
        // the default RangeAssignor, revokes ALL assignments (stop-the-world) —
        // the active consumer loses its partition, the next poll() reassigns,
        // offset commit often races with the revoke → lag is not reduced, the
        // whole cycle repeats (PREPARING_REBALANCE ↔ STABLE flapping).
        //
        // CooperativeStickyAssignor switches to incremental cooperative rebalance:
        // only partitions that actually change ownership are revoked. The active
        // consumer keeps its partition across most rebalances, so idle members
        // joining/leaving no longer interrupts active consumption.
        //
        // session.timeout.ms = 30 s (relaxed from the previous 10 s hotfix, which
        // was set very aggressively to expedite zombie-member cleanup but caused
        // false-positive evictions under load: a single GC pause or saturated
        // heartbeat thread now takes ~10 s to recover, which exceeds 10 s and
        // triggers an unnecessary rebalance — see issue #45). 30 s gives enough
        // headroom for normal worker-node hiccups while still cleaning up real
        // zombie members from a previous deployment within ~30 s.
        // heartbeat.interval.ms must remain ≤ session.timeout.ms / 3 so the
        // client gets at least three heartbeat attempts before being evicted.
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);    // 30 s
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // 10 s (≤ session/3)

        // Static Membership (KIP-345) -- a stable group.instance.id makes the
        // broker recognize a returning pod within session.timeout.ms WITHOUT
        // triggering a rebalance. CF_INSTANCE_INDEX is set by Cloud Foundry per
        // worker instance and stable across restarts of the same instance slot
        // (e.g. "0".."6" for 7 instances). Falls back to HOSTNAME if not on CF
        // (e.g. local Camel test). If neither is available, static membership is
        // skipped and the consumer falls back to dynamic membership (auto-UUID
        // member.id, every reconnect counts as a new member -> rebalance).
        String memberSuffix = resolveStaticMemberSuffix();
        if (memberSuffix != null) {
            String groupInstanceId = endpoint.getGroupId() + "-" + memberSuffix;
            props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
            LOG.error("[CPI-KAFKA-PLUS-DIAG] group.instance.id={} (static membership enabled)",
                    groupInstanceId);
        } else {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] group.instance.id NOT set "
                    + "(neither CF_INSTANCE_INDEX nor HOSTNAME available) -- using dynamic membership");
        }

        // Connection timeouts — prevent blocking CPI worker nodes on SSL/network hangs
        props.put("socket.connection.setup.timeout.ms", 10000);        // 10s TCP/SSL handshake timeout
        props.put("socket.connection.setup.timeout.max.ms", 30000);    // 30s backoff upper bound
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);    // 30s request timeout
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000); // 60s for commitSync() etc.

        String adapterInstanceId = endpoint.getCamelContext().getGlobalOption("adapterInstanceID");
        if (adapterInstanceId != null && !adapterInstanceId.isEmpty()) {
            props.put(ConsumerConfig.CLIENT_ID_CONFIG,
                    endpoint.getGroupId() + "-" + adapterInstanceId);
        }

        // Security
        SecurityConfigHelper.configureSecurityProperties(props, endpoint);

        return props;
    }

    /**
     * Resolves a stable per-pod identifier for {@code group.instance.id} (KIP-345
     * Static Membership). Returns the first non-empty value of:
     * <ol>
     *   <li>{@code CF_INSTANCE_INDEX} — Cloud Foundry instance number, stable
     *       across restarts of the same slot (this is what runs in CPI BTP).</li>
     *   <li>{@code HOSTNAME} — fallback for non-CF runtimes (local Camel test,
     *       k8s where CF_INSTANCE_INDEX is unset).</li>
     * </ol>
     * Returns {@code null} if neither is available, in which case the consumer
     * falls back to dynamic membership.
     *
     * <p>Package-private and static so unit tests can verify the call site
     * behavior using whichever env vars are present in the test JVM.
     */
    static String resolveStaticMemberSuffix() {
        String cfInstance = System.getenv("CF_INSTANCE_INDEX");
        if (cfInstance != null && !cfInstance.isEmpty()) {
            return cfInstance;
        }
        String hostname = System.getenv("HOSTNAME");
        return (hostname != null && !hostname.isEmpty()) ? hostname : null;
    }


    private void closeConsumerQuietly() {
        if (kafkaConsumer != null) {
            try {
                BundleBackedClassLoader.runWithBundleClassLoader(getClass(),
                        () -> kafkaConsumer.close(Duration.ofSeconds(5)));
            } catch (Exception e) {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] closeConsumerQuietly: error closing consumer: {}", e.getMessage());
            }
            kafkaConsumer = null;
        }
    }
}
