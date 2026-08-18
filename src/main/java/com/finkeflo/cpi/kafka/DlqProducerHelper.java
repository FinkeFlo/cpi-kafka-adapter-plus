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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages an internal KafkaProducer that sends failed records to a Dead Letter Queue topic.
 * Preserves the original record key, value, headers and adds error metadata as Kafka headers.
 *
 * <p><b>Why this producer heals itself.</b> A dead-letter send is the last exit of the consume
 * path: {@link RecordProcessor} only commits an offset once the failed record is safely parked. If
 * the send throws, the offset is not committed, the very same record is polled again, and the
 * partition stops moving — every later message is blocked behind a record that can never succeed.
 * A producer that has become permanently unusable (a fenced or authentication-failed client, or the
 * KAFKA-10902 monitor fault that wedges the metadata wait) therefore turns a single poison record
 * into a total consumer outage that only a redeploy clears. That failure mode was observed in
 * production: no dead-letter write happened for over an hour while the partition sat on one offset.
 *
 * <p>The producer is consequently held in a replaceable field rather than a final one, and a send
 * that fails for a reason a fresh client could survive rebuilds the client once and retries the
 * record once. The bounds mirror {@link CpiKafkaPlusProducer}: only classifications that a rebuild
 * can plausibly fix qualify, and {@link #REBUILD_BACKOFF_MS} caps how often a re-polled record can
 * trigger one.
 */
public final class DlqProducerHelper implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DlqProducerHelper.class);

    /**
     * Minimum interval between rebuild attempts. A stalled partition re-delivers the same record
     * continuously, so without this a single unrecoverable record would rebuild the client on every
     * poll instead of once.
     */
    static final long REBUILD_BACKOFF_MS = 30_000L;

    /**
     * Consecutive failures after which a rebuild is attempted even though the classification did not
     * ask for one. Exists because a classification is a prediction: if a send has failed this many
     * times in a row, the partition is not advancing and "retriable" has been disproven by events.
     */
    static final int REBUILD_ESCALATION_THRESHOLD = 3;

    /** How the producer is (re)created. Kept behind an interface so tests can drive a rebuild. */
    interface ProducerFactory {
        Producer<byte[], byte[]> create() throws Exception;
    }

    private final String dlqTopic;
    private final ProducerSendGuard sendGuard;
    /** {@code null} disables self-healing; only the legacy test constructor leaves it unset. */
    private final ProducerFactory producerFactory;
    /** Guards replacement of {@link #producer} so two threads cannot close or rebuild in parallel. */
    private final Object producerLock = new Object();

    private volatile Producer<byte[], byte[]> producer;
    private volatile boolean closed;
    private volatile long lastRebuildAttemptMs;
    private volatile int rebuildAttemptsSinceSuccess;
    private volatile int consecutiveSendFailures;

    public DlqProducerHelper(CpiKafkaPlusEndpoint endpoint) {
        this.dlqTopic = endpoint.getDlqTopic();
        this.sendGuard = ProducerSendGuard.forEndpoint(endpoint);
        // Properties are rebuilt on every create, not captured once, so a rotated credential is
        // picked up by a rebuild instead of being frozen at first construction.
        this.producerFactory = () -> {
            Properties props = buildProducerProperties(endpoint);
            TlsListenerProbe.assertNoTlsListener(endpoint.getBootstrapServers(),
                    endpoint.getSecurityProtocol());
            return BundleBackedClassLoader.withBundleClassLoader(DlqProducerHelper.class,
                    () -> new KafkaProducer<byte[], byte[]>(props,
                            new ByteArraySerializer(), new ByteArraySerializer()));
        };
        LOG.info("Creating DLQ producer for topic '{}'", dlqTopic);

        try {
            this.producer = producerFactory.create();
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] DLQ producer created OK for topic='{}'", dlqTopic);
        } catch (Exception e) {
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("dlq.producer.init.failed")
                    .with("errorCode", CpiKafkaPlusErrorCode.fromProducerInitFailure(e).code())
                    .with("dlqTopic", dlqTopic), e);
            throw (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
        }
    }

    /** Visible for testing. Self-healing is off: there is no way to build a replacement. */
    DlqProducerHelper(String dlqTopic, Producer<byte[], byte[]> producer, ProducerSendGuard sendGuard) {
        this(dlqTopic, producer, sendGuard, null);
    }

    /** Visible for testing, with a factory so the rebuild path can be exercised. */
    DlqProducerHelper(String dlqTopic, Producer<byte[], byte[]> producer, ProducerSendGuard sendGuard,
                      ProducerFactory producerFactory) {
        this.dlqTopic = dlqTopic;
        this.producer = producer;
        this.sendGuard = sendGuard;
        this.producerFactory = producerFactory;
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

        Map<String, Object> context = new LinkedHashMap<>();
        context.put("errorCode", CpiKafkaPlusErrorCode.KP_DLQ_001.code());
        context.put("dlqTopic", dlqTopic);
        context.put("offset", record.offset());
        context.put("partition", record.partition());

        try {
            dispatch(dlqRecord, "DLQ send to topic '" + dlqTopic + "'", context);
        } catch (Exception e) {
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

        Map<String, Object> context = new LinkedHashMap<>();
        context.put("dlqTopic", dlqTopic);
        context.put("offset", offset);
        context.put("partition", tp.partition());
        context.put("reason", "poison-pill record could not be written to the DLQ");

        try {
            dispatch(dlqRecord, "DLQ deserialization-failure send to topic '" + dlqTopic + "'", context);
        } catch (Exception e) {
            throw new RuntimeException("Failed to send poison-pill record to DLQ topic '" + dlqTopic + "'", e);
        }
    }

    /**
     * Sends one dead-letter record, rebuilding the producer and retrying once if the first attempt
     * failed for a reason a fresh client can survive.
     *
     * <p>Two things are deliberately bounded. The rebuild happens at most once per call and at most
     * once per {@link #REBUILD_BACKOFF_MS}, so a record that is re-polled forever cannot turn into a
     * rebuild loop. And only classifications a new client could actually fix qualify: a record that
     * is too large stays too large, and rebuilding on it would be a self-inflicted outage.
     *
     * <p>{@link #REBUILD_ESCALATION_THRESHOLD} is the safety net under that rule. A classification
     * is only a prediction, and {@code RETRIABLE} predicts a recovery that a stalled partition has
     * already disproven, so after that many consecutive failures a rebuild is attempted anyway. Data
     * errors stay excluded, because there the prediction is not a guess.
     *
     * <p><b>Duplicate trade-off.</b> If the first attempt failed after the record was accepted by
     * the client ({@code phase=AWAIT_FUTURE}), the retry can produce a second copy in the dead-letter
     * topic — a replacement client gets a new producer id, so idempotence does not span a rebuild.
     * That is accepted on purpose: a duplicate dead-letter record is visible, deduplicable via the
     * {@code CpiKafkaPlusDlqOriginalTopic/Partition/Offset} headers and harmless, whereas the
     * alternative is a partition that never moves again. The chosen phase is recorded as
     * {@code duplicateRisk} on the failure line so the operator can tell the two apart.
     */
    private void dispatch(ProducerRecord<byte[], byte[]> dlqRecord, String description,
                          Map<String, Object> context) throws Exception {
        Attempt attempt = new Attempt();
        try {
            sendOnce(dlqRecord, description, attempt);
            recordSendSuccess();
            return;
        } catch (Exception firstFailure) {
            int consecutiveFailures = ++consecutiveSendFailures;
            KafkaErrorHelper.Classification classification = KafkaErrorHelper.classify(firstFailure);
            boolean justified = rebuildJustified(firstFailure, classification);
            // A classification of RETRIABLE is a prediction, and a partition that has not moved for
            // several deliveries has already falsified it. Escalating turns "retriable" from a
            // permanent excuse into a bounded one, which is the difference between a consumer that
            // recovers and one that waits for a redeploy. A data error is still excluded: a record
            // the broker rejects is rejected by a new client too.
            boolean escalated = !justified
                    && producerFactory != null
                    && classification != KafkaErrorHelper.Classification.FATAL_DATA_ERROR
                    && consecutiveFailures >= REBUILD_ESCALATION_THRESHOLD;
            long now = System.currentTimeMillis();
            boolean backoffElapsed = (now - lastRebuildAttemptMs) >= REBUILD_BACKOFF_MS;
            boolean shouldRebuild = (justified || escalated) && backoffElapsed && !closed;

            AdapterDiagnostics.Event event = AdapterDiagnostics.event("dlq.send.failed")
                    .with("classification", classification)
                    .with("phase", attempt.phase)
                    .with("monitorFault", KafkaErrorHelper.isMetadataMonitorFault(firstFailure))
                    .with("consecutiveFailures", consecutiveFailures)
                    .with("rebuildJustified", justified)
                    .with("rebuildEscalated", escalated)
                    .with("rebuildBackoffElapsed", backoffElapsed)
                    .with("rebuildTriggered", shouldRebuild)
                    .with("duplicateRisk", !"SYNC_SEND".equals(attempt.phase))
                    .with("consequence", "offset not committed, record will be reprocessed")
                    .with("thread", Thread.currentThread().getName());
            context.forEach(event::with);
            AdapterDiagnostics.error(LOG, event, firstFailure);

            if (!shouldRebuild) {
                throw firstFailure;
            }

            Exception rebuildFailure = rebuildProducer();
            if (rebuildFailure != null) {
                firstFailure.addSuppressed(rebuildFailure);
                throw firstFailure;
            }

            Attempt retry = new Attempt();
            try {
                sendOnce(dlqRecord, description, retry);
            } catch (Exception secondFailure) {
                AdapterDiagnostics.Event effect = AdapterDiagnostics.event("dlq.producer.rebuild.effect")
                        .with("outcome", "STILL_FAILING")
                        .with("phase", retry.phase)
                        .with("attemptsSinceLastSuccess", rebuildAttemptsSinceSuccess)
                        .with("dlqTopic", dlqTopic)
                        .with("thread", Thread.currentThread().getName());
                AdapterDiagnostics.error(LOG, effect, secondFailure);
                if (secondFailure != firstFailure) {
                    secondFailure.addSuppressed(firstFailure);
                }
                throw secondFailure;
            }

            // Recovery is a rare, high-value event, and only ERROR reaches the tenant trace file.
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("dlq.producer.rebuild.effect")
                    .with("outcome", "RECOVERED")
                    .with("attemptsSinceLastSuccess", rebuildAttemptsSinceSuccess)
                    .with("dlqTopic", dlqTopic)
                    .with("thread", Thread.currentThread().getName()), null);
            recordSendSuccess();
        }
    }

    /**
     * One send against the current producer. {@link MonitorFaultRetry} wraps only the synchronous
     * {@code send(...)} call and never the wait for the acknowledgement: a monitor fault thrown by
     * {@code send} means the record was never appended, while the same exception surfacing from the
     * future would mean it had already been accepted and retrying could duplicate it.
     */
    private void sendOnce(ProducerRecord<byte[], byte[]> dlqRecord, String description,
                          Attempt attempt) throws Exception {
        Producer<byte[], byte[]> current = currentProducer();
        long deadlineMs = sendGuard.newDeadline();

        attempt.phase = "SYNC_SEND";
        Future<RecordMetadata> future = MonitorFaultRetry.execute(
                () -> current.send(dlqRecord), new MonitorFaultRetry.Budget(),
                deadlineMs, dlqTopic, 0);

        attempt.phase = "AWAIT_FUTURE";
        sendGuard.await(future, deadlineMs, description);

        LOG.debug("[CPI-KAFKA-PLUS-DIAG] DLQ send OK for topic='{}'", dlqTopic);
    }

    /**
     * Returns the producer to send with, recreating it if a previous rebuild left the field empty.
     * Without this, a rebuild that itself failed would leave the helper permanently broken with an
     * unexplained {@code NullPointerException} instead of a retryable, named failure.
     */
    private Producer<byte[], byte[]> currentProducer() throws Exception {
        Producer<byte[], byte[]> current = producer;
        if (current != null) {
            return current;
        }
        synchronized (producerLock) {
            if (producer == null) {
                if (closed) {
                    throw new IllegalStateException("DLQ producer for topic '" + dlqTopic
                            + "' is closed");
                }
                if (producerFactory == null) {
                    throw new IllegalStateException("DLQ producer for topic '" + dlqTopic
                            + "' is unavailable and cannot be recreated");
                }
                producer = producerFactory.create();
            }
            return producer;
        }
    }

    /**
     * Decides whether a fresh client could plausibly fix this failure. Mirrors the rule
     * {@link CpiKafkaPlusProducer} applies, with the KAFKA-10902 monitor fault added explicitly:
     * it leaves the metadata wait of that one client wedged, which a replacement does not inherit,
     * and it is not a Kafka exception type so the classifier alone would not recognise it.
     */
    private boolean rebuildJustified(Exception e, KafkaErrorHelper.Classification classification) {
        if (producerFactory == null) {
            return false;
        }
        if (KafkaErrorHelper.isMetadataMonitorFault(e)) {
            return true;
        }
        return classification == KafkaErrorHelper.Classification.FATAL_PRODUCER_UNUSABLE
                || classification == KafkaErrorHelper.Classification.UNKNOWN_FATAL;
    }

    /**
     * Closes the unusable client and builds a replacement.
     *
     * @return {@code null} on success, otherwise the exception that prevented the rebuild
     */
    private Exception rebuildProducer() {
        long startMs = System.currentTimeMillis();
        lastRebuildAttemptMs = startMs;
        rebuildAttemptsSinceSuccess++;

        boolean recreated = false;
        Exception rebuildException = null;
        synchronized (producerLock) {
            Producer<byte[], byte[]> stale = producer;
            producer = null;
            closeQuietly(stale, "stale DLQ producer");
            try {
                producer = producerFactory.create();
                recreated = producer != null;
            } catch (Exception e) {
                rebuildException = e;
            }
        }

        AdapterDiagnostics.Event event = AdapterDiagnostics.event("dlq.producer.rebuild.outcome")
                .with("producerRecreated", recreated)
                .with("durationMs", System.currentTimeMillis() - startMs)
                .with("attemptsSinceLastSuccess", rebuildAttemptsSinceSuccess)
                .with("dlqTopic", dlqTopic)
                .with("thread", Thread.currentThread().getName());
        if (rebuildException != null) {
            AdapterDiagnostics.error(LOG,
                    event.with("rebuildException", rebuildException.getClass().getSimpleName()),
                    rebuildException);
        } else {
            // A constructed producer is not yet a working one; the retry that follows decides that.
            AdapterDiagnostics.error(LOG, event, null);
        }
        return rebuildException;
    }

    private void recordSendSuccess() {
        rebuildAttemptsSinceSuccess = 0;
        consecutiveSendFailures = 0;
    }

    private static void closeQuietly(Producer<byte[], byte[]> victim, String what) {
        if (victim == null) {
            return;
        }
        try {
            victim.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] Error closing {}: {}", what, e.getMessage());
        }
    }

    /** Carries the phase a send attempt reached, so a failure can say whether a retry may duplicate. */
    private static final class Attempt {
        private String phase = "SYNC_SEND";
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing DLQ producer for topic '{}'", dlqTopic);
        Producer<byte[], byte[]> current;
        synchronized (producerLock) {
            closed = true;
            current = producer;
            producer = null;
        }
        if (current == null) {
            return;
        }
        try {
            current.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            // A close failure can mean records were never flushed, so it must be visible. WARN is
            // not: only ERROR reaches the CPI tenant trace file.
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("dlq.producer.close.failed")
                    .with("dlqTopic", dlqTopic)
                    .with("consequence", "buffered DLQ records may not have been flushed"), e);
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
