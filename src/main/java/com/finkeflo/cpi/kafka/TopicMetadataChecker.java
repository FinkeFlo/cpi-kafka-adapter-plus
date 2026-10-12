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
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.finkeflo.cpi.kafka.ProducerBatchHelper.ProducerPath;

/**
 * The receiver's topic checks before a send: the AdminClient probe whether the target topic exists
 * (at start-up as a report, on every send path as a guard) and the metadata pre-warm of the shared
 * producer (KAFKA-10902). Moved out of {@link CpiKafkaPlusProducer}; it logs under the producer's
 * logger category, so log levels and trace filters set for the producer still apply.
 */
final class TopicMetadataChecker {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusProducer.class);

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

    // ─────────────────────────────────────────────────────────────────────────────
    // c4: Slow metadata warn threshold — avoids log noise on the success path while
    // still surfacing pathological fetches. 1 second is well above normal (<50ms)
    // but below the timeout that would cause a hard failure (max.block.ms).
    // ─────────────────────────────────────────────────────────────────────────────
    private static final long SLOW_METADATA_THRESHOLD_MS = 1000L;

    private final Supplier<String> effectiveTopic;
    private final Supplier<String> bootstrapServers;
    private final Supplier<String> securityProtocol;
    private final Supplier<Properties> producerProperties;
    private final Supplier<KafkaProducer<byte[], byte[]>> kafkaProducer;
    private final Supplier<String> resolvedClientId;
    /**
     * Topics already confirmed to exist. Only ever holds <em>positive</em> results: a topic that
     * the broker reported as missing must never be cached, otherwise creating the topic would not
     * take effect until the IFlow is redeployed.
     */
    private final java.util.Set<String> verifiedTopics =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<String, Boolean>());
    /**
     * Topics whose metadata the current producer instance has fetched up front. Cleared whenever the
     * producer is replaced, because a new client starts with an empty metadata cache; changed only
     * under {@link #prewarmLock}, so a pre-warm that finishes on the replaced client cannot mark a
     * topic for the new one.
     */
    private final java.util.Set<String> prewarmedTopics =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<String, Boolean>());
    /**
     * Topics currently being pre-warmed, with the client that fetches them. Keeps concurrent sender
     * threads from issuing duplicate metadata fetches for the same topic while still allowing retries
     * after a failed attempt. Claimed only for the current client, under {@link #prewarmLock}, and
     * released only by the pre-warm that claimed it: a claim for a replaced client would hold back
     * the new client's pre-warm, and a pre-warm still running on a replaced client must not release
     * the new client's claim.
     */
    private final java.util.concurrent.ConcurrentHashMap<String, KafkaProducer<byte[], byte[]>> prewarmingTopics =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final Object prewarmLock = new Object();
    /**
     * Why the last probe of each topic could not reach the broker, kept so that the send failure
     * which follows can name a cause instead of reporting a bare metadata timeout. Only set for
     * inconclusive <em>connectivity</em> failures; cleared as soon as a probe of the same topic
     * succeeds. Per topic, so that one topic's probe neither lends its cause to another topic's
     * send error nor clears it (#246). Bounded like {@link #verifiedTopics}.
     */
    private final java.util.Map<String, Throwable> probeFailures = new java.util.concurrent.ConcurrentHashMap<>();
    /**
     * Asks the broker whether a topic exists; {@link #checkTopicExists(String)} outside tests. A
     * field rather than a direct call, like {@link CpiKafkaPlusProducer#txnProducerFactory}: it is the seam through which
     * a test scripts the probe's verdict without a broker.
     */
    java.util.function.Function<String, TopicCheckResult> topicProbe = this::checkTopicExists;

    TopicMetadataChecker(Supplier<String> effectiveTopic, Supplier<String> bootstrapServers,
                         Supplier<String> securityProtocol, Supplier<Properties> producerProperties,
                         Supplier<KafkaProducer<byte[], byte[]>> kafkaProducer,
                         Supplier<String> resolvedClientId) {
        this.effectiveTopic = effectiveTopic;
        this.bootstrapServers = bootstrapServers;
        this.securityProtocol = securityProtocol;
        this.producerProperties = producerProperties;
        this.kafkaProducer = kafkaProducer;
        this.resolvedClientId = resolvedClientId;
    }

    /**
     * The cause of the last inconclusive probe of {@code topic}, for the send error; {@code null}
     * when there is none.
     */
    Throwable lastProbeFailure(String topic) {
        return topic == null ? null : probeFailures.get(topic);
    }

    /**
     * Forgets the pre-warmed topics: a replacement producer starts with an empty metadata cache.
     * Called after the producer field has been cleared or replaced.
     */
    void onProducerReplaced() {
        synchronized (prewarmLock) {
            prewarmedTopics.clear();
            prewarmingTopics.clear();
        }
    }

    private void rememberProbeFailure(String topic, Throwable cause) {
        if (cause == null) {
            probeFailures.remove(topic);
        } else if (probeFailures.containsKey(topic) || probeFailures.size() < VERIFIED_TOPICS_CACHE_LIMIT) {
            probeFailures.put(topic, cause);
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
    void prewarmTopicMetadata(String topic) {
        // Fetch from the client read here, never from the field again: a rebuild may clear or replace
        // the producer at any moment, and the claim, the fetch and the mark all belong to this one
        // client. Reading the field again for the fetch turned that race into a
        // NullPointerException (#246).
        KafkaProducer<byte[], byte[]> producer = kafkaProducer.get();
        if (producer == null || topic == null || topic.isEmpty()) {
            return;
        }
        // Bounded like verifiedTopics, so a flow with unbounded dynamic topics cannot grow this set
        // without limit. Beyond the limit the pre-warm is simply skipped; the send path still works.
        if (prewarmedTopics.contains(topic)
                || prewarmedTopics.size() >= VERIFIED_TOPICS_CACHE_LIMIT
                || !claimPrewarm(topic, producer)) {
            return;
        }

        // c4: Measure the metadata fetch duration. This is the exact place the KAFKA-10902 incident
        // happened (IllegalMonitorStateException inside ProducerMetadata.awaitUpdate). Tracking
        // the duration makes a slow or blocking metadata fetch visible before it turns into a hard
        // failure. On failure we always emit; on success we only emit if the wait exceeded the
        // threshold (respects the log volume budget while still surfacing pathological fetches).
        long metadataStartMs = System.currentTimeMillis();
        try {
            // Another thread may have completed the pre-warm between the first read and our in-flight
            // marker acquisition. Re-check before doing the fetch.
            if (prewarmedTopics.contains(topic)) {
                return;
            }
            // A short retry deadline on purpose: the monitor fault throws immediately and is worth
            // retrying, while a genuine metadata timeout has already consumed max.block.ms and must
            // not be multiplied by retrying it.
            MonitorFaultRetry.execute(() -> producer.partitionsFor(topic),
                    new MonitorFaultRetry.Budget(),
                    System.currentTimeMillis() + PREWARM_RETRY_WINDOW_MS,
                    topic, -1);

            markPrewarmed(topic, producer);

            long metadataWaitMs = System.currentTimeMillis() - metadataStartMs;
            // Only log slow pre-warms to avoid log noise on the success path.
            if (metadataWaitMs > SLOW_METADATA_THRESHOLD_MS) {
                LOG.error("[CPI-KAFKA-PLUS-DIAG] producer.metadata.prewarm: outcome=SLOW, "
                        + "topic={}, metadataWaitMs={}, clientId={}, producerPath=SHARED, thread={}",
                        topic, metadataWaitMs, resolvedClientId.get(), Thread.currentThread().getName());
            }
        } catch (Exception e) {
            long metadataWaitMs = System.currentTimeMillis() - metadataStartMs;
            if (kafkaProducer.get() != producer) {
                // A rebuild (reported with its own cause) or a stop closed the client under the
                // fetch. Nothing is wrong with the topic, and the next send pre-warms the new client.
                LOG.info("[CPI-KAFKA-PLUS-DIAG] Metadata pre-warm of topic '{}' abandoned after {} ms: the "
                        + "producer was replaced while it ran ({})", topic, metadataWaitMs,
                        KafkaErrorHelper.describeChain(e));
                return;
            }
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.metadata.prewarm")
                    .with("outcome", "FAILED")
                    .with("topic", topic)
                    .with("metadataWaitMs", metadataWaitMs)
                    .with("producerPath", ProducerPath.SHARED)
                    .withOptional("clientId", resolvedClientId.get())
                    .with("thread", Thread.currentThread().getName()), e);
        } finally {
            prewarmingTopics.remove(topic, producer);
        }
    }

    /**
     * Claims the pre-warm of {@code topic} for {@code producer}, unless another sender is pre-warming
     * it or {@code producer} has been replaced since it was read. Under the lock that
     * {@link #onProducerReplaced()} clears under, which runs after the producer field has changed,
     * so no claim for a replaced client survives the replacement.
     */
    private boolean claimPrewarm(String topic, KafkaProducer<byte[], byte[]> producer) {
        synchronized (prewarmLock) {
            return kafkaProducer.get() == producer && prewarmingTopics.putIfAbsent(topic, producer) == null;
        }
    }

    /**
     * Marks {@code topic} as pre-warmed, unless {@code producer} has been replaced meanwhile: the
     * fetch filled the replaced client's cache, not the new one's (#246). Under the lock that
     * {@link #onProducerReplaced()} clears under, so the check and the mark cannot straddle a
     * replacement.
     */
    private void markPrewarmed(String topic, KafkaProducer<byte[], byte[]> producer) {
        synchronized (prewarmLock) {
            if (kafkaProducer.get() == producer && prewarmedTopics.size() < VERIFIED_TOPICS_CACHE_LIMIT) {
                prewarmedTopics.add(topic);
            }
        }
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
    static final class TopicCheckResult {

        static final TopicCheckResult EXISTS = new TopicCheckResult(TopicCheck.EXISTS, null);
        static final TopicCheckResult MISSING = new TopicCheckResult(TopicCheck.MISSING, null);

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
     * within {@code max.block.ms}. Its cause is remembered for the topic in {@link #probeFailures}
     * instead and folded into the send error if the send does go on to fail.
     *
     * <p>A single {@code MISSING} answer is likewise not treated as proof: {@code createTopics}
     * returns once the controller has accepted the request, and the topic can be absent from broker
     * metadata for a moment afterwards. Failing on the first answer would break the entirely normal
     * "create the topic, then send" sequence, so the probe is repeated briefly before giving up.
     */
    void assertTopicExists(String topic) {
        if (topic == null || topic.isEmpty() || verifiedTopics.contains(topic)) {
            return;
        }
        TopicCheckResult result = topicProbe.apply(topic);
        for (int attempt = 0; result.state == TopicCheck.MISSING
                && attempt < TOPIC_RECHECK_ATTEMPTS; attempt++) {
            if (!sleepQuietly(TOPIC_RECHECK_DELAY_MS)) {
                break;
            }
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] Topic '{}' reported as missing, re-checking ({}/{})",
                    topic, attempt + 1, TOPIC_RECHECK_ATTEMPTS);
            result = topicProbe.apply(topic);
        }
        if (result.state == TopicCheck.MISSING) {
            throw new IllegalArgumentException(
                    "Kafka topic '" + topic + "' does not exist on the broker. "
                    + "Please create the topic before sending to it "
                    + "(auto-create may be disabled on this cluster).");
        }
        if (result.state == TopicCheck.EXISTS) {
            probeFailures.remove(topic);
            if (verifiedTopics.size() < VERIFIED_TOPICS_CACHE_LIMIT) {
                verifiedTopics.add(topic);
            }
            return;
        }

        if (KafkaErrorHelper.isDefinitiveAuthOrTlsFailure(result.cause)) {
            throw new IllegalStateException(
                    "Cannot connect to Kafka broker '" + bootstrapServers.get() + "' for topic '"
                    + topic + "': " + KafkaErrorHelper.describeChain(result.cause)
                    + ". Please check Security Protocol, Credential Alias and SSL Keystore Alias.",
                    result.cause);
        }
        rememberProbeFailure(topic, result.cause);
    }

    /**
     * Deployment-time probe, called from {@link CpiKafkaPlusProducer#doStart()}: reports a missing topic as a warning
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
    void startTopicCheckInBackground() {
        final String topic = effectiveTopic.get() == null
                ? null : effectiveTopic.get().trim();
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
        TopicCheckResult result = topicProbe.apply(topic);
        if (result.state == TopicCheck.MISSING) {
            // ERROR, not WARN: nothing below ERROR reaches the tenant trace, so this deployment-time
            // finding was invisible exactly where it is needed. Runs once per start (#177).
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.start.topic.missing")
                    .with("topic", topic)
                    .with("consequence", "sending to this endpoint fails until the topic is created; "
                            + "the route is started anyway so the topic can be created without a "
                            + "redeployment"));
            return;
        }
        if (result.state == TopicCheck.EXISTS) {
            probeFailures.remove(topic);
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
            String hint = KafkaErrorHelper.tlsMismatchHint(securityProtocol.get());
            AdapterDiagnostics.Event event = AdapterDiagnostics.event("producer.start.broker.unreachable")
                    .with("bootstrapServers", bootstrapServers.get())
                    .with("topic", topic)
                    .with("consequence", "the route is started anyway; sending will report the same cause");
            if (hint != null) {
                event.with("hint", hint);
            }
            AdapterDiagnostics.error(LOG, event, result.cause);
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

        try (AdminClient admin = BundleBackedClassLoader.withBundleClassLoader(CpiKafkaPlusProducer.class,
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
    // Package-private for CpiKafkaPlusProducer and TopicMetadataCheckerTest.
    Properties buildTopicCheckProperties() {
        Properties producerProps = producerProperties.get();
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        for (String key : producerProps.stringPropertyNames()) {
            // The keystore alias is not an ssl.* key, but ssl.engine.factory.class is. Without the
            // alias the engine factory's configure() throws, the AdminClient cannot be created and
            // every probe on an mTLS channel ends INCONCLUSIVE.
            if (key.startsWith("ssl.") || key.startsWith("sasl.") || key.equals("security.protocol")
                    || key.equals(CpiKafkaPlusSslEngineFactory.SSL_KEYSTORE_ALIAS_CONFIG)) {
                adminProps.put(key, producerProps.getProperty(key));
            }
        }
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TOPIC_CHECK_TIMEOUT_MS);
        // Leave room for one retry inside the API timeout rather than making them equal.
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 2 * TOPIC_CHECK_TIMEOUT_MS);
        return adminProps;
    }
}
