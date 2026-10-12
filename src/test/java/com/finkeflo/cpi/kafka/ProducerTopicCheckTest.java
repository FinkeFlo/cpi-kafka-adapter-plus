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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLHandshakeException;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.finkeflo.cpi.kafka.TopicMetadataChecker.TopicCheckResult;

/**
 * Pins the receiver's topic checks: the AdminClient probe on the send path (a missing topic, a
 * rejected authentication or TLS handshake, an inconclusive answer remembered for the send error)
 * and at start-up, the probe's own client settings, the cache of confirmed topics, and the metadata
 * pre-warm of the shared producer. The producer runs its real send path and start-up; the probe's
 * verdicts ({@code topicProbe}) and the shared {@code KafkaProducer} are scripted, except where the
 * real probe fails before it would reach a broker. No broker.
 */
public class ProducerTopicCheckTest {

    private static final String BOOTSTRAP = "kafka-1.test:9092";
    private static final String CLIENT_ID = "cpi-kafka-plus-producer-topic-check-test";
    private static final String PRODUCER_CATEGORY = CpiKafkaPlusProducer.class.getName();
    private static final String NOT_IN_METADATA = " not present in metadata after 1000 ms.";
    private static final long WAIT_SECONDS = 10;

    private DefaultCamelContext ctx;
    private final List<CpiKafkaPlusProducer> producers = new ArrayList<>();
    private final List<StubProducer> stubs = new ArrayList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        Thread.interrupted();
        CredentialHelper.setCredentialResolver(null);
        executor.shutdownNow();
        for (CpiKafkaPlusProducer producer : producers) {
            producer.doStop();
        }
        for (StubProducer stub : stubs) {
            stub.close(Duration.ZERO);
        }
        ctx.close();
    }

    // ── send path ────────────────────────────────────────────────────────────────

    /**
     * A topic the broker reports as missing is asked about twice more, half a second apart, before
     * the exchange fails: a topic created a moment ago can still be absent from broker metadata. The
     * exchange fails before the pre-warm and before any send.
     */
    @Test
    public void aMissingTopicIsAskedAboutTwiceMoreThenFailsTheExchange() throws Exception {
        String topic = "orders-missing";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        StubProducer kafka = install(producer);
        ScriptedProbe probe = script(producer).answer(topic, missing());

        long started = System.nanoTime();
        try {
            send(producer, topic);
            Assert.fail("expected the send to a missing topic to fail");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Kafka topic 'orders-missing' does not exist on the broker. Please create "
                    + "the topic before sending to it (auto-create may be disabled on this cluster).",
                    e.getMessage());
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        Assert.assertEquals("one probe and two re-checks", 3, probe.calls(topic));
        Assert.assertTrue("two pauses of 500 ms between the probes, but took " + elapsedMs + " ms",
                elapsedMs >= 950);
        Assert.assertEquals("no pre-warm for a missing topic", 0, kafka.partitionsForCalls());
        Assert.assertEquals("no send to a missing topic", 0, kafka.sends());
    }

    /**
     * An interrupt ends the re-checks at once: the exchange fails as missing after the first answer
     * and the interrupt stays set. A missing topic is never cached, so the next message asks again.
     */
    @Test
    public void anInterruptEndsTheReChecksAndAMissingTopicIsNeverCached() throws Exception {
        String topic = "orders-missing-interrupted";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        install(producer);
        ScriptedProbe probe = script(producer).answer(topic, missing());

        for (int message = 1; message <= 2; message++) {
            Thread.currentThread().interrupt();
            try {
                send(producer, topic);
                Assert.fail("expected the send to a missing topic to fail");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage(),
                        e.getMessage().startsWith("Kafka topic 'orders-missing-interrupted' does not exist"));
            }
            Assert.assertTrue("the interrupt must stay set for the caller", Thread.interrupted());
            Assert.assertEquals("no re-check after an interrupt, and no cached answer", message,
                    probe.calls(topic));
        }
    }

    /** A topic that shows up on a re-check is sent to, and confirmed and pre-warmed only once. */
    @Test
    public void aTopicThatShowsUpOnAReCheckIsSentToAndRemembered() throws Exception {
        String topic = "orders-created-meanwhile";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        StubProducer kafka = install(producer);
        ScriptedProbe probe = script(producer).answer(topic, missing(), exists());

        send(producer, topic);
        send(producer, topic);

        Assert.assertEquals("one probe, one re-check, then remembered", 2, probe.calls(topic));
        Assert.assertEquals("pre-warmed once", 1, kafka.partitionsForCalls());
        Assert.assertEquals(2, kafka.sends());
    }

    /**
     * A probe that timed out does not fail the exchange: the pre-warm and the send go ahead. When the
     * send then fails, its error names the probe's cause, adds the TLS hint on a protocol without TLS,
     * and carries the probe failure as a suppressed exception.
     */
    @Test
    public void aTimedOutProbeLetsTheSendGoAheadAndItsCauseJoinsTheSendError() throws Exception {
        String topic = "orders-probe-timeout";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        StubProducer kafka = install(producer);
        Exception probeFailure = new TimeoutException("Timed out waiting for a node assignment.");
        ScriptedProbe probe = script(producer).answer(topic, inconclusive(probeFailure));
        RuntimeException sendError = metadataTimeout(topic);
        kafka.failSends(sendError);

        RuntimeException failure = sendExpectingFailure(producer, topic);

        Assert.assertEquals("Failed to send message to Kafka topic 'orders-probe-timeout': Topic "
                + "orders-probe-timeout" + NOT_IN_METADATA + " The broker could not be reached during the "
                + "pre-send check either (TimeoutException: Timed out waiting for a node assignment.). "
                + KafkaErrorHelper.tlsMismatchHint("PLAINTEXT"), failure.getMessage());
        Assert.assertSame(sendError, failure.getCause());
        Assert.assertEquals(Arrays.asList(probeFailure), Arrays.asList(failure.getSuppressed()));
        Assert.assertEquals(1, probe.calls(topic));
        Assert.assertEquals("the pre-warm still ran", 1, kafka.partitionsForCalls());
        Assert.assertEquals("the send still ran", 1, kafka.sends());
    }

    /** On a TLS channel the send error names the probe's cause without the TLS hint. */
    @Test
    public void theSendErrorOfATlsChannelNamesTheProbeCauseWithoutTheTlsHint() throws Exception {
        String topic = "orders-probe-timeout-tls";
        CpiKafkaPlusProducer producer = newProducer(topic, "SSL");
        StubProducer kafka = install(producer);
        script(producer).answer(topic,
                inconclusive(new TimeoutException("Timed out waiting for a node assignment.")));
        kafka.failSends(metadataTimeout(topic));

        Assert.assertEquals("Failed to send message to Kafka topic 'orders-probe-timeout-tls': Topic "
                + "orders-probe-timeout-tls" + NOT_IN_METADATA + " The broker could not be reached during "
                + "the pre-send check either (TimeoutException: Timed out waiting for a node assignment.).",
                sendExpectingFailure(producer, topic).getMessage());
    }

    /**
     * A confirmed topic is probed once per producer, and the confirmation clears a probe failure
     * remembered from an earlier message: a later send error no longer names it.
     */
    @Test
    public void aConfirmedTopicIsProbedOnceAndClearsTheRememberedProbeFailure() throws Exception {
        String topic = "orders-confirmed";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        StubProducer kafka = install(producer);
        ScriptedProbe probe = script(producer).answer(topic,
                inconclusive(new TimeoutException("Timed out waiting for a node assignment.")), exists());
        kafka.failSends(metadataTimeout(topic));

        RuntimeException first = sendExpectingFailure(producer, topic);
        Assert.assertTrue(first.getMessage(), first.getMessage().contains(
                "pre-send check either (TimeoutException: Timed out waiting for a node assignment.)"));

        RuntimeException second = sendExpectingFailure(producer, topic);
        Assert.assertEquals("Failed to send message to Kafka topic 'orders-confirmed': Topic orders-confirmed"
                + NOT_IN_METADATA, second.getMessage());
        Assert.assertEquals(0, second.getSuppressed().length);

        sendExpectingFailure(producer, topic);
        Assert.assertEquals("a confirmed topic is not probed again", 2, probe.calls(topic));
    }

    /**
     * An inconclusive probe without a cause (no Describe permission) forgets a probe failure
     * remembered before it, so the next send error does not name a stale cause.
     */
    @Test
    public void aProbeWithoutACauseForgetsTheRememberedProbeFailure() throws Exception {
        String topic = "orders-no-describe";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        StubProducer kafka = install(producer);
        ScriptedProbe probe = script(producer).answer(topic,
                inconclusive(new TimeoutException("Timed out waiting for a node assignment.")),
                inconclusive(null));
        kafka.failSends(metadataTimeout(topic));

        RuntimeException first = sendExpectingFailure(producer, topic);
        Assert.assertTrue(first.getMessage(), first.getMessage().contains("during the pre-send check either"));

        RuntimeException second = sendExpectingFailure(producer, topic);
        Assert.assertEquals("Failed to send message to Kafka topic 'orders-no-describe': Topic "
                + "orders-no-describe" + NOT_IN_METADATA, second.getMessage());
        Assert.assertEquals("an inconclusive answer is never remembered as a confirmation", 2,
                probe.calls(topic));
    }

    /**
     * A probe failure belongs to the topic whose probe failed (#246): it does not join the send error
     * of another topic, confirmed before or after it.
     */
    @Test
    public void aTopicsProbeFailureDoesNotJoinAnotherTopicsSendError() throws Exception {
        String confirmed = "orders-confirmed-before";
        String unreachable = "orders-unreachable-after";
        CpiKafkaPlusProducer producer = newProducer(confirmed, "SSL");
        install(producer);
        script(producer).answer(confirmed, exists())
                .answer(unreachable, inconclusive(new TimeoutException("Timed out waiting for a node assignment.")));

        send(producer, confirmed);
        send(producer, unreachable);

        Assert.assertEquals("Failed to send message to Kafka topic 'orders-confirmed-before': send failed",
                sendFailure(producer, confirmed, new RuntimeException("send failed")).getMessage());
    }

    /**
     * Confirming one topic does not clear the probe failure of another (#246): its next send error
     * still names the cause.
     */
    @Test
    public void aConfirmedTopicClearsOnlyItsOwnProbeFailure() throws Exception {
        String unreachable = "orders-unreachable-before";
        String confirmed = "orders-confirmed-after";
        CpiKafkaPlusProducer producer = newProducer(unreachable, "SSL");
        install(producer);
        script(producer).answer(unreachable, inconclusive(new TimeoutException("Timed out waiting for a node assignment.")))
                .answer(confirmed, exists());

        send(producer, unreachable);
        send(producer, confirmed);

        Assert.assertEquals("Failed to send message to Kafka topic 'orders-unreachable-before': send failed The "
                + "broker could not be reached during the pre-send check either (TimeoutException: Timed out "
                + "waiting for a node assignment.).",
                sendFailure(producer, unreachable, new RuntimeException("send failed")).getMessage());
    }

    /**
     * A rejected authentication found by the probe fails the exchange at once, before the pre-warm
     * and the send, naming the broker, the topic and the cause. The probe is not repeated, the topic
     * is not remembered, and the cause is not kept for a later send error: the exception carries it.
     */
    @Test
    public void anAuthenticationFailureFoundByTheProbeFailsTheExchangeAtOnce() throws Exception {
        String confirmed = "orders-confirmed-earlier";
        String topic = "orders-auth-rejected";
        CpiKafkaPlusProducer producer = newProducer(confirmed, "PLAINTEXT");
        StubProducer kafka = install(producer);
        Exception authFailure = new SaslAuthenticationException(
                "Authentication failed: Invalid username or password");
        ScriptedProbe probe = script(producer)
                .answer(confirmed, exists())
                .answer(topic, inconclusive(authFailure), exists());

        send(producer, confirmed);
        try {
            send(producer, topic);
            Assert.fail("expected the send to fail on the rejected authentication");
        } catch (IllegalStateException e) {
            Assert.assertEquals("Cannot connect to Kafka broker 'kafka-1.test:9092' for topic "
                    + "'orders-auth-rejected': SaslAuthenticationException: Authentication failed: Invalid "
                    + "username or password. Please check Security Protocol, Credential Alias and SSL "
                    + "Keystore Alias.", e.getMessage());
            Assert.assertSame(authFailure, e.getCause());
        }
        Assert.assertEquals("no re-check after a definitive failure", 1, probe.calls(topic));
        Assert.assertEquals("only the confirmed topic was pre-warmed", 1, kafka.partitionsForCalls());
        Assert.assertEquals("only the confirmed topic was sent to", 1, kafka.sends());

        kafka.failSends(metadataTimeout(confirmed));
        Assert.assertEquals("Failed to send message to Kafka topic 'orders-confirmed-earlier': Topic "
                + "orders-confirmed-earlier" + NOT_IN_METADATA,
                sendExpectingFailure(producer, confirmed).getMessage());

        sendExpectingFailure(producer, topic);
        Assert.assertEquals("the rejected topic is probed again", 2, probe.calls(topic));
    }

    /** A TLS failure anywhere in the probe's cause chain fails the exchange the same way. */
    @Test
    public void aTlsFailureFoundByTheProbeFailsTheExchangeAtOnce() throws Exception {
        String topic = "orders-tls-rejected";
        CpiKafkaPlusProducer producer = newProducer(topic, "SASL_SSL");
        StubProducer kafka = install(producer);
        Exception tlsFailure = new KafkaException("Failed to create new KafkaAdminClient",
                new SSLHandshakeException("PKIX path building failed"));
        ScriptedProbe probe = script(producer).answer(topic, inconclusive(tlsFailure));

        try {
            send(producer, topic);
            Assert.fail("expected the send to fail on the TLS failure");
        } catch (IllegalStateException e) {
            Assert.assertEquals("Cannot connect to Kafka broker 'kafka-1.test:9092' for topic "
                    + "'orders-tls-rejected': KafkaException: Failed to create new KafkaAdminClient <- "
                    + "SSLHandshakeException: PKIX path building failed. Please check Security Protocol, "
                    + "Credential Alias and SSL Keystore Alias.", e.getMessage());
            Assert.assertSame(tlsFailure, e.getCause());
        }
        Assert.assertEquals(1, probe.calls(topic));
        Assert.assertEquals(0, kafka.partitionsForCalls());
        Assert.assertEquals(0, kafka.sends());
    }

    /**
     * Confirmed and pre-warmed topics are each remembered up to 256. A topic beyond that is probed
     * on every send and not pre-warmed at all, so a flow with unbounded dynamic topics cannot grow
     * either set without limit.
     */
    @Test
    public void confirmedAndPreWarmedTopicsAreRememberedUpTo256() throws Exception {
        CpiKafkaPlusProducer producer = newProducer(topicNo(0), "PLAINTEXT");
        StubProducer kafka = install(producer);
        ScriptedProbe probe = script(producer).answerAll(exists());

        for (int i = 0; i <= 256; i++) {
            send(producer, topicNo(i));
        }
        send(producer, topicNo(0));
        send(producer, topicNo(256));

        Assert.assertEquals("the first 256 topics are confirmed once", 1, probe.calls(topicNo(0)));
        Assert.assertEquals(1, probe.calls(topicNo(255)));
        Assert.assertEquals("the 257th topic is probed on every send", 2, probe.calls(topicNo(256)));
        Assert.assertEquals("only the first 256 topics are pre-warmed", 256, kafka.partitionsForCalls());
        Assert.assertEquals(259, kafka.sends());
    }

    /**
     * A rebuilt producer starts with an empty metadata cache, so the topic is pre-warmed again on it;
     * the topic stays confirmed across the rebuild.
     */
    @Test
    public void aRebuiltProducerPreWarmsAgainWhileTheTopicStaysConfirmed() throws Exception {
        String topic = "orders-rebuilt";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        StubProducer first = install(producer);
        ScriptedProbe probe = script(producer).answer(topic, exists());

        send(producer, topic);
        invoke(producer, "triggerReconnect", new Class<?>[0]);
        StubProducer second = install(producer);
        send(producer, topic);
        send(producer, topic);

        Assert.assertEquals("the topic stays confirmed across the rebuild", 1, probe.calls(topic));
        Assert.assertEquals(1, first.partitionsForCalls());
        Assert.assertEquals("pre-warmed again on the new producer, once", 1, second.partitionsForCalls());
    }

    // ── the probe's own client ───────────────────────────────────────────────────

    /**
     * The probe's AdminClient takes the bootstrap servers and the security settings of the producer
     * config and nothing else, with its own short timeouts: 5 s per request and 10 s per call, because
     * the probe also runs at start-up, once per endpoint.
     */
    @Test
    public void theProbeClientTakesTheConnectionSettingsAndItsOwnShortTimeoutsOnly() throws Exception {
        CredentialHelper.setCredentialResolver(
                alias -> new CredentialHelper.UserCredentials("probe-user", "probe-secret"));
        CpiKafkaPlusEndpoint endpoint = endpoint("orders-probe-client", "SASL_SSL", BOOTSTRAP);
        endpoint.setSaslMechanism("PLAIN");
        endpoint.setCredentialAlias("kafka-user");
        endpoint.setSslKeystoreAlias("tenant-kafka");

        Map<Object, Object> expected = new HashMap<>();
        expected.put("bootstrap.servers", BOOTSTRAP);
        expected.put("security.protocol", "SASL_SSL");
        expected.put("sasl.mechanism", "PLAIN");
        expected.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"probe-user\" password=\"probe-secret\";");
        expected.put("ssl.engine.factory.class", CpiKafkaPlusSslEngineFactory.class.getName());
        expected.put("cpi.kafka.ssl.keystore.alias", "tenant-kafka");
        expected.put("request.timeout.ms", 5_000);
        expected.put("default.api.timeout.ms", 10_000);
        Assert.assertEquals(expected, new HashMap<>(newProducer(endpoint).buildTopicCheckProperties()));
    }

    /**
     * A probe whose AdminClient cannot even be created (here a bootstrap address without a port)
     * learns nothing: the send goes ahead, every send logs why and asks again, and the error of a
     * send that fails names the reason.
     */
    @Test
    public void aProbeClientThatCannotBeCreatedLetsTheSendGoAheadAndNamesWhy() throws Exception {
        String topic = "orders-probe-client-failed";
        CpiKafkaPlusProducer producer = newProducer(endpoint(topic, "PLAINTEXT", "kafka-1.test"));
        StubProducer kafka = install(producer);

        String logged = capture(() -> {
            send(producer, topic);
            send(producer, topic);
        });

        String why = "KafkaException: Failed to create new KafkaAdminClient <- ConfigException: "
                + "Invalid url in bootstrap.servers: kafka-1.test";
        Matcher warning = Pattern.compile(Pattern.quote("WARN " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "AdminClient creation failed while checking topic '" + topic + "' existence (" + why
                + "). Skipping the check.")).matcher(logged);
        Assert.assertTrue(logged, warning.find());
        Assert.assertTrue("asked again on the second send: " + logged, warning.find());
        Assert.assertEquals("both messages went out", 2, kafka.sends());
        String sendError = sendFailure(producer, topic, metadataTimeout(topic)).getMessage();
        Assert.assertTrue(sendError, sendError.contains(
                " The broker could not be reached during the pre-send check either (" + why + ")."));
    }

    // ── pre-warm ─────────────────────────────────────────────────────────────────

    /**
     * A failed pre-warm is reported at ERROR with the time it took and does not fail the exchange;
     * the next message tries again.
     */
    @Test
    public void aFailedPreWarmIsReportedAndTheNextMessageTriesAgain() throws Exception {
        String topic = "orders-prewarm-failed";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        StubProducer kafka = install(producer);
        script(producer).answer(topic, exists());
        kafka.failNextPartitionsFor(new IllegalStateException("metadata fetch failed"));

        String logged = capture(() -> send(producer, topic));
        Assert.assertEquals("the exchange was sent anyway", 1, kafka.sends());
        Matcher line = Pattern.compile(Pattern.quote("ERROR " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "producer.metadata.prewarm outcome=FAILED topic=" + topic + " metadataWaitMs=") + "\\d+"
                + Pattern.quote(" producerPath=SHARED clientId=" + CLIENT_ID + " thread="
                        + Thread.currentThread().getName()
                        + " error='java.lang.IllegalStateException(\"metadata fetch failed\") at ["))
                .matcher(logged);
        Assert.assertTrue(logged, line.find());

        send(producer, topic);
        Assert.assertEquals("a failed pre-warm is tried again", 2, kafka.partitionsForCalls());
    }

    /** A pre-warm slower than one second is reported at ERROR although it succeeded; a fast one is not. */
    @Test
    public void aSlowPreWarmIsReportedAndAFastOneIsNot() throws Exception {
        String slow = "orders-prewarm-slow";
        String fast = "orders-prewarm-fast";
        CpiKafkaPlusProducer producer = newProducer(slow, "PLAINTEXT");
        StubProducer kafka = install(producer);
        script(producer).answer(slow, exists()).answer(fast, exists());

        kafka.delayPartitionsFor(1100);
        String logged = capture(() -> send(producer, slow));
        Matcher line = Pattern.compile(Pattern.quote("ERROR " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "producer.metadata.prewarm: outcome=SLOW, topic=" + slow + ", metadataWaitMs=") + "(\\d+)"
                + Pattern.quote(", clientId=" + CLIENT_ID + ", producerPath=SHARED, thread="
                        + Thread.currentThread().getName())).matcher(logged);
        Assert.assertTrue(logged, line.find());
        Assert.assertTrue(line.group(1), Long.parseLong(line.group(1)) > 1000);

        kafka.delayPartitionsFor(0);
        String quiet = capture(() -> send(producer, fast));
        Assert.assertFalse(quiet, quiet.contains("outcome=SLOW, topic=" + fast));
        Assert.assertEquals(2, kafka.partitionsForCalls());
    }

    /**
     * While one sender pre-warms a topic, a second sender to the same topic neither fetches it again
     * nor waits for the first: it sends right away.
     */
    @Test
    public void aSecondSenderDoesNotPreWarmATopicThatIsBeingPreWarmed() throws Exception {
        String topic = "orders-prewarm-concurrent";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        StubProducer kafka = install(producer);
        script(producer).answer(topic, exists());
        CountDownLatch release = kafka.holdNextPartitionsFor();

        Future<?> first = executor.submit(() -> {
            send(producer, topic);
            return null;
        });
        Assert.assertTrue("the first sender did not reach the pre-warm", kafka.awaitPartitionsFor(WAIT_SECONDS));
        send(producer, topic);
        Assert.assertEquals("the second sender did not fetch again", 1, kafka.partitionsForCalls());
        Assert.assertEquals("the second sender did not wait for the first", 1, kafka.sends());

        release.countDown();
        first.get(WAIT_SECONDS, TimeUnit.SECONDS);
        send(producer, topic);
        Assert.assertEquals("pre-warmed once", 1, kafka.partitionsForCalls());
        Assert.assertEquals(3, kafka.sends());
    }

    /** The pre-warm repeats a fetch that hit the KAFKA-10902 monitor fault, with a budget of its own. */
    @Test
    public void aPreWarmThatHitsTheMonitorFaultIsRepeated() throws Exception {
        String topic = "orders-prewarm-monitor-fault";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        StubProducer kafka = install(producer);
        script(producer).answer(topic, exists());
        kafka.failNextPartitionsFor(monitorFault());

        String logged = capture(() -> send(producer, topic));
        Assert.assertEquals("one failed fetch and one repeat within the same message", 2,
                kafka.partitionsForCalls());
        Assert.assertTrue(logged, logged.contains("producer.send.monitorFaultRetry topic=" + topic
                + " recordIndex=-1 retryCount=1 recordRetryLimit=10 batchRetriesLeft=15"));

        send(producer, topic);
        Assert.assertEquals("pre-warmed by the repeat", 2, kafka.partitionsForCalls());
    }

    // ── start-up ─────────────────────────────────────────────────────────────────

    /**
     * At start-up a missing topic is reported at ERROR from a daemon thread named after the topic,
     * and the route starts without waiting for it. The probe is not repeated there, and the answer is
     * not remembered: the first send asks again.
     */
    @Test
    public void startUpReportsAMissingTopicFromABackgroundThreadAndStartsAnyway() throws Exception {
        String topic = "orders-startup-missing";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        ScriptedProbe probe = script(producer).answer(topic, missing(), exists());
        CountDownLatch release = probe.holdNextCall();
        CountDownLatch probed = probe.nextCall();

        String logged = capture(() -> {
            producer.doStart();
            Assert.assertTrue("the start-up probe did not run", probed.await(WAIT_SECONDS, TimeUnit.SECONDS));
            Thread checker = probe.lastThread();
            Assert.assertNotSame("the probe must not run on the starting thread", Thread.currentThread(), checker);
            Assert.assertTrue("start-up must not wait for the probe", checker.isAlive());
            release.countDown();
            awaitEnd(checker);
        });

        Assert.assertEquals(1, probe.calls(topic));
        Assert.assertEquals("cpi-kafka-plus-topic-check-" + topic, probe.lastThread().getName());
        Assert.assertTrue("the probe thread must not keep the JVM alive", probe.lastThread().isDaemon());
        Assert.assertTrue(logged, logged.contains("ERROR " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "producer.start.topic.missing topic=" + topic + " consequence='sending to this endpoint fails "
                + "until the topic is created; the route is started anyway so the topic can be created without "
                + "a redeployment'"));

        install(producer);
        send(producer, topic);
        Assert.assertEquals("a missing topic is not remembered at start-up either", 2, probe.calls(topic));
    }

    /**
     * A topic confirmed at start-up is logged at INFO and remembered, so the first send does not ask
     * again. The confirmation also clears a probe failure remembered before the start.
     */
    @Test
    public void startUpConfirmsAnExistingTopicAndClearsTheRememberedProbeFailure() throws Exception {
        String topic = "orders-startup-exists";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        StubProducer kafka = install(producer);
        ScriptedProbe probe = script(producer).answer(topic,
                inconclusive(new TimeoutException("Timed out waiting for a node assignment.")), exists());
        kafka.failSends(metadataTimeout(topic));
        Assert.assertTrue(sendExpectingFailure(producer, topic).getMessage()
                .contains("during the pre-send check either"));

        String logged = capture(() -> startAndAwaitProbe(producer, probe));

        Assert.assertTrue(logged, logged.contains("INFO " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "Kafka topic '" + topic + "' confirmed to exist at startup"));
        Assert.assertEquals("Failed to send message to Kafka topic 'orders-startup-exists': Topic "
                + "orders-startup-exists" + NOT_IN_METADATA, sendExpectingFailure(producer, topic).getMessage());
        Assert.assertEquals("confirmed at start-up, so the send does not ask again", 2, probe.calls(topic));
    }

    /**
     * An unreachable broker at start-up is reported at ERROR with the probe's cause and, on a
     * protocol without TLS, the TLS hint. The cause is not remembered for the send path.
     */
    @Test
    public void startUpReportsAnUnreachableBrokerWithTheCauseAndTheTlsHint() throws Exception {
        String topic = "orders-startup-unreachable";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        ScriptedProbe probe = script(producer).answer(topic,
                inconclusive(new TimeoutException("Timed out waiting for a node assignment.")));

        String logged = capture(() -> startAndAwaitProbe(producer, probe));

        Assert.assertTrue(logged, logged.contains("ERROR " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "producer.start.broker.unreachable bootstrapServers=" + BOOTSTRAP + " topic=" + topic
                + " consequence='the route is started anyway; sending will report the same cause' hint='"
                + KafkaErrorHelper.tlsMismatchHint("PLAINTEXT") + "' error='java.util.concurrent.TimeoutException("
                + "\"Timed out waiting for a node assignment.\") at ["));
        Assert.assertEquals("Failed to send message to Kafka topic 'orders-startup-unreachable': send failed",
                sendFailure(producer, topic, new RuntimeException("send failed")).getMessage());
    }

    /** On a TLS channel the start-up report of an unreachable broker carries no TLS hint. */
    @Test
    public void startUpReportsAnUnreachableBrokerWithoutTheTlsHintOnATlsChannel() throws Exception {
        String topic = "orders-startup-unreachable-tls";
        CpiKafkaPlusProducer producer = newProducer(topic, "SSL");
        ScriptedProbe probe = script(producer).answer(topic,
                inconclusive(new TimeoutException("Timed out waiting for a node assignment.")));

        String logged = capture(() -> startAndAwaitProbe(producer, probe));

        Assert.assertTrue(logged, logged.contains("ERROR " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "producer.start.broker.unreachable bootstrapServers=" + BOOTSTRAP + " topic=" + topic
                + " consequence='the route is started anyway; sending will report the same cause' "
                + "error='java.util.concurrent.TimeoutException(\"Timed out waiting for a node assignment.\") at ["));
    }

    /** An inconclusive start-up probe without a cause (no Describe permission) reports nothing. */
    @Test
    public void startUpStaysQuietWhenTheProbeHasNeitherAnswerNorCause() throws Exception {
        String topic = "orders-startup-no-describe";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        ScriptedProbe probe = script(producer).answer(topic, inconclusive(null));

        String logged = capture(() -> startAndAwaitProbe(producer, probe));

        Assert.assertEquals(1, probe.calls(topic));
        Assert.assertFalse(logged, logged.contains("topic=" + topic));
        Assert.assertFalse(logged, logged.contains("Kafka topic '" + topic + "'"));
    }

    /** A topic given as an expression is only known per message, so start-up does not probe it. */
    @Test
    public void startUpLeavesATopicExpressionToTheFirstSend() throws Exception {
        String topic = "${header.targetTopic}";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        ScriptedProbe probe = script(producer).answerAll(exists());

        String logged = capture(producer::doStart);

        Assert.assertTrue(logged, logged.contains("INFO " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "Topic '${header.targetTopic}' is an expression and can only be resolved per message, so its "
                + "existence is checked on first send instead of at startup."));
        Assert.assertEquals(0, probe.totalCalls());
    }

    /** A start-up probe that fails unexpectedly is logged as a WARN and takes nothing down with it. */
    @Test
    public void anUnexpectedFailureOfTheStartUpProbeIsOnlyAWarning() throws Exception {
        String topic = "orders-startup-probe-broken";
        CpiKafkaPlusProducer producer = newProducer(topic, "PLAINTEXT");
        ScriptedProbe probe = script(producer).answer(topic, failing(new IllegalStateException("probe exploded")));

        String logged = capture(() -> startAndAwaitProbe(producer, probe));

        Assert.assertTrue(logged, logged.contains("WARN " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "Startup topic check for '" + topic + "' failed unexpectedly: probe exploded"));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private CpiKafkaPlusProducer newProducer(String topic, String securityProtocol) throws Exception {
        return newProducer(endpoint(topic, securityProtocol, BOOTSTRAP));
    }

    private CpiKafkaPlusEndpoint endpoint(String topic, String securityProtocol, String bootstrapServers) {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(ctx);
        endpoint.setBootstrapServers(bootstrapServers);
        endpoint.setSecurityProtocol(securityProtocol);
        endpoint.setTopic(topic);
        endpoint.setProducerBatchMode("NONE");
        return endpoint;
    }

    private CpiKafkaPlusProducer newProducer(CpiKafkaPlusEndpoint endpoint) throws Exception {
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        // Normally assigned in doStart(); injected so the send path can run without a start.
        setField(producer, "tracingHelper", new AdapterTracingHelper(endpoint));
        producers.add(producer);
        return producer;
    }

    /** Hands the producer a scripted shared producer, as if it had just been initialized. */
    private StubProducer install(CpiKafkaPlusProducer producer) throws Exception {
        StubProducer stub = new StubProducer();
        stubs.add(stub);
        setField(producer, "kafkaProducer", stub);
        setField(producer, "initialized", true);
        setField(producer, "resolvedClientId", CLIENT_ID);
        // Keeps a failing send from rebuilding the producer in the middle of a test.
        setField(producer, "lastRebuildAttemptMs", System.currentTimeMillis());
        return stub;
    }

    private static ScriptedProbe script(CpiKafkaPlusProducer producer) {
        ScriptedProbe probe = new ScriptedProbe();
        producer.topicChecker.topicProbe = probe;
        return probe;
    }

    private void send(CpiKafkaPlusProducer producer, String topic) throws Exception {
        Exchange exchange = new DefaultExchange(ctx);
        exchange.getIn().setBody("payload");
        exchange.getIn().setHeader("CamelKafkaTopic", topic);
        producer.doProcess(exchange);
    }

    private RuntimeException sendExpectingFailure(CpiKafkaPlusProducer producer, String topic) throws Exception {
        try {
            send(producer, topic);
        } catch (RuntimeException e) {
            return e;
        }
        throw new AssertionError("expected the send to '" + topic + "' to fail");
    }

    /** The producer's own error for a failed send, built from what the probes left behind. */
    private static RuntimeException sendFailure(CpiKafkaPlusProducer producer, String topic, Exception cause)
            throws Exception {
        return (RuntimeException) invoke(producer, "sendFailure",
                new Class<?>[] { String.class, String.class, Exception.class },
                "Failed to send message to", topic, cause);
    }

    private static void startAndAwaitProbe(CpiKafkaPlusProducer producer, ScriptedProbe probe) throws Exception {
        CountDownLatch probed = probe.nextCall();
        producer.doStart();
        Assert.assertTrue("the start-up probe did not run", probed.await(WAIT_SECONDS, TimeUnit.SECONDS));
        awaitEnd(probe.lastThread());
    }

    /** Waits for the start-up probe's thread to finish, so everything it logs is captured. */
    private static void awaitEnd(Thread thread) throws InterruptedException {
        Assert.assertNotSame("the start-up probe ran on the test thread", Thread.currentThread(), thread);
        thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        Assert.assertFalse("the start-up probe thread did not finish", thread.isAlive());
    }

    private static String topicNo(int i) {
        return String.format("orders-%03d", i);
    }

    private static RuntimeException metadataTimeout(String topic) {
        return new org.apache.kafka.common.errors.TimeoutException("Topic " + topic + NOT_IN_METADATA);
    }

    /** KAFKA-10902 as the producer throws it: the bare message of Object.wait(), from a Kafka frame. */
    private static RuntimeException monitorFault() {
        IllegalMonitorStateException imse = new IllegalMonitorStateException("current thread is not owner");
        imse.setStackTrace(new StackTraceElement[] {
            new StackTraceElement("org.apache.kafka.common.utils.SystemTime", "waitObject", "SystemTime.java", 62),
            new StackTraceElement("org.apache.kafka.clients.producer.internals.ProducerMetadata",
                    "awaitUpdate", "ProducerMetadata.java", 119),
        });
        return new RuntimeException("partitionsFor failed: " + imse.getMessage(), imse);
    }

    private static Supplier<TopicCheckResult> exists() {
        return () -> TopicCheckResult.EXISTS;
    }

    private static Supplier<TopicCheckResult> missing() {
        return () -> TopicCheckResult.MISSING;
    }

    private static Supplier<TopicCheckResult> inconclusive(Throwable cause) {
        return () -> TopicCheckResult.inconclusive(cause);
    }

    private static Supplier<TopicCheckResult> failing(RuntimeException failure) {
        return () -> {
            throw failure;
        };
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Runs {@code action} and returns what was logged meanwhile; slf4j-simple writes to System.err. */
    private static String capture(ThrowingRunnable action) throws Exception {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, "UTF-8"));
        try {
            action.run();
        } finally {
            System.setErr(original);
        }
        return new String(captured.toByteArray(), StandardCharsets.UTF_8);
    }

    private static Object invoke(Object target, String name, Class<?>[] types, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(name, types);
        method.setAccessible(true);
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw (Error) cause;
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    /**
     * The topic probe's verdicts per topic, in order; the last one repeats. Records how often each
     * topic was asked about and on which thread.
     */
    private static final class ScriptedProbe implements Function<String, TopicCheckResult> {
        private final Map<String, List<Supplier<TopicCheckResult>>> answers = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        private volatile CountDownLatch nextCall = new CountDownLatch(1);
        private volatile List<Supplier<TopicCheckResult>> defaultAnswer;
        private volatile CountDownLatch hold;
        private volatile Thread lastThread;

        @SafeVarargs
        final ScriptedProbe answer(String topic, Supplier<TopicCheckResult>... verdicts) {
            answers.put(topic, Arrays.asList(verdicts));
            return this;
        }

        ScriptedProbe answerAll(Supplier<TopicCheckResult> verdict) {
            defaultAnswer = Collections.singletonList(verdict);
            return this;
        }

        /** A latch the next call releases, after it has recorded its thread. */
        CountDownLatch nextCall() {
            CountDownLatch latch = new CountDownLatch(1);
            nextCall = latch;
            return latch;
        }

        /** The next call waits until the returned latch is released, at most {@link #WAIT_SECONDS}. */
        CountDownLatch holdNextCall() {
            CountDownLatch latch = new CountDownLatch(1);
            hold = latch;
            return latch;
        }

        @Override
        public TopicCheckResult apply(String topic) {
            lastThread = Thread.currentThread();
            int call = calls.computeIfAbsent(topic, t -> new AtomicInteger()).getAndIncrement();
            nextCall.countDown();
            CountDownLatch latch = hold;
            if (latch != null) {
                hold = null;
                try {
                    latch.await(WAIT_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            List<Supplier<TopicCheckResult>> script = answers.getOrDefault(topic, defaultAnswer);
            if (script == null) {
                throw new AssertionError("unexpected probe for topic " + topic);
            }
            return script.get(Math.min(call, script.size() - 1)).get();
        }

        int calls(String topic) {
            AtomicInteger count = calls.get(topic);
            return count == null ? 0 : count.get();
        }

        int totalCalls() {
            int total = 0;
            for (AtomicInteger count : calls.values()) {
                total += count.get();
            }
            return total;
        }

        Thread lastThread() {
            return lastThread;
        }
    }

    /**
     * The shared producer: counts metadata fetches and sends, and fails, delays or holds them on
     * demand. No broker is ever contacted.
     */
    private static final class StubProducer extends KafkaProducer<byte[], byte[]> {
        private final AtomicInteger partitionsForCalls = new AtomicInteger();
        private final AtomicInteger sends = new AtomicInteger();
        private final CountDownLatch partitionsForEntered = new CountDownLatch(1);
        private volatile RuntimeException nextPartitionsForFailure;
        private volatile CountDownLatch nextPartitionsForHold;
        private volatile long partitionsForDelayMs;
        private volatile RuntimeException sendFailure;

        StubProducer() {
            super(stubProps(), new ByteArraySerializer(), new ByteArraySerializer());
        }

        void failNextPartitionsFor(RuntimeException failure) {
            nextPartitionsForFailure = failure;
        }

        CountDownLatch holdNextPartitionsFor() {
            CountDownLatch latch = new CountDownLatch(1);
            nextPartitionsForHold = latch;
            return latch;
        }

        void delayPartitionsFor(long millis) {
            partitionsForDelayMs = millis;
        }

        void failSends(RuntimeException failure) {
            sendFailure = failure;
        }

        boolean awaitPartitionsFor(long seconds) throws InterruptedException {
            return partitionsForEntered.await(seconds, TimeUnit.SECONDS);
        }

        int partitionsForCalls() {
            return partitionsForCalls.get();
        }

        int sends() {
            return sends.get();
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            partitionsForCalls.incrementAndGet();
            partitionsForEntered.countDown();
            RuntimeException failure = nextPartitionsForFailure;
            if (failure != null) {
                nextPartitionsForFailure = null;
                throw failure;
            }
            CountDownLatch latch = nextPartitionsForHold;
            try {
                if (latch != null) {
                    nextPartitionsForHold = null;
                    latch.await(WAIT_SECONDS, TimeUnit.SECONDS);
                }
                if (partitionsForDelayMs > 0) {
                    Thread.sleep(partitionsForDelayMs);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Collections.emptyList();
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            sends.incrementAndGet();
            RuntimeException failure = sendFailure;
            if (failure != null) {
                throw failure;
            }
            return java.util.concurrent.CompletableFuture.completedFuture(
                    new RecordMetadata(new TopicPartition(record.topic(), 0), 0L, 0, 0L, 0, 0));
        }
    }

    private static Properties stubProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-topic-check-test");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        return props;
    }
}
