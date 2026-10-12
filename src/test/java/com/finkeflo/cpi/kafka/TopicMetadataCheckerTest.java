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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.finkeflo.cpi.kafka.TopicMetadataChecker.TopicCheckResult;

/**
 * {@link TopicMetadataChecker} on its own, without a Camel context and without a broker: what it
 * reads from the producer and when, what a replaced client resets, the settings of the probe's own
 * client, and a probe whose settings cannot be built. How the producer uses the checker is pinned
 * in {@link ProducerTopicCheckTest}.
 */
public class TopicMetadataCheckerTest {

    private static final String BOOTSTRAP = "kafka-1.test:9092";
    private static final String PRODUCER_CATEGORY = CpiKafkaPlusProducer.class.getName();
    private static final long WAIT_SECONDS = 10;

    private final List<StubClient> stubs = new ArrayList<>();

    @After
    public void closeStubs() {
        for (StubClient stub : stubs) {
            stub.close(Duration.ZERO);
        }
    }

    /**
     * The producer builds its checker in its own constructor, before it has a client, a client id
     * or a started route, so the checker reads nothing from it until it is used.
     */
    @Test
    public void buildingTheCheckerReadsNothingFromTheProducer() {
        AtomicInteger reads = new AtomicInteger();

        new TopicMetadataChecker(counting(reads, "orders"), counting(reads, BOOTSTRAP),
                counting(reads, "PLAINTEXT"), counting(reads, new Properties()),
                counting(reads, (KafkaProducer<byte[], byte[]>) null), counting(reads, "client"));

        Assert.assertEquals(0, reads.get());
    }

    /**
     * The pre-warm uses whichever client the producer has at that moment and does nothing while it
     * has none. A replaced client is pre-warmed again, while a confirmed topic stays confirmed: the
     * topic did not go away with the client.
     */
    @Test
    public void aReplacedClientIsPreWarmedAgainWhileTheTopicStaysConfirmed() {
        AtomicReference<KafkaProducer<byte[], byte[]>> client = new AtomicReference<>();
        TopicMetadataChecker checker = checker(() -> "orders", Properties::new, client::get, () -> null);
        RecordingProbe probe = probe(checker, TopicCheckResult.EXISTS);

        checker.assertTopicExists("orders");
        checker.prewarmTopicMetadata("orders");
        StubClient first = stub();
        client.set(first);
        checker.prewarmTopicMetadata("orders");
        checker.prewarmTopicMetadata("orders");
        Assert.assertEquals("pre-warmed once, and only once there was a client", 1, first.partitionsForCalls());

        checker.onProducerReplaced();
        StubClient second = stub();
        client.set(second);
        checker.assertTopicExists("orders");
        checker.prewarmTopicMetadata("orders");

        Assert.assertEquals("the new client is pre-warmed again", 1, second.partitionsForCalls());
        Assert.assertEquals(1, first.partitionsForCalls());
        Assert.assertEquals("the topic stays confirmed", 1, probe.calls());
    }

    /**
     * A pre-warm still running on the replaced client does not keep the new client from being
     * pre-warmed: the replacement forgets the topics in flight as well.
     */
    @Test
    public void aPreWarmStillRunningOnTheOldClientDoesNotHoldBackTheNewOne() throws Exception {
        StubClient first = stub();
        CountDownLatch release = first.holdNextPartitionsFor();
        AtomicReference<KafkaProducer<byte[], byte[]>> client = new AtomicReference<>(first);
        TopicMetadataChecker checker = checker(() -> "orders", Properties::new, client::get, () -> null);
        Thread oldPreWarm = new Thread(() -> checker.prewarmTopicMetadata("orders"), "old-client-pre-warm");
        oldPreWarm.start();
        try {
            Assert.assertTrue("the old client's pre-warm did not start", first.awaitPartitionsFor(WAIT_SECONDS));

            checker.onProducerReplaced();
            StubClient second = stub();
            client.set(second);
            checker.prewarmTopicMetadata("orders");

            Assert.assertEquals(1, second.partitionsForCalls());
        } finally {
            release.countDown();
            oldPreWarm.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        }
        Assert.assertFalse("the old client's pre-warm did not finish", oldPreWarm.isAlive());
    }

    /**
     * A rebuild that clears the producer right after the pre-warm read it is no pre-warm failure.
     * The pre-warm used to read the producer a second time for the fetch and reported that race as
     * a failed pre-warm with a {@code NullPointerException} (#246).
     */
    @Test
    public void aRebuildRightAfterThePreWarmReadTheProducerIsNoPreWarmFailure() throws Exception {
        StubClient first = stub();
        AtomicReference<KafkaProducer<byte[], byte[]>> client = new AtomicReference<>(first);
        TopicMetadataChecker checker = checker(() -> "orders", Properties::new,
                () -> client.getAndSet(null), () -> null);

        String logged = ownLines(capture(() -> checker.prewarmTopicMetadata("orders")));

        Assert.assertFalse(logged, logged.contains("outcome=FAILED"));
        Assert.assertFalse(logged, logged.contains("NullPointerException"));
    }

    /**
     * A pre-warm that read the client just before a rebuild replaced it does not claim the topic for
     * the replaced client. Such a claim outlived the replacement and held back the new client's
     * pre-warm for as long as the fetch on the replaced client took.
     */
    @Test
    public void aPreWarmDoesNotClaimTheTopicForAClientReplacedBeforeTheClaim() throws Exception {
        StubClient first = stub();
        StubClient second = stub();
        CountDownLatch releaseFirst = first.holdNextPartitionsFor();
        AtomicReference<KafkaProducer<byte[], byte[]>> client = new AtomicReference<>(first);
        AtomicReference<TopicMetadataChecker> self = new AtomicReference<>();
        TopicMetadataChecker checker = checker(() -> "orders", Properties::new, () -> {
            KafkaProducer<byte[], byte[]> current = client.get();
            if (current == first) {
                // The rebuild, right after this read.
                replace(self.get(), client, second);
            }
            return current;
        }, () -> null);
        self.set(checker);
        Thread oldPreWarm = new Thread(() -> checker.prewarmTopicMetadata("orders"), "old-client-pre-warm");
        try {
            oldPreWarm.start();
            oldPreWarm.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            checker.prewarmTopicMetadata("orders");

            Assert.assertEquals("the new client is pre-warmed", 1, second.partitionsForCalls());
            Assert.assertEquals("nothing is fetched from the replaced client", 0, first.partitionsForCalls());
        } finally {
            releaseFirst.countDown();
            oldPreWarm.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        }
        Assert.assertFalse("the old client's pre-warm did not finish", oldPreWarm.isAlive());
    }

    /**
     * A pre-warm that ends after its client was replaced filled the cache of the replaced client, so
     * it does not count for the new one (#246): the next send pre-warms the new client.
     */
    @Test
    public void aPreWarmThatEndsOnAReplacedClientDoesNotCountForTheNewOne() {
        StubClient first = stub();
        StubClient second = stub();
        AtomicReference<KafkaProducer<byte[], byte[]>> client = new AtomicReference<>(first);
        TopicMetadataChecker checker = checker(() -> "orders", Properties::new, client::get, () -> null);
        first.onNextPartitionsFor(() -> replace(checker, client, second));

        checker.prewarmTopicMetadata("orders");
        checker.prewarmTopicMetadata("orders");

        Assert.assertEquals(1, first.partitionsForCalls());
        Assert.assertEquals("the new client is pre-warmed", 1, second.partitionsForCalls());
    }

    /**
     * A pre-warm whose client the rebuild closed under it is abandoned without an error: the rebuild
     * reported its own cause, and the new client is pre-warmed on the next send.
     */
    @Test
    public void aPreWarmWhoseClientIsClosedUnderItIsAbandonedQuietly() throws Exception {
        StubClient first = stub();
        AtomicReference<KafkaProducer<byte[], byte[]>> client = new AtomicReference<>(first);
        TopicMetadataChecker checker = checker(() -> "orders", Properties::new, client::get, () -> null);
        first.onNextPartitionsFor(() -> replace(checker, client, null));
        first.failNextPartitionsFor(new IllegalStateException("Cannot perform operation after producer has been closed"));

        String logged = ownLines(capture(() -> checker.prewarmTopicMetadata("orders")));

        Assert.assertFalse(logged, logged.contains("ERROR"));
        Assert.assertTrue(logged, logged.contains("INFO " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] Metadata "
                + "pre-warm of topic 'orders' abandoned after "));
        Assert.assertTrue(logged, logged.contains(" ms: the producer was replaced while it ran (IllegalStateException: "
                + "Cannot perform operation after producer has been closed)"));
    }

    /**
     * A pre-warm on a replaced client that ends while the new client's pre-warm is in flight leaves
     * the new one's marker alone, so a third sender does not fetch the topic a second time.
     */
    @Test
    public void aPreWarmOnAReplacedClientLeavesTheNewClientsMarkerAlone() throws Exception {
        StubClient first = stub();
        StubClient second = stub();
        CountDownLatch releaseFirst = first.holdNextPartitionsFor();
        first.failNextPartitionsFor(new IllegalStateException("Cannot perform operation after producer has been closed"));
        CountDownLatch releaseSecond = second.holdNextPartitionsFor();
        AtomicReference<KafkaProducer<byte[], byte[]>> client = new AtomicReference<>(first);
        TopicMetadataChecker checker = checker(() -> "orders", Properties::new, client::get, () -> null);
        Thread oldPreWarm = new Thread(() -> checker.prewarmTopicMetadata("orders"), "old-client-pre-warm");
        Thread newPreWarm = new Thread(() -> checker.prewarmTopicMetadata("orders"), "new-client-pre-warm");
        oldPreWarm.start();
        try {
            Assert.assertTrue("the old client's pre-warm did not start", first.awaitPartitionsFor(WAIT_SECONDS));
            replace(checker, client, second);
            newPreWarm.start();
            Assert.assertTrue("the new client's pre-warm did not start", second.awaitPartitionsFor(WAIT_SECONDS));

            releaseFirst.countDown();
            oldPreWarm.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            Assert.assertFalse("the old client's pre-warm did not finish", oldPreWarm.isAlive());
            checker.prewarmTopicMetadata("orders");

            Assert.assertEquals("the new client's pre-warm is still in flight", 1, second.partitionsForCalls());
        } finally {
            releaseFirst.countDown();
            releaseSecond.countDown();
            oldPreWarm.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            newPreWarm.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        }
        Assert.assertFalse("the new client's pre-warm did not finish", newPreWarm.isAlive());
    }

    /** A failed pre-warm names the client id the producer has at that moment, and none while it has none. */
    @Test
    public void aFailedPreWarmNamesTheClientIdOfTheMoment() throws Exception {
        StubClient client = stub();
        AtomicReference<String> clientId = new AtomicReference<>();
        TopicMetadataChecker checker = checker(() -> "orders", Properties::new, () -> client, clientId::get);

        client.failNextPartitionsFor(new IllegalStateException("metadata fetch failed"));
        String withoutId = capture(() -> checker.prewarmTopicMetadata("orders"));
        clientId.set("cpi-kafka-plus-producer-1");
        client.failNextPartitionsFor(new IllegalStateException("metadata fetch failed"));
        String withId = capture(() -> checker.prewarmTopicMetadata("orders"));

        String failed = "ERROR " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] producer.metadata.prewarm "
                + "outcome=FAILED topic=orders metadataWaitMs=";
        String thread = " thread=" + Thread.currentThread().getName() + " error=";
        Assert.assertTrue(withoutId, withoutId.contains(failed));
        Assert.assertTrue(withoutId, withoutId.contains(" producerPath=SHARED" + thread));
        Assert.assertTrue(withId, withId.contains(failed));
        Assert.assertTrue(withId, withId.contains(" producerPath=SHARED clientId=cpi-kafka-plus-producer-1" + thread));
        Assert.assertEquals("a failed pre-warm is tried again", 2, client.partitionsForCalls());
    }

    /** The start-up probe asks for the endpoint's topic trimmed, on its own daemon thread. */
    @Test
    public void theStartUpProbeAsksForTheTrimmedTopicOnItsOwnThread() throws Exception {
        TopicMetadataChecker checker = checker(() -> "  orders  ", Properties::new, () -> null, () -> null);
        RecordingProbe probe = probe(checker, TopicCheckResult.EXISTS);

        String logged = capture(() -> {
            checker.startTopicCheckInBackground();
            Assert.assertTrue("the start-up probe did not run", probe.awaitCall(WAIT_SECONDS));
            probe.thread().join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        });

        Assert.assertEquals(Collections.singletonList("orders"), probe.topics());
        Assert.assertEquals("cpi-kafka-plus-topic-check-orders", probe.thread().getName());
        Assert.assertTrue(probe.thread().isDaemon());
        Assert.assertFalse(probe.thread().isAlive());
        Assert.assertTrue(logged, logged.contains("INFO " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "Kafka topic 'orders' confirmed to exist at startup"));
    }

    /** Without a topic there is nothing to probe at start-up, to check before a send or to pre-warm. */
    @Test
    public void withoutATopicNothingIsProbedOrPreWarmed() {
        StubClient client = stub();
        AtomicReference<String> topic = new AtomicReference<>();
        TopicMetadataChecker checker = checker(topic::get, Properties::new, () -> client, () -> null);
        RecordingProbe probe = probe(checker, TopicCheckResult.MISSING);

        for (String none : Arrays.asList(null, "", "   ")) {
            topic.set(none);
            checker.startTopicCheckInBackground();
        }
        checker.assertTopicExists(null);
        checker.assertTopicExists("");
        checker.prewarmTopicMetadata(null);
        checker.prewarmTopicMetadata("");

        Assert.assertEquals(0, probe.calls());
        Assert.assertEquals(0, client.partitionsForCalls());
    }

    /**
     * The probe's client takes the bootstrap servers, {@code security.protocol}, every {@code ssl.}
     * and {@code sasl.} key and the CPI keystore alias from the producer config, nothing else, and
     * its own timeouts in place of the producer's.
     */
    @Test
    public void theProbeClientTakesOnlyTheConnectionAndSecurityKeys() {
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "kafka-1.test:9092,kafka-2.test:9092");
        producerConfig.put("security.protocol", "SASL_SSL");
        producerConfig.put("sasl.mechanism", "SCRAM-SHA-512");
        producerConfig.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required;");
        producerConfig.put("ssl.engine.factory.class", CpiKafkaPlusSslEngineFactory.class.getName());
        producerConfig.put("ssl.endpoint.identification.algorithm", "https");
        producerConfig.put("cpi.kafka.ssl.keystore.alias", "tenant-kafka");
        producerConfig.put("security.providers", "com.example.SecurityProviderCreator");
        producerConfig.put("client.id", "cpi-kafka-plus-producer-1");
        producerConfig.put("acks", "all");
        producerConfig.put("request.timeout.ms", "30000");
        producerConfig.put("max.block.ms", 30_000L);
        TopicMetadataChecker checker = checker(() -> "orders", () -> producerConfig, () -> null, () -> null);

        Map<Object, Object> expected = new HashMap<>();
        expected.put("bootstrap.servers", "kafka-1.test:9092,kafka-2.test:9092");
        expected.put("security.protocol", "SASL_SSL");
        expected.put("sasl.mechanism", "SCRAM-SHA-512");
        expected.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required;");
        expected.put("ssl.engine.factory.class", CpiKafkaPlusSslEngineFactory.class.getName());
        expected.put("ssl.endpoint.identification.algorithm", "https");
        expected.put("cpi.kafka.ssl.keystore.alias", "tenant-kafka");
        expected.put("request.timeout.ms", 5_000);
        expected.put("default.api.timeout.ms", 10_000);
        Assert.assertEquals(expected, new HashMap<>(checker.buildTopicCheckProperties()));
    }

    /**
     * When the probe's settings cannot be built, the probe learns nothing: a warning, no failure,
     * the cause kept for the error of a send that fails, and the next send asks again. A later
     * confirmation forgets the cause.
     */
    @Test
    public void aProbeWhoseSettingsCannotBeBuiltKeepsTheCauseForTheSendError() throws Exception {
        IllegalStateException unavailable = new IllegalStateException("Secure Store unavailable");
        AtomicInteger builds = new AtomicInteger();
        TopicMetadataChecker checker = checker(() -> "orders", () -> {
            builds.incrementAndGet();
            throw unavailable;
        }, () -> null, () -> null);

        String logged = capture(() -> {
            checker.assertTopicExists("orders");
            checker.assertTopicExists("orders");
        });

        Assert.assertEquals("asked again on the second send", 2, builds.get());
        Assert.assertSame(unavailable, checker.lastProbeFailure("orders"));
        Assert.assertTrue(logged, logged.contains("WARN " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "Could not build AdminClient configuration to check topic 'orders' "
                + "(IllegalStateException: Secure Store unavailable). Skipping the check."));

        checker.topicProbe = topic -> TopicCheckResult.EXISTS;
        checker.assertTopicExists("orders");
        Assert.assertNull("a confirmation forgets the cause", checker.lastProbeFailure("orders"));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    /** A checker on a plaintext channel to {@link #BOOTSTRAP}; the arguments are what the tests vary. */
    private static TopicMetadataChecker checker(Supplier<String> topic, Supplier<Properties> producerConfig,
                                                Supplier<KafkaProducer<byte[], byte[]>> client,
                                                Supplier<String> clientId) {
        return new TopicMetadataChecker(topic, () -> BOOTSTRAP, () -> "PLAINTEXT", producerConfig, client,
                clientId);
    }

    /** What a rebuild does: the producer's field changes first, then the checker forgets its client. */
    private static void replace(TopicMetadataChecker checker, AtomicReference<KafkaProducer<byte[], byte[]>> client,
                                KafkaProducer<byte[], byte[]> replacement) {
        client.set(replacement);
        checker.onProducerReplaced();
    }

    /** The lines the test's own thread logged; clients left behind by other tests log on theirs. */
    private static String ownLines(String logged) {
        String own = "[" + Thread.currentThread().getName() + "] ";
        StringBuilder lines = new StringBuilder();
        for (String line : logged.split("\\R")) {
            if (line.startsWith(own)) {
                lines.append(line).append('\n');
            }
        }
        return lines.toString();
    }

    private static RecordingProbe probe(TopicMetadataChecker checker, TopicCheckResult verdict) {
        RecordingProbe probe = new RecordingProbe(verdict);
        checker.topicProbe = probe;
        return probe;
    }

    private static <T> Supplier<T> counting(AtomicInteger reads, T value) {
        return () -> {
            reads.incrementAndGet();
            return value;
        };
    }

    private StubClient stub() {
        StubClient stub = new StubClient();
        stubs.add(stub);
        return stub;
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

    /** Answers every probe with one verdict and records which topics it was asked about, and where. */
    private static final class RecordingProbe implements Function<String, TopicCheckResult> {
        private final TopicCheckResult verdict;
        private final List<String> topics = new CopyOnWriteArrayList<>();
        private final CountDownLatch called = new CountDownLatch(1);
        private volatile Thread thread;

        RecordingProbe(TopicCheckResult verdict) {
            this.verdict = verdict;
        }

        @Override
        public TopicCheckResult apply(String topic) {
            topics.add(topic);
            thread = Thread.currentThread();
            called.countDown();
            return verdict;
        }

        int calls() {
            return topics.size();
        }

        List<String> topics() {
            return topics;
        }

        Thread thread() {
            return thread;
        }

        boolean awaitCall(long seconds) throws InterruptedException {
            return called.await(seconds, TimeUnit.SECONDS);
        }
    }

    /**
     * A client that never connects: counts metadata fetches, and can run a hook in, hold or fail the
     * next one, in that order.
     */
    private static final class StubClient extends KafkaProducer<byte[], byte[]> {
        private final AtomicInteger partitionsForCalls = new AtomicInteger();
        private final CountDownLatch partitionsForEntered = new CountDownLatch(1);
        private volatile Runnable nextHook;
        private volatile RuntimeException nextFailure;
        private volatile CountDownLatch nextHold;

        StubClient() {
            super(stubProps(), new ByteArraySerializer(), new ByteArraySerializer());
        }

        /** Runs {@code hook} inside the next fetch, for a rebuild that happens while it runs. */
        void onNextPartitionsFor(Runnable hook) {
            nextHook = hook;
        }

        void failNextPartitionsFor(RuntimeException failure) {
            nextFailure = failure;
        }

        CountDownLatch holdNextPartitionsFor() {
            CountDownLatch latch = new CountDownLatch(1);
            nextHold = latch;
            return latch;
        }

        boolean awaitPartitionsFor(long seconds) throws InterruptedException {
            return partitionsForEntered.await(seconds, TimeUnit.SECONDS);
        }

        int partitionsForCalls() {
            return partitionsForCalls.get();
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            partitionsForCalls.incrementAndGet();
            partitionsForEntered.countDown();
            Runnable hook = nextHook;
            if (hook != null) {
                nextHook = null;
                hook.run();
            }
            CountDownLatch hold = nextHold;
            if (hold != null) {
                nextHold = null;
                try {
                    hold.await(WAIT_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            RuntimeException failure = nextFailure;
            if (failure != null) {
                nextFailure = null;
                throw failure;
            }
            return Collections.emptyList();
        }

        private static Properties stubProps() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "topic-metadata-checker-test");
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
            return props;
        }
    }
}
