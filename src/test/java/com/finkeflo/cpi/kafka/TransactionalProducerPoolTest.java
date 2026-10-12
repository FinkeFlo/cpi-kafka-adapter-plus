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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link TransactionalProducerPool} on its own, without a Camel context and without a broker: the
 * {@code transactional.id} layout, the start-up checks and log lines, which slot a message gets and
 * when it has to wait, and the bounded close. How the producer uses the pool is pinned in
 * {@link ProducerTransactionSlotTest}.
 */
public class TransactionalProducerPoolTest {

    private static final String TOPIC = "test-topic";
    /** First four bytes of SHA-256("test-topic"), pinned in {@link NodeIdentityTest}. */
    private static final String TOPIC_HASH = "c6fb053c";
    private static final String PRODUCER_CATEGORY = CpiKafkaPlusProducer.class.getName();
    private static final long WAIT_MS = TimeUnit.SECONDS.toMillis(10);

    @Test
    public void theIdIsPrefixTopicHashMemberSuffixAndSlot() {
        TransactionalProducerPool pool = new TransactionalProducerPool(() -> "orders-tx", TOPIC_HASH, "3", 2);

        Assert.assertEquals("orders-tx-c6fb053c-3-0", pool.transactionalId(0));
        Assert.assertEquals("orders-tx-c6fb053c-3-1", pool.transactionalId(1));
        Assert.assertEquals(TOPIC_HASH, pool.topicHash());
    }

    @Test
    public void thePrefixIsReadForEveryId() {
        AtomicReference<String> prefix = new AtomicReference<>("before");
        TransactionalProducerPool pool = new TransactionalProducerPool(prefix::get, TOPIC_HASH, "3", 1);

        Assert.assertEquals("before-c6fb053c-3-0", pool.transactionalId(0));
        prefix.set("after");
        Assert.assertEquals("after-c6fb053c-3-0", pool.transactionalId(0));
    }

    @Test
    public void startResolvesTheTopicHashAndTheMemberSuffixOnce() throws Exception {
        AtomicInteger suffixReads = new AtomicInteger();
        AtomicReference<TransactionalProducerPool> started = new AtomicReference<>();
        String logged = capture(() -> started.set(TransactionalProducerPool.start(() -> "orders-tx", TOPIC,
                () -> {
                    suffixReads.incrementAndGet();
                    return "3";
                }, 4)));

        TransactionalProducerPool pool = started.get();
        Assert.assertEquals(TOPIC_HASH, pool.topicHash());
        Assert.assertEquals("orders-tx-c6fb053c-3-0", pool.transactionalId(0));
        Assert.assertEquals("orders-tx-c6fb053c-3-3", pool.transactionalId(3));
        Assert.assertEquals("the member suffix is read once, at start", 1, suffixReads.get());
        Assert.assertTrue(logged, logged.contains("INFO " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "Transactional batching enabled with max 4 concurrent transactions. Prefix: orders-tx, "
                + "topicHash: c6fb053c, memberSuffix: 3, example transactional.id: orders-tx-c6fb053c-3-0"));
        Assert.assertFalse(logged, logged.contains("Could not determine a stable identifier"));
    }

    @Test
    public void withoutANodeIdentifierStartUsesARandomSuffixAndWarns() throws Exception {
        AtomicReference<TransactionalProducerPool> started = new AtomicReference<>();
        String logged = capture(() -> started.set(
                TransactionalProducerPool.start(() -> "orders-tx", TOPIC, () -> null, 1)));

        String id = started.get().transactionalId(0);
        Matcher layout = Pattern.compile("orders-tx-c6fb053c-(r[0-9a-f]{12})-0").matcher(id);
        Assert.assertTrue(id, layout.matches());
        Assert.assertTrue(logged, logged.contains("WARN " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "Could not determine a stable identifier for this worker node, so a random one ('"
                + layout.group(1) + "') will be used instead. This is safe, but note that it will change on "
                + "every restart."));
        Assert.assertTrue(logged, logged.contains("memberSuffix: " + layout.group(1) + ", "));

        AtomicReference<TransactionalProducerPool> restarted = new AtomicReference<>();
        capture(() -> restarted.set(TransactionalProducerPool.start(() -> "orders-tx", TOPIC, () -> null, 1)));
        Assert.assertNotEquals("every start draws a new suffix", id, restarted.get().transactionalId(0));
    }

    /**
     * The last slot has the longest id, one character more from slot 10 on. With the 13 characters
     * of a random member suffix, a 224-character prefix gives 249 characters up to slot 9 and 250
     * for slot 10 (#262).
     */
    @Test
    public void startChecksTheIdOfTheLastSlot() throws Exception {
        String prefix = repeat('p', 224);
        String suffix = "r0123456789ab";
        AtomicReference<TransactionalProducerPool> started = new AtomicReference<>();
        capture(() -> started.set(TransactionalProducerPool.start(() -> prefix, TOPIC, () -> suffix, 10)));
        Assert.assertEquals("exactly 249 characters are accepted", 249, started.get().transactionalId(9).length());

        String logged = capture(() -> {
            try {
                TransactionalProducerPool.start(() -> prefix, TOPIC, () -> suffix, 11);
                Assert.fail("expected the start to fail");
            } catch (IllegalArgumentException expected) {
                Assert.assertEquals("'Transactional ID Prefix' (transactionalIdPrefix) must be at most 223 "
                        + "characters long, got: 224 characters. With 11 for 'Max Concurrent Transactions', the "
                        + "transactional.id of the last slot, '" + prefix + "-c6fb053c-" + suffix + "-10', would "
                        + "have 250 characters, and the adapter allows at most 249. Please shorten the prefix by "
                        + "at least 1 character.", expected.getMessage());
            }
        });
        Assert.assertFalse("a rejected start logs no start-up line:\n" + logged,
                ownLines(logged).contains("Transactional batching enabled"));
    }

    @Test
    public void startSaysByHowMuchToShortenThePrefix() {
        try {
            TransactionalProducerPool.start(() -> repeat('p', 300), TOPIC, () -> "3", 1);
            Assert.fail("expected the start to fail");
        } catch (IllegalArgumentException expected) {
            // "<prefix>-c6fb053c-3-0" is the prefix plus 13 characters.
            Assert.assertTrue(expected.getMessage(), expected.getMessage().startsWith("'Transactional ID Prefix' "
                    + "(transactionalIdPrefix) must be at most 236 characters long, got: 300 characters. With 1 "
                    + "for 'Max Concurrent Transactions', the transactional.id of the last slot, '"));
            Assert.assertTrue(expected.getMessage(), expected.getMessage().endsWith("would have 313 characters, "
                    + "and the adapter allows at most 249. Please shorten the prefix by at least 64 characters."));
        }
    }

    @Test
    public void aClaimTakesTheLowestFreeSlot() {
        TransactionalProducerPool pool = new TransactionalProducerPool(() -> "p", TOPIC_HASH, "3", 3);
        Assert.assertEquals(0, take(pool));
        Assert.assertEquals(1, take(pool));
        Assert.assertEquals(2, take(pool));

        pool.release(2);
        pool.release(0);

        Assert.assertEquals(0, take(pool));
        Assert.assertEquals(2, take(pool));
    }

    @Test(timeout = 20_000)
    public void aMessageWaitsWhileEverySlotIsTaken() throws Exception {
        TransactionalProducerPool pool = new TransactionalProducerPool(() -> "p", TOPIC_HASH, "3", 2);
        take(pool);
        int second = take(pool);
        Taker waiting = new Taker(pool, "waiting");
        waiting.start();
        awaitWaitingForSlot(waiting);

        pool.release(second);
        waiting.join(WAIT_MS);

        Assert.assertNull(String.valueOf(waiting.failure.get()), waiting.failure.get());
        Assert.assertEquals(second, waiting.slot.get());
    }

    /**
     * The semaphore is fair: a message that asks right after a release still queues behind one that
     * was already waiting. An unfair semaphore would most likely let the later one in first.
     */
    @Test(timeout = 20_000)
    public void aWaitingMessageGetsTheFreedSlotBeforeALaterOne() throws Exception {
        TransactionalProducerPool pool = new TransactionalProducerPool(() -> "p", TOPIC_HASH, "3", 1);
        List<String> order = Collections.synchronizedList(new ArrayList<String>());
        int held = take(pool);
        Thread waiting = new Thread(() -> takeAndGiveBack(pool, "waiting", order), "waiting");
        waiting.setDaemon(true);
        waiting.start();
        awaitWaitingForSlot(waiting);

        pool.release(held);
        takeAndGiveBack(pool, "later", order);
        waiting.join(WAIT_MS);

        Assert.assertEquals(Arrays.asList("waiting", "later"), order);
    }

    @Test(timeout = 20_000)
    public void anInterruptedWaitFailsWithoutTakingAPermit() throws Exception {
        TransactionalProducerPool pool = new TransactionalProducerPool(() -> "p", TOPIC_HASH, "3", 1);
        int held = take(pool);
        Taker interrupted = new Taker(pool, "interrupted");
        interrupted.start();
        awaitWaitingForSlot(interrupted);

        interrupted.interrupt();
        interrupted.join(WAIT_MS);

        RuntimeException failure = interrupted.failure.get();
        Assert.assertNotNull("the interrupted wait must fail", failure);
        Assert.assertEquals("Interrupted while waiting for a transaction slot", failure.getMessage());
        Assert.assertTrue(String.valueOf(failure.getCause()), failure.getCause() instanceof InterruptedException);
        Assert.assertTrue("the interrupt flag is set again for the caller", interrupted.interruptedWhenFailed);

        // Neither taken nor given back: the single permit is still held, so the next message waits.
        Taker next = new Taker(pool, "next");
        next.start();
        awaitWaitingForSlot(next);
        pool.release(held);
        next.join(WAIT_MS);
        Assert.assertEquals(held, next.slot.get());
    }

    @Test(timeout = 20_000)
    public void aReleaseWithoutASlotOnlyReturnsThePermit() {
        TransactionalProducerPool pool = new TransactionalProducerPool(() -> "p", TOPIC_HASH, "3", 2);
        Assert.assertEquals(0, take(pool));

        // As after a claim that failed: the permit was taken, no slot was marked.
        pool.acquire();
        pool.release(-1);

        Assert.assertEquals("slot 0 is still in use", 1, take(pool));
    }

    @Test
    public void aClaimWithoutAFreeSlotIsReportedAsABug() {
        TransactionalProducerPool pool = new TransactionalProducerPool(() -> "p", TOPIC_HASH, "3", 1);
        take(pool);

        try {
            pool.claimFreeSlot();
            Assert.fail("expected an IllegalStateException");
        } catch (IllegalStateException expected) {
            Assert.assertEquals("Acquired semaphore but no slot was free. This is a bug.", expected.getMessage());
        }
    }

    @Test
    public void theCloseIsBoundedAndAFailedCloseIsOnlyLogged() throws Exception {
        TransactionalProducerPool.closeTxnProducerQuietly(null, 0);

        ClosingProducer failing = new ClosingProducer(new KafkaException("injected close failure"));
        String logged = capture(() -> TransactionalProducerPool.closeTxnProducerQuietly(failing, 2));

        Assert.assertEquals(Duration.ofSeconds(5), failing.closeTimeout);
        Assert.assertTrue(logged, logged.contains("WARN " + PRODUCER_CATEGORY + " - [CPI-KAFKA-PLUS-DIAG] "
                + "Failed to close transactional producer for slot 2: injected close failure"));
    }

    @Test
    public void anInvalidConfigurationReachesTheCallerUnwrapped() {
        Properties props = stubProps();
        props.put(ProducerConfig.ACKS_CONFIG, "sometimes");

        try {
            TransactionalProducerPool.createTransactionalProducer(props).close(Duration.ZERO);
            Assert.fail("expected a ConfigException");
        } catch (ConfigException expected) {
            // Unwrapped, so the retry classification sees the client's own exception.
        }
    }

    private static int take(TransactionalProducerPool pool) {
        pool.acquire();
        return pool.claimFreeSlot();
    }

    private static void takeAndGiveBack(TransactionalProducerPool pool, String who, List<String> order) {
        int slot = take(pool);
        order.add(who);
        pool.release(slot);
    }

    private static String repeat(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
    }

    /** Waits until {@code thread} is parked on the transaction slot semaphore. */
    private static void awaitWaitingForSlot(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
        while (System.nanoTime() < deadline) {
            if (thread.getState() == Thread.State.WAITING && waitsOnSemaphore(thread)) {
                return;
            }
            Thread.sleep(10);
        }
        Assert.fail(thread.getName() + " never waited for a transaction slot: "
                + Arrays.toString(thread.getStackTrace()));
    }

    private static boolean waitsOnSemaphore(Thread thread) {
        for (StackTraceElement frame : thread.getStackTrace()) {
            if (frame.getClassName().equals(Semaphore.class.getName()) && frame.getMethodName().equals("acquire")) {
                return true;
            }
        }
        return false;
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

    /** The lines of {@code logged} from this thread: clients of other tests keep logging on theirs. */
    private static String ownLines(String logged) {
        String own = "[" + Thread.currentThread().getName() + "] ";
        StringBuilder out = new StringBuilder();
        for (String line : logged.split("\\R")) {
            if (line.startsWith(own)) {
                out.append(line).append('\n');
            }
        }
        return out.toString();
    }

    private static Properties stubProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "transactional-producer-pool-test");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        return props;
    }

    /** Takes a slot on its own thread and keeps it; records the slot or why it got none. */
    private static final class Taker extends Thread {
        private final TransactionalProducerPool pool;
        final AtomicInteger slot = new AtomicInteger(-1);
        final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        volatile boolean interruptedWhenFailed;

        Taker(TransactionalProducerPool pool, String name) {
            super(name);
            this.pool = pool;
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                slot.set(take(pool));
            } catch (RuntimeException e) {
                failure.set(e);
                interruptedWhenFailed = Thread.currentThread().isInterrupted();
            }
        }
    }

    /** Records the timeout it is closed with, and fails the close if asked to. */
    private static final class ClosingProducer extends KafkaProducer<byte[], byte[]> {
        private final RuntimeException closeFailure;
        volatile Duration closeTimeout;

        ClosingProducer(RuntimeException closeFailure) {
            super(stubProps(), new ByteArraySerializer(), new ByteArraySerializer());
            this.closeFailure = closeFailure;
        }

        @Override
        public void close(Duration timeout) {
            closeTimeout = timeout;
            super.close(Duration.ZERO);
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
