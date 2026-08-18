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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.Assert;
import org.junit.Test;

/**
 * Proves that a dead-letter send survives a producer that has become permanently unusable.
 *
 * <p>Why this matters more than it looks: the dead-letter send is what allows the consumer to commit
 * the offset of a failed record. If it throws, nothing is committed, the same record is polled
 * again, and the partition never advances — so a wedged dead-letter producer is not a lost message,
 * it is a stopped consumer. That is exactly what happened in production, where no dead-letter write
 * occurred for over an hour until the iFlow was redeployed by hand.
 *
 * <p>Each test therefore asserts an exact producer-creation count, not just the final outcome: the
 * cure for a stalled partition must not be a rebuild loop.
 */
public class DlqProducerSelfHealingTest {

    private static final long SHORT_BUDGET_MS = 500L;

    private static ConsumerRecord<byte[], byte[]> failedRecord() {
        return new ConsumerRecord<byte[], byte[]>("orders", 0, 42L, "k".getBytes(), "v".getBytes());
    }

    /** The real KAFKA-10902 signature: the bare {@code Object.wait()} message from a Kafka frame. */
    private static RuntimeException monitorFault() {
        IllegalMonitorStateException imse =
                new IllegalMonitorStateException("current thread is not owner");
        imse.setStackTrace(new StackTraceElement[] {
            new StackTraceElement("org.apache.kafka.common.utils.SystemTime",
                    "waitObject", "SystemTime.java", 62),
            new StackTraceElement("org.apache.kafka.clients.producer.internals.ProducerMetadata",
                    "awaitUpdate", "ProducerMetadata.java", 119),
        });
        return imse;
    }

    @Test
    public void anUnusableProducerIsRebuiltAndTheRecordStillReachesTheDlq() {
        ScriptedProducer wedged = new ScriptedProducer();
        wedged.failWith(new AuthenticationException("client is no longer authenticated"));
        RecordingFactory factory = new RecordingFactory();

        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", wedged,
                ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"), factory);

        helper.sendToDlq(failedRecord(), new RuntimeException("boom"), 0);

        Assert.assertEquals("the unusable producer must be replaced exactly once",
                1, factory.creations.get());
        Assert.assertEquals("the record must be retried on the replacement, not dropped",
                1, factory.last().accepted.size());
        Assert.assertTrue("the wedged producer must be closed, not leaked", wedged.closed);
    }

    @Test
    public void theKafka10902MonitorFaultIsRecoveredByARebuild() {
        ScriptedProducer wedged = new ScriptedProducer();
        wedged.failWith(monitorFault());
        RecordingFactory factory = new RecordingFactory();

        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", wedged,
                ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"), factory);

        helper.sendToDlq(failedRecord(), new RuntimeException("boom"), 0);

        Assert.assertEquals(1, factory.creations.get());
        Assert.assertEquals(1, factory.last().accepted.size());
    }

    @Test
    public void aPoisonPillSendIsAlsoRecovered() {
        ScriptedProducer wedged = new ScriptedProducer();
        wedged.failWith(new AuthenticationException("client is no longer authenticated"));
        RecordingFactory factory = new RecordingFactory();

        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", wedged,
                ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"), factory);

        helper.sendDeserializationFailure(new TopicPartition("orders", 0), 42L,
                null, "bad".getBytes(), null, -1L, new RuntimeException("bad bytes"));

        Assert.assertEquals(1, factory.creations.get());
        Assert.assertEquals(1, factory.last().accepted.size());
    }

    /**
     * A record the broker will never accept must not cost a rebuild. Rebuilding on a data error
     * would mean a single oversized message could recycle the client on every redelivery.
     */
    @Test
    public void aDataErrorDoesNotRebuildBecauseAFreshProducerCannotFixIt() {
        ScriptedProducer producer = new ScriptedProducer();
        producer.failWith(new RecordTooLargeException("record is 5 MB"));
        RecordingFactory factory = new RecordingFactory();

        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", producer,
                ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"), factory);

        try {
            helper.sendToDlq(failedRecord(), new RuntimeException("boom"), 0);
            Assert.fail("Expected the data error to be reported, not retried");
        } catch (RuntimeException expected) {
            Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("orders-dlq"));
        }
        Assert.assertEquals("a data error must not trigger a rebuild", 0, factory.creations.get());
    }

    /**
     * Kafka's own retriable failures are handled by the client and by the next poll of the
     * uncommitted record, so they must not consume a rebuild either.
     */
    @Test
    public void aRetriableFailureDoesNotRebuild() {
        ScriptedProducer producer = new ScriptedProducer();
        producer.failWith(new TimeoutException("metadata not available"));
        RecordingFactory factory = new RecordingFactory();

        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", producer,
                ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"), factory);

        try {
            helper.sendToDlq(failedRecord(), new RuntimeException("boom"), 0);
            Assert.fail("Expected the retriable failure to be reported");
        } catch (RuntimeException expected) {
            Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("orders-dlq"));
        }
        Assert.assertEquals(0, factory.creations.get());
    }

    /**
     * The stalled-partition scenario: the same record is redelivered over and over. The backoff has
     * to hold, or the mitigation becomes its own outage.
     */
    @Test
    public void aRedeliveredRecordCannotTriggerARebuildStorm() {
        ScriptedProducer wedged = new ScriptedProducer();
        wedged.failWith(new AuthenticationException("client is no longer authenticated"));
        // Every replacement is wedged too, which is the worst case: nothing ever succeeds.
        RecordingFactory factory = new RecordingFactory(
                new AuthenticationException("client is no longer authenticated"));

        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", wedged,
                ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"), factory);

        for (int redelivery = 0; redelivery < 5; redelivery++) {
            try {
                helper.sendToDlq(failedRecord(), new RuntimeException("boom"), 0);
                Assert.fail("Expected every attempt to fail");
            } catch (RuntimeException expected) {
                Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("orders-dlq"));
            }
        }

        Assert.assertEquals("five redeliveries inside the backoff window must rebuild once",
                1, factory.creations.get());
    }

    /**
     * A rebuild that cannot even construct a client must not leave the helper holding nothing: the
     * next delivery has to be able to try again, and it must fail with a named error rather than a
     * NullPointerException.
     */
    @Test
    public void aFailedRebuildLeavesTheHelperUsableForTheNextDelivery() {
        ScriptedProducer wedged = new ScriptedProducer();
        wedged.failWith(new AuthenticationException("client is no longer authenticated"));
        RecordingFactory factory = new RecordingFactory();
        factory.failNextCreation(new IllegalStateException("broker unreachable"));

        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", wedged,
                ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"), factory);

        try {
            helper.sendToDlq(failedRecord(), new RuntimeException("boom"), 0);
            Assert.fail("Expected the failed rebuild to surface");
        } catch (RuntimeException expected) {
            Assert.assertTrue("the rebuild failure must be attached to the original failure",
                    KafkaErrorHelper.describeChain(expected).contains("AuthenticationException"));
        }

        // The next delivery re-creates lazily and succeeds, without waiting out the backoff.
        helper.sendToDlq(failedRecord(), new RuntimeException("boom"), 0);
        Assert.assertEquals("one failed creation plus one lazy creation", 2, factory.creations.get());
        Assert.assertEquals(1, factory.last().accepted.size());
    }

    @Test
    public void sendingAfterCloseFailsWithANamedErrorInsteadOfRecreatingAProducer() throws Exception {
        ScriptedProducer producer = new ScriptedProducer();
        RecordingFactory factory = new RecordingFactory();

        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", producer,
                ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"), factory);
        helper.close();

        try {
            helper.sendToDlq(failedRecord(), new RuntimeException("boom"), 0);
            Assert.fail("Expected a send after close to fail");
        } catch (RuntimeException expected) {
            Assert.assertTrue(KafkaErrorHelper.describeChain(expected),
                    KafkaErrorHelper.describeChain(expected).contains("is closed"));
        }
        Assert.assertEquals("a closed helper must not resurrect itself", 0, factory.creations.get());
        Assert.assertTrue(producer.closed);
    }

    @Test
    public void closeIsIdempotent() throws Exception {
        ScriptedProducer producer = new ScriptedProducer();
        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", producer,
                ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"), new RecordingFactory());

        helper.close();
        helper.close();

        Assert.assertEquals("the producer must be closed exactly once", 1, producer.closeCount);
    }

    /**
     * The gap a pure classification rule would leave open: Kafka calls a failure retriable, the
     * adapter believes it, and the partition never moves again. After enough consecutive failures
     * the prediction has been falsified by events, so a rebuild is attempted regardless.
     */
    @Test
    public void aRetriableFailureThatNeverRecoversEventuallyForcesARebuild() {
        ScriptedProducer producer = new ScriptedProducer();
        producer.failWith(new TimeoutException("topic not present in metadata"));
        RecordingFactory factory = new RecordingFactory();

        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", producer,
                ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"), factory);

        for (int i = 1; i < DlqProducerHelper.REBUILD_ESCALATION_THRESHOLD; i++) {
            try {
                helper.sendToDlq(failedRecord(), new RuntimeException("boom"), 0);
                Assert.fail("Expected redelivery " + i + " to fail without a rebuild");
            } catch (RuntimeException expected) {
                Assert.assertEquals("no rebuild before the threshold is reached",
                        0, factory.creations.get());
            }
        }

        // The threshold delivery escalates, rebuilds, and the record finally lands.
        helper.sendToDlq(failedRecord(), new RuntimeException("boom"), 0);

        Assert.assertEquals(1, factory.creations.get());
        Assert.assertEquals(1, factory.last().accepted.size());
    }

    /** A record the broker rejects is rejected by a new client too, so escalation must not apply. */
    @Test
    public void aRepeatedDataErrorNeverEscalatesIntoARebuild() {
        ScriptedProducer producer = new ScriptedProducer();
        producer.failWith(new RecordTooLargeException("record is 5 MB"));
        RecordingFactory factory = new RecordingFactory();

        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", producer,
                ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT"), factory);

        for (int i = 0; i < DlqProducerHelper.REBUILD_ESCALATION_THRESHOLD + 2; i++) {
            try {
                helper.sendToDlq(failedRecord(), new RuntimeException("boom"), 0);
                Assert.fail("Expected the data error to be reported");
            } catch (RuntimeException expected) {
                // expected
            }
        }

        Assert.assertEquals("a data error must never escalate into a rebuild",
                0, factory.creations.get());
    }

    /** Builds replacements on demand and remembers every one of them. */
    private static final class RecordingFactory implements DlqProducerHelper.ProducerFactory {
        private final AtomicInteger creations = new AtomicInteger();
        private final List<ScriptedProducer> built =
                Collections.synchronizedList(new ArrayList<ScriptedProducer>());
        private final RuntimeException replacementFailure;
        private Exception nextCreationFailure;

        RecordingFactory() {
            this(null);
        }

        RecordingFactory(RuntimeException replacementFailure) {
            this.replacementFailure = replacementFailure;
        }

        void failNextCreation(Exception e) {
            this.nextCreationFailure = e;
        }

        ScriptedProducer last() {
            return built.get(built.size() - 1);
        }

        @Override
        public Producer<byte[], byte[]> create() throws Exception {
            creations.incrementAndGet();
            if (nextCreationFailure != null) {
                Exception e = nextCreationFailure;
                nextCreationFailure = null;
                throw e;
            }
            ScriptedProducer replacement = new ScriptedProducer();
            if (replacementFailure != null) {
                replacement.failWith(replacementFailure);
            }
            built.add(replacement);
            return replacement;
        }
    }

    /** A producer that either accepts a record or throws a scripted failure from {@code send}. */
    private static final class ScriptedProducer implements Producer<byte[], byte[]> {
        private final List<ProducerRecord<byte[], byte[]>> accepted =
                Collections.synchronizedList(new ArrayList<ProducerRecord<byte[], byte[]>>());
        private volatile RuntimeException failure;
        private volatile boolean closed;
        private volatile int closeCount;

        void failWith(RuntimeException e) {
            this.failure = e;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            RuntimeException scripted = failure;
            if (scripted != null) {
                throw scripted;
            }
            accepted.add(record);
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            future.complete(new RecordMetadata(
                    new TopicPartition(record.topic(), 0), 0L, 0, 0L, 0, 0));
            return future;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            return send(record);
        }

        @Override
        public void close() {
            close(Duration.ZERO);
        }

        @Override
        public void close(Duration timeout) {
            closed = true;
            closeCount++;
        }

        @Override
        public void initTransactions() {
        }

        @Override
        public void beginTransaction() throws KafkaException {
        }

        @Override
        public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                             ConsumerGroupMetadata groupMetadata) throws KafkaException {
        }

        @Override
        public void commitTransaction() throws KafkaException {
        }

        @Override
        public void abortTransaction() throws KafkaException {
        }

        @Override
        public void registerMetricForSubscription(KafkaMetric metric) {
        }

        @Override
        public void unregisterMetricFromSubscription(KafkaMetric metric) {
        }

        @Override
        public void flush() {
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return Collections.emptyList();
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return Collections.emptyMap();
        }

        @Override
        public Uuid clientInstanceId(Duration timeout) {
            return Uuid.randomUuid();
        }
    }
}
