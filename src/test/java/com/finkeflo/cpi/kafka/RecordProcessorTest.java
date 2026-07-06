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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class RecordProcessorTest {

    private final List<DefaultCamelContext> contexts = new ArrayList<DefaultCamelContext>();

    @After
    public void closeContexts() {
        for (DefaultCamelContext ctx : contexts) {
            try {
                ctx.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
        contexts.clear();
    }

    /**
     * Creates a started endpoint for the given commit strategy. The backing
     * {@link DefaultCamelContext} is closed after the test via {@link #closeContexts()}.
     */
    private CpiKafkaPlusEndpoint endpoint(String commitStrategy) {
        try {
            CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
            DefaultCamelContext ctx = new DefaultCamelContext();
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            contexts.add(ctx);
            return (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=my-group"
                    + "&commitStrategy=" + commitStrategy);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // --- setBatchHeaders tests (replaces reflection-based tests from CpiKafkaPlusConsumerTest) ---

    @Test
    public void testSetBatchHeadersContainsRequiredHeaders() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=my-group"
                    + "&commitStrategy=BATCH_COMPLETE");
            RecordProcessor rp = createProcessor(endpoint);

            Message message = endpoint.createExchange().getIn();
            List<ConsumerRecord<byte[], byte[]>> batch = Arrays.asList(
                    rec("test-topic", 0, 5),
                    rec("test-topic", 0, 6));

            rp.setBatchHeaders(message, batch, 0, 0, 42);

            Assert.assertEquals(42, message.getHeader("CpiKafkaPlusPayloadSize"));
            Assert.assertEquals(2, message.getHeader("CpiKafkaPlusRecordCount"));
            Assert.assertEquals("my-group", message.getHeader("CpiKafkaPlusConsumerGroup"));
            Assert.assertEquals("BATCH_COMPLETE", message.getHeader("CpiKafkaPlusCommitStrategy"));
            Assert.assertEquals(5L, message.getHeader("CpiKafkaPlusFirstOffset"));
            Assert.assertEquals(6L, message.getHeader("CpiKafkaPlusLastOffset"));
            Assert.assertEquals(0, message.getHeader("CpiKafkaPlusPartition"));
            Assert.assertNull(message.getHeader("CpiKafkaPlusDlqCount"));
            Assert.assertNull(message.getHeader("CpiKafkaPlusSchemaValidationFailures"));
            ctx.stop();
        }
    }

    @Test
    public void testSetBatchHeadersIncludesDlqCountWhenDlqEnabled() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=my-group"
                    + "&dlqEnabled=true&dlqTopic=dead-letter");
            RecordProcessor rp = createProcessor(endpoint);

            Message message = endpoint.createExchange().getIn();
            List<ConsumerRecord<byte[], byte[]>> batch = Arrays.asList(rec("test-topic", 0, 10));

            rp.setBatchHeaders(message, batch, 0, 3, 0);

            Assert.assertEquals(3, message.getHeader("CpiKafkaPlusDlqCount"));
            ctx.stop();
        }
    }

    @Test
    public void testSetBatchHeadersIncludesSchemaValidationFailures() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            String schema = "{\"type\":\"object\"}";
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=my-group"
                    + "&jsonSchemaValidation=true&jsonSchema=" + java.net.URLEncoder.encode(schema, "UTF-8"));
            RecordProcessor rp = createProcessor(endpoint);

            Message message = endpoint.createExchange().getIn();
            List<ConsumerRecord<byte[], byte[]>> batch = Arrays.asList(rec("test-topic", 0, 20));

            rp.setBatchHeaders(message, batch, 2, 0, 0);

            Assert.assertEquals(2, message.getHeader("CpiKafkaPlusSchemaValidationFailures"));
            ctx.stop();
        }
    }

    @Test
    public void testSetSingleRecordHeadersContainsAllFields() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=single-group"
                    + "&commitStrategy=AUTO");
            RecordProcessor rp = createProcessor(endpoint);

            Message message = endpoint.createExchange().getIn();
            ConsumerRecord<byte[], byte[]> record = rec("test-topic", 1, 42);

            rp.setSingleRecordHeaders(message, record, "my-key", 100);

            Assert.assertEquals(100, message.getHeader("CpiKafkaPlusPayloadSize"));
            Assert.assertEquals("single-group", message.getHeader("CpiKafkaPlusConsumerGroup"));
            Assert.assertEquals("AUTO", message.getHeader("CpiKafkaPlusCommitStrategy"));
            Assert.assertEquals("test-topic", message.getHeader("CpiKafkaPlusTopic"));
            Assert.assertEquals(1, message.getHeader("CpiKafkaPlusPartition"));
            Assert.assertEquals(42L, message.getHeader("CpiKafkaPlusOffset"));
            Assert.assertEquals("my-key", message.getHeader("CpiKafkaPlusKey"));
            ctx.stop();
        }
    }

    // --- Deserialization tests ---

    @Test
    public void testDeserializeValueUtf8() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=test-group");
            RecordProcessor rp = createProcessor(endpoint);

            Assert.assertEquals("hello", rp.deserializeValue("t", "hello".getBytes(StandardCharsets.UTF_8)));
            Assert.assertNull(rp.deserializeValue("t", null));
            ctx.stop();
        }
    }

    @Test
    public void testDeserializeKeyUtf8() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", component);
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=test-group");
            RecordProcessor rp = createProcessor(endpoint);

            Assert.assertEquals("key-1", rp.deserializeKey("t", "key-1".getBytes(StandardCharsets.UTF_8)));
            Assert.assertNull(rp.deserializeKey("t", null));
            ctx.stop();
        }
    }

    // --- commitWithRebalanceHandling tests (issue #45) ---

    @Test
    public void testCommitWithRebalanceHandlingSwallowsCommitFailedException() {
        // Should not throw — CommitFailedException during rebalance is benign — and
        // must report false so the caller keeps the offsets pending for a re-commit.
        boolean committed = RecordProcessor.commitWithRebalanceHandling(
                () -> { throw new org.apache.kafka.clients.consumer.CommitFailedException(
                        "Offset commit cannot be completed since the consumer is undergoing a rebalance"); },
                "unit test");
        Assert.assertFalse("rebalance commit must report not-committed", committed);
    }

    @Test
    public void testCommitWithRebalanceHandlingSwallowsRebalanceInProgressException() {
        // Should not throw — RebalanceInProgressException is thrown by commitSync()
        // while a (cooperative) rebalance is in progress. It is benign: the offsets
        // are retained and re-committed after the rebalance completes. See issue #45/#49.
        boolean committed = RecordProcessor.commitWithRebalanceHandling(
                () -> { throw new org.apache.kafka.common.errors.RebalanceInProgressException(
                        "Offset commit cannot be completed since the consumer is undergoing "
                        + "a rebalance for auto partition assignment."); },
                "unit test");
        Assert.assertFalse("rebalance commit must report not-committed", committed);
    }

    @Test
    public void testCommitWithRebalanceHandlingPropagatesOtherExceptions() {
        try {
            RecordProcessor.commitWithRebalanceHandling(
                    () -> { throw new RuntimeException("network error"); },
                    "unit test");
            Assert.fail("expected RuntimeException to propagate");
        } catch (RuntimeException e) {
            Assert.assertEquals("network error", e.getMessage());
        }
    }

    @Test
    public void testCommitWithRebalanceHandlingRunsCommitOnHappyPath() {
        final boolean[] called = {false};
        boolean committed = RecordProcessor.commitWithRebalanceHandling(
                () -> called[0] = true, "unit test");
        Assert.assertTrue("commitOp must be invoked", called[0]);
        Assert.assertTrue("happy-path commit must report committed", committed);
    }

    // --- offset-tracking commit tests (issue #49: phantom lag / lost commits) ---

    @Test
    public void testCommitTrackedClearsPendingOnSuccess() {
        CpiKafkaPlusEndpoint endpoint = endpoint("BATCH_COMPLETE");
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        RecordProcessor processor = createProcessor(endpoint, tracker);
        TopicPartition tp = new TopicPartition("orders", 0);
        tracker.markProcessed(tp, 41);

        final Map<TopicPartition, OffsetAndMetadata> captured =
                new HashMap<TopicPartition, OffsetAndMetadata>();
        boolean committed = processor.commitTracked(captured::putAll, "unit test");

        Assert.assertTrue(committed);
        Assert.assertTrue("pending must be cleared after successful commit", tracker.isEmpty());
        Assert.assertEquals(42L, captured.get(tp).offset());
    }

    @Test
    public void testCommitTrackedRetainsPendingWhenRebalanceInProgress() {
        CpiKafkaPlusEndpoint endpoint = endpoint("BATCH_COMPLETE");
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        RecordProcessor processor = createProcessor(endpoint, tracker);
        TopicPartition tp = new TopicPartition("orders", 0);
        tracker.markProcessed(tp, 111); // pending -> 112

        boolean committed = processor.commitTracked(
                offsets -> { throw new org.apache.kafka.common.errors.RebalanceInProgressException("rebalance"); },
                "unit test");

        Assert.assertFalse("commit during rebalance must report not-committed", committed);
        Assert.assertFalse("offsets must be retained for re-commit", tracker.isEmpty());
        Assert.assertEquals(112L, tracker.snapshot().get(tp).offset());
    }

    @Test
    public void testRetainedOffsetIsReCommittedOnNextAttempt() {
        // Regression for issue #49: a commit swallowed during a rebalance must be
        // re-committed on the next attempt, not silently lost.
        CpiKafkaPlusEndpoint endpoint = endpoint("BATCH_COMPLETE");
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        RecordProcessor processor = createProcessor(endpoint, tracker);
        TopicPartition tp = new TopicPartition("orders", 0);
        tracker.markProcessed(tp, 111);

        // First attempt fails due to rebalance -> retained.
        processor.commitTracked(
                offsets -> { throw new org.apache.kafka.common.errors.RebalanceInProgressException("rebalance"); },
                "attempt 1");

        // Second attempt (rebalance completed) succeeds -> committed and cleared.
        final Map<TopicPartition, OffsetAndMetadata> captured =
                new HashMap<TopicPartition, OffsetAndMetadata>();
        boolean committed = processor.commitTracked(captured::putAll, "attempt 2");

        Assert.assertTrue(committed);
        Assert.assertTrue(tracker.isEmpty());
        Assert.assertEquals("re-commit must carry the processed offset", 112L, captured.get(tp).offset());
    }

    @Test
    public void testCommitTrackedForCommitsOnlyRequestedPartitions() {
        CpiKafkaPlusEndpoint endpoint = endpoint("BATCH_COMPLETE");
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        RecordProcessor processor = createProcessor(endpoint, tracker);
        TopicPartition tp0 = new TopicPartition("orders", 0);
        TopicPartition tp1 = new TopicPartition("orders", 1);
        tracker.markProcessed(tp0, 5);
        tracker.markProcessed(tp1, 9);

        final Map<TopicPartition, OffsetAndMetadata> captured =
                new HashMap<TopicPartition, OffsetAndMetadata>();
        boolean committed = processor.commitTrackedFor(
                captured::putAll, Arrays.asList(tp0), "revoke commit");

        Assert.assertTrue(committed);
        Assert.assertEquals("only revoked partition committed", 1, captured.size());
        Assert.assertEquals(6L, captured.get(tp0).offset());
        Assert.assertFalse("non-revoked partition stays pending", tracker.isEmpty());
        Assert.assertEquals(10L, tracker.snapshot().get(tp1).offset());
    }

    @Test
    public void testDropLostRemovesPendingWithoutCommitting() {
        CpiKafkaPlusEndpoint endpoint = endpoint("BATCH_COMPLETE");
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        RecordProcessor processor = createProcessor(endpoint, tracker);
        TopicPartition tp = new TopicPartition("orders", 0);
        tracker.markProcessed(tp, 3);

        processor.dropLost(Arrays.asList(tp));

        Assert.assertTrue("lost partition offsets must be dropped", tracker.isEmpty());
    }

    // --- commit-or-drop on revoke (ownership boundary; prevents stale-offset rewind) ---

    @Test
    public void testCommitOnRevokeCommitsPendingOffsetsBeforeDropping() {
        CpiKafkaPlusEndpoint endpoint = endpoint("BATCH_COMPLETE");
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        RecordProcessor processor = createProcessor(endpoint, tracker);
        TopicPartition tp = new TopicPartition("orders", 0);
        tracker.markProcessed(tp, 111); // pending -> 112

        final Map<TopicPartition, OffsetAndMetadata> captured =
                new HashMap<TopicPartition, OffsetAndMetadata>();
        processor.commitOnRevoke(captured::putAll, Arrays.asList(tp));

        Assert.assertEquals("revoke must commit the pending offset while still owned",
                112L, captured.get(tp).offset());
        Assert.assertTrue("committed offsets must leave the tracker", tracker.isEmpty());
    }

    @Test
    public void testCommitOnRevokeDropsPendingWhenCommitFailsWithRebalance() {
        // A revoked partition may never be committed by this consumer again: the new
        // owner advances independently, and a later stale commit would rewind the
        // group offset. After the (failed) revoke commit the entry must be dropped.
        CpiKafkaPlusEndpoint endpoint = endpoint("BATCH_COMPLETE");
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        RecordProcessor processor = createProcessor(endpoint, tracker);
        TopicPartition tp = new TopicPartition("orders", 0);
        tracker.markProcessed(tp, 111);

        processor.commitOnRevoke(
                offsets -> { throw new org.apache.kafka.common.errors.RebalanceInProgressException("rebalance"); },
                Arrays.asList(tp));

        Assert.assertTrue("revoked partition must not stay pending after failed commit",
                tracker.isEmpty());
    }

    @Test
    public void testCommitOnRevokeDropsOnlyRevokedPartitions() {
        CpiKafkaPlusEndpoint endpoint = endpoint("BATCH_COMPLETE");
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        RecordProcessor processor = createProcessor(endpoint, tracker);
        TopicPartition revoked = new TopicPartition("orders", 0);
        TopicPartition kept = new TopicPartition("orders", 1);
        tracker.markProcessed(revoked, 5);
        tracker.markProcessed(kept, 9);

        processor.commitOnRevoke(
                offsets -> { throw new org.apache.kafka.clients.consumer.CommitFailedException(); },
                Arrays.asList(revoked));

        Assert.assertNull("revoked partition must be dropped",
                tracker.snapshot().get(revoked));
        Assert.assertEquals("still-owned partition must stay pending",
                10L, tracker.snapshot().get(kept).offset());
    }

    @Test
    public void testCommitOnRevokeSwallowsKafkaExceptionAndStillDrops() {
        // commitSync inside the rebalance listener may throw e.g. TimeoutException
        // (broker unreachable). It must not escape the listener — Kafka would wrap it
        // into a KafkaException out of poll(), turning a rebalance into a reported
        // connection error — and the revoked entries must still be dropped.
        CpiKafkaPlusEndpoint endpoint = endpoint("BATCH_COMPLETE");
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        RecordProcessor processor = createProcessor(endpoint, tracker);
        TopicPartition tp = new TopicPartition("orders", 0);
        tracker.markProcessed(tp, 7);

        processor.commitOnRevoke(
                offsets -> { throw new org.apache.kafka.common.errors.TimeoutException("broker unreachable"); },
                Arrays.asList(tp));

        Assert.assertTrue("revoked partition must be dropped despite commit timeout",
                tracker.isEmpty());
    }

    @Test
    public void testCommitOnRevokeIgnoresNullPartitions() {
        CpiKafkaPlusEndpoint endpoint = endpoint("BATCH_COMPLETE");
        OffsetCommitTracker tracker = new OffsetCommitTracker();
        RecordProcessor processor = createProcessor(endpoint, tracker);
        TopicPartition tp = new TopicPartition("orders", 0);
        tracker.markProcessed(tp, 3);

        processor.commitOnRevoke(offsets -> Assert.fail("commit must not run for null partitions"),
                null);

        Assert.assertFalse("pending offsets must be untouched", tracker.isEmpty());
    }

    // --- Helpers ---

    private RecordProcessor createProcessor(CpiKafkaPlusEndpoint endpoint) {
        return createProcessor(endpoint, new OffsetCommitTracker());
    }

    private RecordProcessor createProcessor(CpiKafkaPlusEndpoint endpoint,
                                            OffsetCommitTracker tracker) {
        AdapterTracingHelper tracingHelper = new AdapterTracingHelper(endpoint);
        return new RecordProcessor(endpoint, tracingHelper, null, null, null,
                new RecordProcessor.ConsumerCallback() {
                    @Override
                    public void processExchange(Exchange exchange) throws Exception {
                        if (exchange.getException() != null) {
                            throw exchange.getException();
                        }
                    }

                    @Override
                    public void handleException(String message, Exchange exchange, Exception e) {
                        // no-op for unit tests
                    }

                    @Override
                    public Exchange createExchange() {
                        return endpoint.createExchange();
                    }
                }, tracker);
    }

    private static ConsumerRecord<byte[], byte[]> rec(String topic, int partition, long offset) {
        return new ConsumerRecord<>(topic, partition, offset, null, "value".getBytes());
    }
}
