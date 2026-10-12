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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * What the record processor commits and rewinds when records fail (issues #187, #172, #173):
 * no commit may pass a record that was neither processed, dead-lettered nor deliberately skipped.
 */
public class RecordProcessorProgressTest {

    private static final String TOPIC = "orders";
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);
    private static final String SCHEMA =
            "{\"type\":\"object\",\"required\":[\"id\"],\"properties\":{\"id\":{\"type\":\"string\"}}}";

    private final List<DefaultCamelContext> contexts = new ArrayList<>();
    /** Offsets the route was invoked for, in order (partition-qualified as "p:offset"). */
    private final List<String> invoked = new ArrayList<>();
    private final Set<String> failing = new HashSet<>();
    /** Like {@link #failing}, but the failure has a transient cause (the backend is unreachable). */
    private final Set<String> failingTransiently = new HashSet<>();
    /** Fails transiently this many times for the id, then permanently on every further call. */
    private final Map<String, Integer> transientThenPermanent = new HashMap<>();
    /** Like {@link #failing}, but the exception's cause chain is cyclic: A → B → A (#241). */
    private final Set<String> failingCyclically = new HashSet<>();
    /** The body of each route call, in the order of {@link #invoked}. */
    private final List<String> bodies = new ArrayList<>();
    /** The headers of each route call, in the order of {@link #invoked}. */
    private final List<Map<String, Object>> headers = new ArrayList<>();
    private final AtomicBoolean stopAfterFirstRecord = new AtomicBoolean();
    private final OffsetCommitTracker tracker = new OffsetCommitTracker();

    @After
    public void tearDown() throws Exception {
        Thread.interrupted(); // never leak an interrupt into the next test
        for (DefaultCamelContext ctx : contexts) {
            ctx.close();
        }
    }

    // --- option C: a failed DLQ write must not lose the record ---

    @Test
    public void dlqWriteFailureLeavesTheRecordUncommittedAndRewindsItsPartition() throws Exception {
        failing.add("0:1");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        RecordProcessor processor = processor(endpoint(true, null), failingDlq(), null);

        PollProgress progress = processSingle(processor, consumer);

        Assert.assertEquals("the record after the blocked one must not be processed",
                Arrays.asList("0:0", "0:1"), invoked);
        Assert.assertEquals(1L, committed(consumer, P0));
        Assert.assertEquals("rewound to the record whose DLQ write failed", 1L, consumer.position(P0));
        Assert.assertTrue(progress.isBlocked(P0));
    }

    @Test
    public void blockedPartitionDoesNotHoldBackOtherPartitions() throws Exception {
        failing.add("0:0");
        Map<TopicPartition, List<String>> values = new HashMap<>();
        values.put(P0, Arrays.asList("a", "b"));
        values.put(P1, Arrays.asList("x", "y"));
        MockConsumer<byte[], byte[]> consumer = consumerWith(values);
        RecordProcessor processor = processor(endpoint(true, null), failingDlq(), null);

        processSingle(processor, consumer);

        Assert.assertEquals(2L, committed(consumer, P1));
        Assert.assertEquals(2L, consumer.position(P1));
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void successfulDlqWriteResolvesTheRecord() throws Exception {
        failing.add("0:1");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint(true, null), dlqHelper(dlq), null);

        processSingle(processor, consumer);

        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals(3L, committed(consumer, P0));
        Assert.assertEquals(3L, consumer.position(P0));
    }

    @Test
    public void failedDlqWriteIsTracedAsSendFailed() throws Exception {
        failing.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        CpiKafkaPlusEndpoint endpoint = endpoint(true, null);
        RecordingTracingHelper tracing = new RecordingTracingHelper(endpoint);
        RecordProcessor processor = processor(endpoint, failingDlq(), null, tracing);

        processSingle(processor, consumer);

        Assert.assertEquals(1, tracing.traced.size());
        Assert.assertEquals("SEND_FAILED", tracing.traced.get(0).get("dlqOutcome"));
        Assert.assertEquals("orders-dlq", tracing.traced.get(0).get("dlqTopic"));
        Assert.assertEquals("PERMANENT", tracing.traced.get(0).get("errorType"));
        Assert.assertEquals("0", tracing.traced.get(0).get("retryAttempts"));
    }

    @Test
    public void withoutDlqTheFailedRecordIsRetriedByDefault() throws Exception {
        failing.add("0:1");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        RecordProcessor processor = processor(endpoint(false, null), null, null);

        PollProgress progress = processSingle(processor, consumer);

        Assert.assertEquals(Arrays.asList("0:0", "0:1"), invoked);
        Assert.assertEquals(1L, committed(consumer, P0));
        Assert.assertEquals(1L, consumer.position(P0));
        Assert.assertTrue(progress.isBlocked(P0));
    }

    @Test
    public void withoutDlqSkipFailedMessagesSkipsTheFailedRecord() throws Exception {
        failing.add("0:1");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        RecordProcessor processor = processor(skip(endpoint(false, null)), null, null);

        processSingle(processor, consumer);

        Assert.assertEquals(Arrays.asList("0:0", "0:1", "0:2"), invoked);
        Assert.assertEquals(3L, committed(consumer, P0));
    }

    @Test
    public void withoutDlqAFailedRecordIsNotRetriedWithinThePoll() throws Exception {
        // Max Retries before DLQ only applies with a DLQ. Without one, the blocked partition retries
        // the record on a later poll, after a backoff.
        failingTransiently.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        CpiKafkaPlusEndpoint endpoint = endpoint(false, null);
        endpoint.setDlqMaxRetries(3);
        RecordProcessor processor = processor(endpoint, null, null);

        PollProgress progress = processSingle(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:0"), invoked);
        Assert.assertTrue(progress.isBlocked(P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void withoutDlqATransientFailureIsTracedWithoutADlqOutcome() throws Exception {
        failingTransiently.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        CpiKafkaPlusEndpoint endpoint = endpoint(false, null);
        endpoint.setDlqMaxRetries(3);
        RecordingTracingHelper tracing = new RecordingTracingHelper(endpoint);
        RecordProcessor processor = processor(endpoint, null, null, tracing);

        PollProgress progress = processSingle(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:0"), invoked);
        Assert.assertEquals(1, tracing.traced.size());
        Assert.assertEquals("TRANSIENT", tracing.traced.get(0).get("errorType"));
        Assert.assertEquals("0", tracing.traced.get(0).get("retryAttempts"));
        Assert.assertNull(tracing.traced.get(0).get("dlqOutcome"));
        Assert.assertTrue(progress.isBlocked(P0));
    }

    // --- #172: a commit failure is not a processing failure ---

    @Test
    public void wakeupDuringCommitStopsWithoutRerunningTheRoute() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        AtomicInteger commits = new AtomicInteger();
        consumer = failingCommits(consumer, commits, new WakeupException());
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint(true, null), dlqHelper(dlq), null);

        PollProgress progress = processSingle(processor, consumer);

        Assert.assertEquals("the successful route must not run again", Collections.singletonList("0:0"), invoked);
        Assert.assertTrue("a successful record must not be dead-lettered", dlq.history().isEmpty());
        Assert.assertTrue(progress.isStopped());
        Assert.assertEquals("the processed offset stays pending for the revoke commit",
                Collections.singletonMap(P0, new OffsetAndMetadata(1L)), tracker.snapshot());
        Assert.assertEquals(1L, consumer.position(P0));
    }

    @Test
    public void commitTimeoutKeepsTheOffsetPendingAndContinues() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        AtomicInteger commits = new AtomicInteger();
        consumer = failingCommits(consumer, commits, new TimeoutException("commit timed out"));
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint(true, null), dlqHelper(dlq), null);

        processSingle(processor, consumer);

        Assert.assertEquals(Arrays.asList("0:0", "0:1"), invoked);
        Assert.assertTrue(dlq.history().isEmpty());
        Assert.assertEquals("the second commit carries the first offset along", 2L, committed(consumer, P0));
        Assert.assertTrue(tracker.isEmpty());
    }

    @Test
    public void interruptedRetryBackoffDoesNotDeadLetter() throws Exception {
        failing.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        CpiKafkaPlusEndpoint endpoint = endpoint(true, null);
        endpoint.setDlqMaxRetries(2);
        endpoint.setRetryDelaySeconds(1);
        endpoint.setRetryOnlyTransientErrors(false);
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), null);

        Thread.currentThread().interrupt(); // as doStop() does to a sleeping poll thread
        PollProgress progress = processSingle(processor, consumer);
        Thread.interrupted();

        Assert.assertTrue("an interrupted retry is a shutdown, not a poison record", dlq.history().isEmpty());
        Assert.assertTrue(progress.isStopped());
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void stopRequestBetweenRetriesDoesNotDeadLetter() throws Exception {
        stopAfterFirstRecord.set(true);
        failingTransiently.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        CpiKafkaPlusEndpoint endpoint = endpoint(true, null);
        endpoint.setDlqMaxRetries(2);
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), null);

        PollProgress progress = processSingle(processor, consumer);

        Assert.assertEquals("no further attempt once the consumer stops",
                Collections.singletonList("0:0"), invoked);
        Assert.assertTrue("a stop is a shutdown, not a poison record", dlq.history().isEmpty());
        Assert.assertTrue(progress.isStopped());
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    // --- retryOnlyTransientErrors (on by default): only a transient failure is retried ---

    @Test
    public void transientFailureIsRetriedBeforeItIsDeadLettered() throws Exception {
        failingTransiently.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        CpiKafkaPlusEndpoint endpoint = endpoint(true, null);
        endpoint.setDlqMaxRetries(2);
        Assert.assertTrue("precondition: on by default", endpoint.isRetryOnlyTransientErrors());
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), null);

        processSingle(processor, consumer);

        Assert.assertEquals(Arrays.asList("0:0", "0:0", "0:0", "0:1"), invoked);
        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals("TRANSIENT", header(dlq.history().get(0), "CpiKafkaPlusDlqErrorType"));
        Assert.assertEquals("2", header(dlq.history().get(0), "CpiKafkaPlusDlqRetryCount"));
        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void permanentFailureSkipsTheRetriesAndIsDeadLetteredAtOnce() throws Exception {
        failing.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        CpiKafkaPlusEndpoint endpoint = endpoint(true, null);
        endpoint.setDlqMaxRetries(2);
        Assert.assertTrue("precondition: on by default", endpoint.isRetryOnlyTransientErrors());
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), null);

        processSingle(processor, consumer);

        Assert.assertEquals(Arrays.asList("0:0", "0:1"), invoked);
        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals("PERMANENT", header(dlq.history().get(0), "CpiKafkaPlusDlqErrorType"));
        Assert.assertEquals("0", header(dlq.history().get(0), "CpiKafkaPlusDlqRetryCount"));
        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void permanentFailureAfterATransientOneSkipsTheRetriesLeft() throws Exception {
        transientThenPermanent.put("0:0", 1);
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        CpiKafkaPlusEndpoint endpoint = endpoint(true, null);
        endpoint.setDlqMaxRetries(3);
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordingTracingHelper tracing = new RecordingTracingHelper(endpoint);
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), null, tracing);

        processSingle(processor, consumer);

        Assert.assertEquals("the two retries left are skipped", Arrays.asList("0:0", "0:0", "0:1"), invoked);
        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals("PERMANENT", header(dlq.history().get(0), "CpiKafkaPlusDlqErrorType"));
        // Pinned as it is: the retry that failed permanently is not counted, so one retry ran but the
        // count says none.
        Assert.assertEquals("0", header(dlq.history().get(0), "CpiKafkaPlusDlqRetryCount"));
        Assert.assertEquals("PERMANENT", tracing.traced.get(0).get("errorType"));
        Assert.assertEquals("0", tracing.traced.get(0).get("retryAttempts"));
        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void withRetryOnlyTransientErrorsOffEveryFailureIsRetriedAndNotClassified() throws Exception {
        failing.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        CpiKafkaPlusEndpoint endpoint = endpoint(true, null);
        endpoint.setDlqMaxRetries(2);
        endpoint.setRetryOnlyTransientErrors(false);
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordingTracingHelper tracing = new RecordingTracingHelper(endpoint);
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), null, tracing);

        processSingle(processor, consumer);

        Assert.assertEquals("a permanent failure is retried as well",
                Arrays.asList("0:0", "0:0", "0:0", "0:1"), invoked);
        Assert.assertEquals(1, dlq.history().size());
        Assert.assertNull("an unclassified failure has no error type",
                header(dlq.history().get(0), "CpiKafkaPlusDlqErrorType"));
        Assert.assertEquals("2", header(dlq.history().get(0), "CpiKafkaPlusDlqRetryCount"));
        Assert.assertEquals(1, tracing.traced.size());
        Assert.assertEquals("UNKNOWN", tracing.traced.get(0).get("errorType"));
        Assert.assertEquals("2", tracing.traced.get(0).get("retryAttempts"));
        Assert.assertEquals("MOVED", tracing.traced.get(0).get("dlqOutcome"));
        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void errorHandlingIsCaseInsensitive() throws Exception {
        failing.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        CpiKafkaPlusEndpoint endpoint = endpoint(false, null);
        endpoint.setErrorHandling("skip");
        RecordProcessor processor = processor(endpoint, null, null);

        processSingle(processor, consumer);

        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void stopRequestRewindsTheRemainingRecords() throws Exception {
        stopAfterFirstRecord.set(true);
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        RecordProcessor processor = processor(endpoint(false, null), null, null);

        processSingle(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:0"), invoked);
        Assert.assertEquals(1L, committed(consumer, P0));
        Assert.assertEquals(1L, consumer.position(P0));
    }

    // --- deserialization failures no longer escape the poll ---

    @Test
    public void deserializationFailureWithoutDlqIsRetriedByDefault() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "BAD", "b");
        CpiKafkaPlusEndpoint endpoint = avroEndpoint(false);
        RecordProcessor processor = processor(endpoint, null, failingAvro(endpoint));

        processSingle(processor, consumer);

        Assert.assertTrue(invoked.isEmpty());
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void deserializationFailureWithoutDlqSkipsOnlyThatRecordWhenSkipping() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "BAD", "b");
        CpiKafkaPlusEndpoint endpoint = skip(avroEndpoint(false));
        RecordProcessor processor = processor(endpoint, null, failingAvro(endpoint));

        processSingle(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:1"), invoked);
        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void deserializationFailureWithDlqIsDeadLetteredAtOnce() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "BAD", "b");
        CpiKafkaPlusEndpoint endpoint = avroEndpoint(true);
        endpoint.setDlqMaxRetries(2);
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), failingAvro(endpoint));

        processSingle(processor, consumer);

        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals("DESERIALIZATION", header(dlq.history().get(0), "CpiKafkaPlusDlqErrorType"));
        Assert.assertEquals(Collections.singletonList("0:1"), invoked);
        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void deserializationFailureWithFailingDlqBlocks() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "BAD", "b");
        CpiKafkaPlusEndpoint endpoint = avroEndpoint(true);
        RecordProcessor processor = processor(endpoint, failingDlq(), failingAvro(endpoint));

        processSingle(processor, consumer);

        Assert.assertTrue(invoked.isEmpty());
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    // --- V17(a): JSON Schema failures and the DLQ ---

    @Test
    public void schemaInvalidRecordWithFailingDlqBlocks() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "{}", "{\"id\":\"1\"}");
        RecordProcessor processor = processor(endpoint(true, SCHEMA), failingDlq(), null);

        processSingle(processor, consumer);

        Assert.assertTrue(invoked.isEmpty());
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void schemaInvalidRecordWithoutDlqIsDroppedEvenWhenRetrying() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "{}", "{\"id\":\"1\"}");
        RecordProcessor processor = processor(endpoint(false, SCHEMA), null, null);

        processSingle(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:1"), invoked);
        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void schemaInvalidRecordWithDlqIsDeadLetteredWithoutRetries() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "{}", "{\"id\":\"1\"}");
        CpiKafkaPlusEndpoint endpoint = endpoint(true, SCHEMA);
        endpoint.setDlqMaxRetries(2);
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), null);

        processSingle(processor, consumer);

        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals("0", header(dlq.history().get(0), "CpiKafkaPlusDlqRetryCount"));
        Assert.assertEquals(Collections.singletonList("0:1"), invoked);
        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void schemaInvalidRecordInABatchWithoutDlqIsDroppedEvenWhenRetrying() throws Exception {
        MockConsumer<byte[], byte[]> consumer =
                consumerWith(P0, "{\"id\":\"a\"}", "{}", "{\"id\":\"c\"}");
        RecordProcessor processor = processor(batchEndpoint(false, SCHEMA), null, null);

        PollProgress progress = processBatch(processor, consumer);

        Assert.assertEquals("the batch runs without the invalid record",
                Collections.singletonList("0:0-2"), invoked);
        Assert.assertFalse(bodies.get(0).contains("\"offset\":1"));
        Assert.assertFalse(progress.isBlocked(P0));
        Assert.assertEquals(3L, committed(consumer, P0));
    }

    // --- commitStrategy=AUTO: the adapter commits nothing, and a resolved record is not rewound ---

    @Test
    public void withAutoCommitADeadLetteredRecordIsResolvedButNotCommitted() throws Exception {
        failing.add("0:1");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint(true, null), dlqHelper(dlq), null);

        PollProgress progress = processSingle(processor, consumer, false);

        Assert.assertEquals(1, dlq.history().size());
        assertResolvedButNotCommitted(consumer, progress, 3L);
    }

    @Test
    public void withAutoCommitASkippedRecordIsResolvedButNotCommitted() throws Exception {
        failing.add("0:1");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        RecordProcessor processor = processor(skip(endpoint(false, null)), null, null);

        PollProgress progress = processSingle(processor, consumer, false);

        Assert.assertEquals(Arrays.asList("0:0", "0:1", "0:2"), invoked);
        assertResolvedButNotCommitted(consumer, progress, 3L);
    }

    @Test
    public void withAutoCommitASkippedBatchIsResolvedButNotCommitted() throws Exception {
        failing.add("0:0-1");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        RecordProcessor processor = processor(skip(batchEndpoint(false, null)), null, null);

        PollProgress progress = processBatch(processor, consumer, false);

        assertResolvedButNotCommitted(consumer, progress, 2L);
    }

    @Test
    public void withAutoCommitAnUndeserializableRecordIsDeadLetteredButNotCommitted() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "BAD", "b");
        CpiKafkaPlusEndpoint endpoint = avroEndpoint(true);
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), failingAvro(endpoint));

        PollProgress progress = processSingle(processor, consumer, false);

        Assert.assertEquals(1, dlq.history().size());
        assertResolvedButNotCommitted(consumer, progress, 2L);
    }

    @Test
    public void withAutoCommitASchemaInvalidRecordIsDroppedButNotCommitted() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "{}", "{\"id\":\"1\"}");
        RecordProcessor processor = processor(endpoint(false, SCHEMA), null, null);

        PollProgress progress = processSingle(processor, consumer, false);

        Assert.assertEquals(Collections.singletonList("0:1"), invoked);
        assertResolvedButNotCommitted(consumer, progress, 2L);
    }

    @Test
    public void withAutoCommitADeadLetteredSchemaInvalidRecordIsResolvedButNotCommitted() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "{}", "{\"id\":\"1\"}");
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint(true, SCHEMA), dlqHelper(dlq), null);

        PollProgress progress = processSingle(processor, consumer, false);

        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals(Collections.singletonList("0:1"), invoked);
        assertResolvedButNotCommitted(consumer, progress, 2L);
    }

    // --- #182: what auto-pause gets to see ---

    @Test
    public void everyFailedRecordCountsForAutoPauseEvenWhenDeadLettered() throws Exception {
        failing.addAll(Arrays.asList("0:0", "0:2", "0:3"));
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c", "d");
        RecordProcessor processor = processor(endpoint(true, null), dlqHelper(okDlqProducer()), null);

        PollProgress progress = processSingle(processor, consumer);

        Assert.assertEquals(1, progress.successes());
        Assert.assertEquals(2, progress.failuresSinceLastSuccess());
    }

    // --- batch mode (#173, option C in the individual fallback) ---

    @Test
    public void invalidRecordIsNotCommittedPastAnEarlierBlockedRecord() throws Exception {
        // The filter dead-letters offset 2 first. The batch around it fails, and the individual
        // fallback blocks at offset 0 because its DLQ write fails: offset 2 must stay uncommitted.
        failing.addAll(Arrays.asList("0:0-3", "0:0-0"));
        MockConsumer<byte[], byte[]> consumer =
                consumerWith(P0, "{\"id\":\"a\"}", "{\"id\":\"b\"}", "{}", "{\"id\":\"d\"}");
        RecordProcessor processor = processor(batchEndpoint(true, SCHEMA), dlqFailingFromWrite(2), null);

        processBatch(processor, consumer);

        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void validRecordsBeforeAFilterBlockedRecordAreProcessed() throws Exception {
        // The DLQ write for the invalid record at offset 1 fails: offset 0 must still run.
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "{\"id\":\"a\"}", "{}", "{\"id\":\"c\"}");
        RecordProcessor processor = processor(batchEndpoint(true, SCHEMA), failingDlq(), null);

        processBatch(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:0-0"), invoked);
        Assert.assertEquals(1L, committed(consumer, P0));
        Assert.assertEquals(1L, consumer.position(P0));
    }

    @Test
    public void theBatchCountsEveryInvalidRecordButOnlyTheDeadLetteredOnes() throws Exception {
        // Offsets 1 and 3 are invalid: the DLQ takes the first and rejects the second.
        MockConsumer<byte[], byte[]> consumer =
                consumerWith(P0, "{\"id\":\"a\"}", "{}", "{\"id\":\"c\"}", "{}");
        RecordProcessor processor = processor(batchEndpoint(true, SCHEMA), dlqFailingFromWrite(2), null);

        PollProgress progress = processBatch(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:0-2"), invoked);
        Assert.assertEquals(2, headers.get(0).get("CpiKafkaPlusSchemaValidationFailures"));
        Assert.assertEquals(1, headers.get(0).get("CpiKafkaPlusDlqCount"));
        Assert.assertEquals(Long.valueOf(3L), progress.blockedOffset(P0));
        Assert.assertEquals(3L, committed(consumer, P0));
        Assert.assertEquals(3L, consumer.position(P0));
    }

    @Test
    public void deserializationFailureInTheSchemaFilterReachesTheDlq() throws Exception {
        CpiKafkaPlusEndpoint endpoint = batchEndpoint(true, SCHEMA);
        endpoint.setSchemaRegistryEnabled(true);
        endpoint.setSchemaRegistryUrl("http://localhost:1");
        endpoint.setAvroValueDeserialization(true);
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "BAD", "{\"id\":\"b\"}");
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), failingAvro(endpoint));

        processBatch(processor, consumer);

        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals(Collections.singletonList("0:1-1"), invoked);
        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void failedBatchWithoutDlqIsRetriedByDefault() throws Exception {
        failing.add("0:0-1");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        RecordProcessor processor = processor(batchEndpoint(false, null), null, null);

        processBatch(processor, consumer);

        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void failedBatchWithoutDlqIsSkippedWhenSkipping() throws Exception {
        failing.add("0:0-1");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        RecordProcessor processor = processor(skip(batchEndpoint(false, null)), null, null);

        processBatch(processor, consumer);

        Assert.assertEquals(2L, committed(consumer, P0));
        Assert.assertEquals(2L, consumer.position(P0));
    }

    @Test
    public void batchFormatFailureWithoutDlqIsRetriedByDefault() throws Exception {
        CpiKafkaPlusEndpoint endpoint = batchEndpoint(false, null);
        endpoint.setSchemaRegistryEnabled(true);
        endpoint.setSchemaRegistryUrl("http://localhost:1");
        endpoint.setAvroValueDeserialization(true);
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "BAD", "b");
        RecordProcessor processor = processor(endpoint, null, failingAvro(endpoint));

        PollProgress progress = processBatch(processor, consumer);

        // The whole batch is retried, the good record in it included.
        Assert.assertTrue(invoked.isEmpty());
        Assert.assertTrue(progress.isBlocked(P0));
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void deserializationFailureInTheSchemaFilterIsRetriedWithoutDlq() throws Exception {
        CpiKafkaPlusEndpoint endpoint = batchEndpoint(false, SCHEMA);
        endpoint.setSchemaRegistryEnabled(true);
        endpoint.setSchemaRegistryUrl("http://localhost:1");
        endpoint.setAvroValueDeserialization(true);
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "BAD", "{\"id\":\"b\"}");
        RecordProcessor processor = processor(endpoint, null, failingAvro(endpoint));

        PollProgress progress = processBatch(processor, consumer);

        Assert.assertTrue(invoked.isEmpty());
        Assert.assertEquals(Long.valueOf(0L), progress.blockedOffset(P0));
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void deserializationFailureInTheSchemaFilterIsSkippedWhenSkipping() throws Exception {
        CpiKafkaPlusEndpoint endpoint = skip(batchEndpoint(false, SCHEMA));
        endpoint.setSchemaRegistryEnabled(true);
        endpoint.setSchemaRegistryUrl("http://localhost:1");
        endpoint.setAvroValueDeserialization(true);
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "BAD", "{\"id\":\"b\"}");
        RecordProcessor processor = processor(endpoint, null, failingAvro(endpoint));

        PollProgress progress = processBatch(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:1-1"), invoked);
        Assert.assertFalse(progress.isBlocked(P0));
        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void batchFormatFailureWithoutDlqDoesNotEscapeThePoll() throws Exception {
        CpiKafkaPlusEndpoint endpoint = skip(batchEndpoint(false, null));
        endpoint.setSchemaRegistryEnabled(true);
        endpoint.setSchemaRegistryUrl("http://localhost:1");
        endpoint.setAvroValueDeserialization(true);
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "BAD", "b");
        RecordProcessor processor = processor(endpoint, null, failingAvro(endpoint));

        processBatch(processor, consumer);

        Assert.assertTrue(invoked.isEmpty());
        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void batchFormatFailureWithDlqFallsBackToSingleRecords() throws Exception {
        CpiKafkaPlusEndpoint endpoint = avroBatchEndpoint(true);
        endpoint.setDlqMaxRetries(2);
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "BAD", "b");
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), failingAvro(endpoint));

        PollProgress progress = processBatch(processor, consumer);

        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals("DESERIALIZATION", header(dlq.history().get(0), "CpiKafkaPlusDlqErrorType"));
        Assert.assertEquals("the good record runs on its own", Collections.singletonList("0:1-1"), invoked);
        Assert.assertEquals(Boolean.TRUE, headers.get(0).get("CpiKafkaPlusFallbackMode"));
        Assert.assertFalse(progress.isBlocked(P0));
        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void formatFailureInTheFallbackHandsTheRawValueToTheRoute() throws Exception {
        CpiKafkaPlusEndpoint endpoint = avroBatchEndpoint(true);
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "FLAKY");
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), flakyAvro(endpoint));

        processBatch(processor, consumer);

        // The batch fails to format, the record deserializes on its own, and formatting the
        // one-record batch fails again: the route gets the raw value rather than nothing.
        Assert.assertEquals(Collections.singletonList("0:0-0"), invoked);
        Assert.assertEquals("FLAKY", bodies.get(0));
        Assert.assertTrue(dlq.history().isEmpty());
        Assert.assertEquals(1L, committed(consumer, P0));
    }

    @Test
    public void errorWhileFormattingABatchEscapesThePollEvenWhenSkipping() throws Exception {
        NoClassDefFoundError error = new NoClassDefFoundError("io/confluent/kafka/schemaregistry/ParsedSchema");
        CpiKafkaPlusEndpoint endpoint = skip(avroBatchEndpoint(false));
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "ERR");
        RecordProcessor processor = processor(endpoint, null, erroringAvro(endpoint, error));

        Throwable escaped = processBatchUntilItEscapes(processor, consumer);

        // An Error is not a data problem, so it does not skip the batch: the poll retries it.
        Assert.assertEquals(RuntimeException.class, escaped.getClass());
        Assert.assertSame(error, escaped.getCause());
        Assert.assertTrue(invoked.isEmpty());
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void errorWhileFormattingABatchWithDlqEscapesFromTheFallback() throws Exception {
        NoClassDefFoundError error = new NoClassDefFoundError("io/confluent/kafka/schemaregistry/ParsedSchema");
        CpiKafkaPlusEndpoint endpoint = avroBatchEndpoint(true);
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "ERR", "c");
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), erroringAvro(endpoint, error));

        Throwable escaped = processBatchUntilItEscapes(processor, consumer);

        // With a DLQ the batch falls back to single records before the Error is looked at: the
        // records before the failing one are processed, and the Error escapes from that record.
        Assert.assertSame(error, escaped);
        Assert.assertEquals(Collections.singletonList("0:0-0"), invoked);
        Assert.assertTrue("an Error is not dead-lettered", dlq.history().isEmpty());
        Assert.assertEquals(1L, committed(consumer, P0));
        Assert.assertEquals(1L, consumer.position(P0));
    }

    @Test
    public void wakeupDuringBatchCommitDoesNotReprocessTheBatch() throws Exception {
        MockConsumer<byte[], byte[]> consumer =
                failingCommits(consumerWith(P0, "a", "b"), new AtomicInteger(), new WakeupException());
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(batchEndpoint(true, null), dlqHelper(dlq), null);

        PollProgress progress = processBatch(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:0-1"), invoked);
        Assert.assertTrue(dlq.history().isEmpty());
        Assert.assertTrue(progress.isStopped());
        Assert.assertEquals(Collections.singletonMap(P0, new OffsetAndMetadata(2L)), tracker.snapshot());
    }

    @Test
    public void successfulBatchIsCommittedOnce() throws Exception {
        AtomicInteger commits = new AtomicInteger();
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>("earliest") {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                commits.incrementAndGet();
                super.commitSync(offsets);
            }
        };
        fill(consumer, Collections.singletonMap(P0, Arrays.asList("a", "b")));
        RecordProcessor processor = processor(batchEndpoint(false, null), null, null);

        processBatch(processor, consumer);

        Assert.assertEquals("one broker round trip per batch", 1, commits.get());
        Assert.assertEquals(2L, committed(consumer, P0));
    }

    @Test
    public void individualFallbackStopsAtTheFirstBlockedRecord() throws Exception {
        failing.addAll(Arrays.asList("0:0-2", "0:1-1"));
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        RecordProcessor processor = processor(batchEndpoint(true, null), failingDlq(), null);

        processBatch(processor, consumer);

        Assert.assertEquals(Arrays.asList("0:0-2", "0:0-0", "0:1-1"), invoked);
        Assert.assertEquals(1L, committed(consumer, P0));
        Assert.assertEquals(1L, consumer.position(P0));
    }

    // --- a cyclic cause chain (#241): dead-lettered, not an endless walk on the poll thread ---

    @Test(timeout = 10_000L)
    public void routeFailureWithACyclicCauseIsDeadLettered() throws Exception {
        assertCyclicRouteFailureIsDeadLettered(false, true, "PERMANENT");
    }

    @Test(timeout = 10_000L)
    public void routeFailureWithACyclicCauseIsDeadLetteredWithoutTransientCheck() throws Exception {
        assertCyclicRouteFailureIsDeadLettered(false, false, null);
    }

    @Test(timeout = 10_000L)
    public void batchRouteFailureWithACyclicCauseIsDeadLettered() throws Exception {
        assertCyclicRouteFailureIsDeadLettered(true, true, "PERMANENT");
    }

    @Test(timeout = 10_000L)
    public void batchRouteFailureWithACyclicCauseIsDeadLetteredWithoutTransientCheck() throws Exception {
        assertCyclicRouteFailureIsDeadLettered(true, false, null);
    }

    private void assertCyclicRouteFailureIsDeadLettered(boolean batch, boolean retryOnlyTransientErrors,
                                                        String errorType) throws Exception {
        // the batch, then the record the batch fell back to; in single mode only the record
        failingCyclically.addAll(Arrays.asList("0:0-2", "0:1-1", "0:1"));
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        CpiKafkaPlusEndpoint endpoint = batch ? batchEndpoint(true, null) : endpoint(true, null);
        endpoint.setRetryOnlyTransientErrors(retryOnlyTransientErrors);
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), null);

        if (batch) {
            processBatch(processor, consumer);
            Assert.assertEquals(Arrays.asList("0:0-2", "0:0-0", "0:1-1", "0:2-2"), invoked);
        } else {
            processSingle(processor, consumer);
            Assert.assertEquals(Arrays.asList("0:0", "0:1", "0:2"), invoked);
        }
        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals("1", header(dlq.history().get(0), "CpiKafkaPlusDlqOriginalOffset"));
        Assert.assertEquals(errorType, header(dlq.history().get(0), "CpiKafkaPlusDlqErrorType"));
        Assert.assertEquals(3L, committed(consumer, P0));
    }

    // --- a stop during the route call (#242): the record stays unresolved, it is no poison record ---

    @Test
    public void stopDuringAPermanentRouteFailureDoesNotDeadLetter() throws Exception {
        stopAfterFirstRecord.set(true);
        failing.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        CpiKafkaPlusEndpoint endpoint = endpoint(true, null);
        endpoint.setDlqMaxRetries(2);
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordingTracingHelper tracing = new RecordingTracingHelper(endpoint);
        RecordProcessor processor = processor(endpoint, dlqHelper(dlq), null, tracing);

        PollProgress progress = processSingle(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:0"), invoked);
        Assert.assertTrue("a route call that fails during a stop is no poison record", dlq.history().isEmpty());
        Assert.assertTrue("nor a failure to report", tracing.traced.isEmpty());
        Assert.assertEquals("nor one that counts towards auto-pause", 0, progress.failuresSinceLastSuccess());
        Assert.assertFalse("a stop starts no retry backoff", progress.isBlocked(P0));
        Assert.assertTrue(progress.isStopped());
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void stopDuringTheLastAttemptDoesNotDeadLetter() throws Exception {
        // dlqMaxRetries=0: the first attempt is the last one, so no pause between two retries follows it
        stopAfterFirstRecord.set(true);
        failingTransiently.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(endpoint(true, null), dlqHelper(dlq), null);

        PollProgress progress = processSingle(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:0"), invoked);
        Assert.assertTrue(dlq.history().isEmpty());
        Assert.assertTrue(progress.isStopped());
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void stopDuringAFailedRouteCallDoesNotSkipTheRecord() throws Exception {
        stopAfterFirstRecord.set(true);
        failing.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        RecordProcessor processor = processor(skip(endpoint(false, null)), null, null);

        PollProgress progress = processSingle(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:0"), invoked);
        Assert.assertTrue(progress.isStopped());
        Assert.assertEquals("Skip Failed Message must not commit what the stop broke off",
                -1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void stopDuringAFailedBatchDoesNotSkipTheBatch() throws Exception {
        stopAfterFirstRecord.set(true);
        failing.add("0:0-2");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        RecordProcessor processor = processor(skip(batchEndpoint(false, null)), null, null);

        PollProgress progress = processBatch(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:0-2"), invoked);
        Assert.assertEquals(0, progress.failuresSinceLastSuccess());
        Assert.assertTrue(progress.isStopped());
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    @Test
    public void stopDuringAFailedBatchWithDlqDeadLettersNothing() throws Exception {
        stopAfterFirstRecord.set(true);
        failing.add("0:0-2");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(batchEndpoint(true, null), dlqHelper(dlq), null);

        PollProgress progress = processBatch(processor, consumer);

        Assert.assertEquals("no fallback to single records once the consumer stops",
                Collections.singletonList("0:0-2"), invoked);
        Assert.assertTrue(dlq.history().isEmpty());
        Assert.assertTrue(progress.isStopped());
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private PollProgress processBatch(RecordProcessor processor, MockConsumer<byte[], byte[]> consumer)
            throws Exception {
        return processBatch(processor, consumer, true);
    }

    /** {@code commitAfterSuccess=false} is how the consumer runs with {@code commitStrategy=AUTO}. */
    private PollProgress processBatch(RecordProcessor processor, MockConsumer<byte[], byte[]> consumer,
                                      boolean commitAfterSuccess) throws Exception {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ZERO);
        PollProgress progress = new PollProgress(records);
        processor.processBatchRecords(consumer, records, commitAfterSuccess, progress);
        processor.rewind(consumer, progress);
        return progress;
    }

    /**
     * Expects the poll's processing to throw and settles the poll as the consumer does then: the
     * record it escaped from is blocked and the partition rewound.
     */
    private static Throwable processBatchUntilItEscapes(RecordProcessor processor,
                                                        MockConsumer<byte[], byte[]> consumer) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ZERO);
        PollProgress progress = new PollProgress(records);
        try {
            processor.processBatchRecords(consumer, records, true, progress);
        } catch (Throwable t) {
            progress.blockCurrent();
            processor.rewind(consumer, progress);
            return t;
        }
        throw new AssertionError("expected the failure to escape the poll");
    }

    private void assertResolvedButNotCommitted(MockConsumer<byte[], byte[]> consumer, PollProgress progress,
                                               long next) {
        Assert.assertEquals("the auto-commit commits, not the adapter", -1L, committed(consumer, P0));
        Assert.assertTrue(tracker.isEmpty());
        Assert.assertFalse(progress.isBlocked(P0));
        Assert.assertEquals("resolved, so not rewound", next, consumer.position(P0));
    }

    private CpiKafkaPlusEndpoint batchEndpoint(boolean dlq, String jsonSchema) throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint(dlq, jsonSchema);
        endpoint.setBatchMode(true);
        endpoint.setBatchSize(10);
        endpoint.setBatchOutputFormat("JSON_ARRAY");
        return endpoint;
    }

    private PollProgress processSingle(RecordProcessor processor, MockConsumer<byte[], byte[]> consumer)
            throws Exception {
        return processSingle(processor, consumer, true);
    }

    /** {@code commitAfterSuccess=false} is how the consumer runs with {@code commitStrategy=AUTO}. */
    private PollProgress processSingle(RecordProcessor processor, MockConsumer<byte[], byte[]> consumer,
                                       boolean commitAfterSuccess) throws Exception {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ZERO);
        PollProgress progress = new PollProgress(records);
        processor.processSingleRecords(consumer, records, commitAfterSuccess, progress);
        processor.rewind(consumer, progress);
        return progress;
    }

    private CpiKafkaPlusEndpoint endpoint(boolean dlq, String jsonSchema) throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        DefaultCamelContext ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", component);
        ctx.start();
        contexts.add(ctx);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:" + TOPIC + "?bootstrapServers=localhost:9092&groupId=g"
                + "&commitStrategy=BATCH_COMPLETE");
        endpoint.setDlqEnabled(dlq);
        endpoint.setDlqTopic(dlq ? "orders-dlq" : null);
        endpoint.setDlqMaxRetries(0);
        endpoint.setRetryDelaySeconds(0);
        if (jsonSchema != null) {
            endpoint.setJsonSchemaValidation(true);
            endpoint.setJsonSchema(jsonSchema);
        }
        return endpoint;
    }

    private static CpiKafkaPlusEndpoint skip(CpiKafkaPlusEndpoint endpoint) {
        endpoint.setErrorHandling("SKIP");
        return endpoint;
    }

    private CpiKafkaPlusEndpoint avroEndpoint(boolean dlq) throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint(dlq, null);
        endpoint.setSchemaRegistryEnabled(true);
        endpoint.setSchemaRegistryUrl("http://localhost:1");
        endpoint.setAvroValueDeserialization(true);
        return endpoint;
    }

    private CpiKafkaPlusEndpoint avroBatchEndpoint(boolean dlq) throws Exception {
        CpiKafkaPlusEndpoint endpoint = batchEndpoint(dlq, null);
        endpoint.setSchemaRegistryEnabled(true);
        endpoint.setSchemaRegistryUrl("http://localhost:1");
        endpoint.setAvroValueDeserialization(true);
        return endpoint;
    }

    private RecordProcessor processor(CpiKafkaPlusEndpoint endpoint, DlqProducerHelper dlq,
                                      AvroDeserializerHelper avro) {
        return processor(endpoint, dlq, avro, new AdapterTracingHelper(endpoint));
    }

    private RecordProcessor processor(CpiKafkaPlusEndpoint endpoint, DlqProducerHelper dlq,
                                      AvroDeserializerHelper avro, AdapterTracingHelper tracing) {
        JsonSchemaValidator validator = endpoint.isJsonSchemaValidation()
                ? new JsonSchemaValidator(endpoint.getJsonSchema()) : null;
        return new RecordProcessor(endpoint, tracing, validator, dlq, avro,
                new RecordProcessor.ConsumerCallback() {
                    @Override
                    public void processExchange(Exchange exchange) throws Exception {
                        Object offset = exchange.getIn().getHeader("CpiKafkaPlusOffset");
                        String id = exchange.getIn().getHeader("CpiKafkaPlusPartition") + ":"
                                + (offset != null ? offset
                                        : exchange.getIn().getHeader("CpiKafkaPlusFirstOffset") + "-"
                                        + exchange.getIn().getHeader("CpiKafkaPlusLastOffset"));
                        invoked.add(id);
                        bodies.add(exchange.getIn().getBody(String.class));
                        headers.add(new HashMap<>(exchange.getIn().getHeaders()));
                        if (failing.contains(id)) {
                            throw new IllegalStateException("backend rejected " + id);
                        }
                        if (failingCyclically.contains(id)) {
                            IllegalStateException first = new IllegalStateException("backend rejected " + id);
                            first.initCause(new IllegalArgumentException("retry of " + id, first));
                            throw first;
                        }
                        Integer transientLeft = transientThenPermanent.get(id);
                        if (failingTransiently.contains(id) || (transientLeft != null && transientLeft > 0)) {
                            if (transientLeft != null) {
                                transientThenPermanent.put(id, transientLeft - 1);
                            }
                            throw new IllegalStateException("backend unreachable " + id,
                                    new java.net.ConnectException("Connection refused"));
                        }
                        if (transientLeft != null) {
                            throw new IllegalStateException("backend rejected " + id);
                        }
                    }

                    @Override
                    public void handleException(String message, Exchange exchange, Exception e) {
                        // reported through the tracing helper; nothing to do off-platform
                    }

                    @Override
                    public Exchange createExchange() {
                        return endpoint.createExchange();
                    }

                    @Override
                    public boolean isStopRequested() {
                        return stopAfterFirstRecord.get() && !invoked.isEmpty();
                    }
                }, tracker);
    }

    private static MockConsumer<byte[], byte[]> consumerWith(TopicPartition tp, String... values) {
        return consumerWith(Collections.singletonMap(tp, Arrays.asList(values)));
    }

    private static MockConsumer<byte[], byte[]> consumerWith(Map<TopicPartition, List<String>> values) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
        fill(consumer, values);
        return consumer;
    }

    private static void fill(MockConsumer<byte[], byte[]> consumer, Map<TopicPartition, List<String>> values) {
        consumer.assign(values.keySet());
        Map<TopicPartition, Long> beginning = new HashMap<>();
        for (TopicPartition tp : values.keySet()) {
            beginning.put(tp, 0L);
        }
        consumer.updateBeginningOffsets(beginning);
        for (Map.Entry<TopicPartition, List<String>> e : values.entrySet()) {
            long offset = 0L;
            for (String value : e.getValue()) {
                consumer.addRecord(new ConsumerRecord<>(TOPIC, e.getKey().partition(), offset++,
                        null, value.getBytes(StandardCharsets.UTF_8)));
            }
        }
    }

    /** Copies {@code source}'s records into a consumer whose first commit throws {@code failure}. */
    private static MockConsumer<byte[], byte[]> failingCommits(MockConsumer<byte[], byte[]> source,
                                                               AtomicInteger commits,
                                                               KafkaException failure) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>("earliest") {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                if (commits.getAndIncrement() == 0) {
                    throw failure;
                }
                super.commitSync(offsets);
            }
        };
        ConsumerRecords<byte[], byte[]> records = source.poll(Duration.ZERO);
        Map<TopicPartition, List<String>> values = new HashMap<>();
        for (ConsumerRecord<byte[], byte[]> r : records) {
            values.computeIfAbsent(new TopicPartition(r.topic(), r.partition()), k -> new ArrayList<>())
                    .add(new String(r.value(), StandardCharsets.UTF_8));
        }
        fill(consumer, values);
        return consumer;
    }

    private static long committed(MockConsumer<byte[], byte[]> consumer, TopicPartition tp) {
        OffsetAndMetadata committed = consumer.committed(Collections.singleton(tp)).get(tp);
        return committed != null ? committed.offset() : -1L;
    }

    private static String header(ProducerRecord<byte[], byte[]> record, String key) {
        org.apache.kafka.common.header.Header header = record.headers().lastHeader(key);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    private static MockProducer<byte[], byte[]> okDlqProducer() {
        return new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static DlqProducerHelper dlqHelper(MockProducer<byte[], byte[]> producer) {
        return new DlqProducerHelper("orders-dlq", producer, ProducerSendGuard.of(500L, "PLAINTEXT"));
    }

    /** A DLQ whose every write is rejected, like a missing topic or a missing ACL. */
    private static DlqProducerHelper failingDlq() {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
                true, null, new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
                CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
                future.completeExceptionally(new TopicAuthorizationException("no WRITE on orders-dlq"));
                return future;
            }
        };
        return dlqHelper(producer);
    }

    /** A DLQ that accepts the first {@code firstFailingWrite - 1} writes and rejects the rest. */
    private static DlqProducerHelper dlqFailingFromWrite(int firstFailingWrite) {
        AtomicInteger writes = new AtomicInteger();
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(
                true, null, new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
                if (writes.incrementAndGet() < firstFailingWrite) {
                    return super.send(record);
                }
                CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
                future.completeExceptionally(new TopicAuthorizationException("no WRITE on orders-dlq"));
                return future;
            }
        };
        return dlqHelper(producer);
    }

    /** Fails for the value "BAD", like a record that is not in Confluent Avro wire format. */
    private static AvroDeserializerHelper failingAvro(CpiKafkaPlusEndpoint endpoint) {
        return new AvroDeserializerHelper(endpoint) {
            @Override
            public String deserialize(String topic, byte[] data) {
                String value = new String(data, StandardCharsets.UTF_8);
                if ("BAD".equals(value)) {
                    throw new IllegalArgumentException("Unknown magic byte!");
                }
                return value;
            }
        };
    }

    /** Throws {@code error} for the value "ERR", like a class that an adapter update took away. */
    private static AvroDeserializerHelper erroringAvro(CpiKafkaPlusEndpoint endpoint, Error error) {
        return new AvroDeserializerHelper(endpoint) {
            @Override
            public String deserialize(String topic, byte[] data) {
                String value = new String(data, StandardCharsets.UTF_8);
                if ("ERR".equals(value)) {
                    throw error;
                }
                return value;
            }
        };
    }

    /** Fails the first and the third time it reads the value "FLAKY", and succeeds in between. */
    private static AvroDeserializerHelper flakyAvro(CpiKafkaPlusEndpoint endpoint) {
        AtomicInteger reads = new AtomicInteger();
        return new AvroDeserializerHelper(endpoint) {
            @Override
            public String deserialize(String topic, byte[] data) {
                String value = new String(data, StandardCharsets.UTF_8);
                if ("FLAKY".equals(value) && reads.incrementAndGet() != 2) {
                    throw new IllegalStateException("Schema Registry unreachable");
                }
                return value;
            }
        };
    }

    /** Records the context of every failure trace, which carries the failure's error type. */
    private static final class RecordingTracingHelper extends AdapterTracingHelper {
        final List<Map<String, String>> traced = new ArrayList<>();

        RecordingTracingHelper(CpiKafkaPlusEndpoint endpoint) {
            super(endpoint);
        }

        @Override
        public void traceError(Exchange exchange, Exception e, Map<String, String> context,
                               boolean senderDirection) {
            traced.add(new HashMap<>(context));
            super.traceError(exchange, e, context, senderDirection);
        }
    }
}
