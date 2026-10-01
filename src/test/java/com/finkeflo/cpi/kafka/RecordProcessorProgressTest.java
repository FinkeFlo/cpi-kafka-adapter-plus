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
    public void withoutDlqTheFailedRecordIsSkipped() throws Exception {
        failing.add("0:1");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        RecordProcessor processor = processor(endpoint(false, null), null, null);

        processSingle(processor, consumer);

        Assert.assertEquals(Arrays.asList("0:0", "0:1", "0:2"), invoked);
        Assert.assertEquals(3L, committed(consumer, P0));
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
    public void deserializationFailureWithoutDlqSkipsOnlyThatRecord() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "BAD", "b");
        CpiKafkaPlusEndpoint endpoint = avroEndpoint(false);
        RecordProcessor processor = processor(endpoint, null, failingAvro(endpoint));

        processSingle(processor, consumer);

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
    public void schemaInvalidRecordWithoutDlqIsDroppedAsDocumented() throws Exception {
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "{}", "{\"id\":\"1\"}");
        RecordProcessor processor = processor(endpoint(false, SCHEMA), null, null);

        processSingle(processor, consumer);

        Assert.assertEquals(Collections.singletonList("0:1"), invoked);
        Assert.assertEquals(2L, committed(consumer, P0));
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
    public void failedBatchWithoutDlqIsSkipped() throws Exception {
        failing.add("0:0-1");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b");
        RecordProcessor processor = processor(batchEndpoint(false, null), null, null);

        processBatch(processor, consumer);

        Assert.assertEquals(2L, committed(consumer, P0));
        Assert.assertEquals(2L, consumer.position(P0));
    }

    @Test
    public void batchFormatFailureWithoutDlqDoesNotEscapeThePoll() throws Exception {
        CpiKafkaPlusEndpoint endpoint = batchEndpoint(false, null);
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
    public void individualFallbackStopsAtTheFirstBlockedRecord() throws Exception {
        failing.addAll(Arrays.asList("0:0-2", "0:1-1"));
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        RecordProcessor processor = processor(batchEndpoint(true, null), failingDlq(), null);

        processBatch(processor, consumer);

        Assert.assertEquals(Arrays.asList("0:0-2", "0:0-0", "0:1-1"), invoked);
        Assert.assertEquals(1L, committed(consumer, P0));
        Assert.assertEquals(1L, consumer.position(P0));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private PollProgress processBatch(RecordProcessor processor, MockConsumer<byte[], byte[]> consumer)
            throws Exception {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ZERO);
        PollProgress progress = new PollProgress(records);
        processor.processBatchRecords(consumer, records, true, progress);
        processor.rewind(consumer, progress);
        return progress;
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
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ZERO);
        PollProgress progress = new PollProgress(records);
        processor.processSingleRecords(consumer, records, true, progress);
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

    private CpiKafkaPlusEndpoint avroEndpoint(boolean dlq) throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint(dlq, null);
        endpoint.setSchemaRegistryEnabled(true);
        endpoint.setSchemaRegistryUrl("http://localhost:1");
        endpoint.setAvroValueDeserialization(true);
        return endpoint;
    }

    private RecordProcessor processor(CpiKafkaPlusEndpoint endpoint, DlqProducerHelper dlq,
                                      AvroDeserializerHelper avro) {
        JsonSchemaValidator validator = endpoint.isJsonSchemaValidation()
                ? new JsonSchemaValidator(endpoint.getJsonSchema()) : null;
        return new RecordProcessor(endpoint, new AdapterTracingHelper(endpoint), validator, dlq, avro,
                new RecordProcessor.ConsumerCallback() {
                    @Override
                    public void processExchange(Exchange exchange) throws Exception {
                        Object offset = exchange.getIn().getHeader("CpiKafkaPlusOffset");
                        String id = exchange.getIn().getHeader("CpiKafkaPlusPartition") + ":"
                                + (offset != null ? offset
                                        : exchange.getIn().getHeader("CpiKafkaPlusFirstOffset") + "-"
                                        + exchange.getIn().getHeader("CpiKafkaPlusLastOffset"));
                        invoked.add(id);
                        if (failing.contains(id)) {
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
}
