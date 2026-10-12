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
import java.lang.reflect.Method;
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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * A poll starts no record, batch or retry after its time limit, half of {@code max.poll.interval.ms},
 * and a record whose retries the limit interrupts continues with a later poll instead of starting over
 * (issue #192). Time is the test's clock; every route call advances it.
 */
public class PollTimeLimitTest {

    private static final String TOPIC = "orders";
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);

    private final List<DefaultCamelContext> contexts = new ArrayList<>();
    private final AtomicLong now = new AtomicLong();
    /** How far each route call advances {@link #now}. */
    private long routeCallMs = 30_000L;
    /** Route calls in order: "p:offset" for a record, "p:first-last" for a batch. */
    private final List<String> invoked = new ArrayList<>();
    private final Set<String> failing = new HashSet<>();
    private final OffsetCommitTracker tracker = new OffsetCommitTracker();

    @After
    public void tearDown() throws Exception {
        for (DefaultCamelContext ctx : contexts) {
            ctx.close();
        }
    }

    @Test
    public void noRecordStartsAfterTheTimeLimit() throws Exception {
        routeCallMs = 100_000L;
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c", "d", "e");
        RecordProcessor processor = processor(endpoint(false), null);

        List<String> lines = new ArrayList<>();
        PollProgress first = logged(lines, () -> poll(processor, consumer, false, 250_000L));

        Assert.assertEquals("started at 0, 100 and 200 s, the limit is 250 s",
                Arrays.asList("0:0", "0:1", "0:2"), invoked);
        Assert.assertTrue(first.isOutOfTime());
        Assert.assertFalse(first.isBlocked(P0));
        Assert.assertEquals(3L, committed(consumer, P0));
        Assert.assertEquals("the rest comes with the next poll", 3L, consumer.position(P0));
        List<String> limitLines = matching(lines, "consumer.poll.timeLimit");
        Assert.assertEquals(lines.toString(), 1, limitLines.size());
        Assert.assertTrue(limitLines.get(0), limitLines.get(0).contains(
                " topic=orders partition=0 offset=3 pollElapsedMs=300000 pollTimeLimitMs=250000 "));

        offer(consumer, P0, "a", "b", "c", "d", "e");
        PollProgress second = poll(processor, consumer, false, 250_000L);

        Assert.assertEquals(Arrays.asList("0:0", "0:1", "0:2", "0:3", "0:4"), invoked);
        Assert.assertFalse(second.isOutOfTime());
        Assert.assertEquals(5L, committed(consumer, P0));
    }

    @Test
    public void noBatchStartsAfterTheTimeLimit() throws Exception {
        routeCallMs = 100_000L;
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c", "d", "e", "f");
        CpiKafkaPlusEndpoint endpoint = batchEndpoint(false);
        endpoint.setBatchSize(2);
        RecordProcessor processor = processor(endpoint, null);

        PollProgress progress = poll(processor, consumer, true, 150_000L);

        Assert.assertEquals(Arrays.asList("0:0-1", "0:2-3"), invoked);
        Assert.assertTrue(progress.isOutOfTime());
        Assert.assertEquals(4L, committed(consumer, P0));
        Assert.assertEquals(4L, consumer.position(P0));
    }

    @Test
    public void noRecordOfABatchFallbackStartsAfterTheTimeLimit() throws Exception {
        failing.add("0:0-3");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c", "d");
        RecordProcessor processor = processor(batchEndpoint(true), dlqHelper(okDlqProducer()));

        PollProgress progress = poll(processor, consumer, true, 80_000L);

        Assert.assertEquals("the batch and two of its records, the third would start at 90 s",
                Arrays.asList("0:0-3", "0:0-0", "0:1-1"), invoked);
        Assert.assertTrue(progress.isOutOfTime());
        Assert.assertEquals(2L, committed(consumer, P0));
        Assert.assertEquals(2L, consumer.position(P0));
    }

    @Test
    public void interruptedRetriesContinueWithTheNextPoll() throws Exception {
        failing.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a");
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(retrying(endpoint(true), 0), dlqHelper(dlq));

        List<String> lines = new ArrayList<>();
        PollProgress first = logged(lines, () -> poll(processor, consumer, false, 50_000L));

        Assert.assertEquals("the second attempt ended at 60 s, past the limit of 50 s",
                Arrays.asList("0:0", "0:0"), invoked);
        Assert.assertTrue(first.isBlocked(P0));
        Assert.assertEquals(Long.valueOf(60_000L), first.deferredUntil(P0));
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
        Assert.assertTrue(dlq.history().isEmpty());
        List<String> deferred = matching(lines, "consumer.retry.deferred");
        Assert.assertEquals(lines.toString(), 1, deferred.size());
        Assert.assertTrue(deferred.get(0), deferred.get(0).contains(" topic=orders partition=0 offset=0 retry=2 "
                + "maxRetries=3 retryInMs=0 pollElapsedMs=60000 pollTimeLimitMs=50000 "));

        offer(consumer, P0, "a");
        poll(processor, consumer, false, 50_000L);

        Assert.assertEquals("two attempts in each poll, not four in the second",
                Arrays.asList("0:0", "0:0", "0:0", "0:0"), invoked);
        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals("3", header(dlq.history().get(0), "CpiKafkaPlusDlqRetryCount"));
        Assert.assertEquals(1L, committed(consumer, P0));
    }

    @Test(timeout = 10_000L)
    public void aRetryDelayPastTheTimeLimitIsNotSleptOnThePollThread() throws Exception {
        routeCallMs = 1_000L;
        failing.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a");
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(retrying(endpoint(true), 60), dlqHelper(dlq));

        // Retry 1 after 60 s, retry 2 after 120 s, retry 3 after 240 s: each is due past the limit.
        List<Long> dueAt = new ArrayList<>();
        for (int poll = 0; poll < 3; poll++) {
            PollProgress progress = poll(processor, consumer, false, 10_000L);
            Long due = progress.deferredUntil(P0);
            Assert.assertNotNull("poll " + poll + " deferred the next retry", due);
            dueAt.add(due);
            now.set(due);
            offer(consumer, P0, "a");
        }
        poll(processor, consumer, false, 10_000L);

        Assert.assertEquals(Arrays.asList(61_000L, 182_000L, 423_000L), dueAt);
        Assert.assertEquals(Arrays.asList("0:0", "0:0", "0:0", "0:0"), invoked);
        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals("3", header(dlq.history().get(0), "CpiKafkaPlusDlqRetryCount"));
        Assert.assertEquals(1L, committed(consumer, P0));
    }

    @Test
    public void aDeferredRetryHoldsBackOnlyItsOwnPartition() throws Exception {
        routeCallMs = 1_000L;
        failing.add("0:0");
        Map<TopicPartition, List<String>> values = new HashMap<>();
        values.put(P0, Arrays.asList("a", "b"));
        values.put(P1, Arrays.asList("x", "y"));
        MockConsumer<byte[], byte[]> consumer = consumerWith(values);
        RecordProcessor processor = processor(retrying(endpoint(true), 60), dlqHelper(okDlqProducer()));

        PollProgress progress = poll(processor, consumer, false, 10_000L);

        Assert.assertNotNull(progress.deferredUntil(P0));
        Assert.assertFalse(progress.isOutOfTime());
        Assert.assertEquals(Arrays.asList("0:0", "1:0", "1:1"), sorted(invoked));
        Assert.assertEquals(-1L, committed(consumer, P0));
        Assert.assertEquals(0L, consumer.position(P0));
        Assert.assertEquals(2L, committed(consumer, P1));
    }

    @Test
    public void aRevokedPartitionStartsItsRecordOver() throws Exception {
        failing.add("0:0");
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a");
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(retrying(endpoint(true), 0), dlqHelper(dlq));
        poll(processor, consumer, false, 50_000L);

        processor.commitOnRevoke(offsets -> { }, Collections.singleton(P0));
        offer(consumer, P0, "a");
        poll(processor, consumer, false, 1_000_000L);

        Assert.assertEquals("two attempts before the revoke, then all four again", 6, invoked.size());
        Assert.assertEquals("3", header(dlq.history().get(0), "CpiKafkaPlusDlqRetryCount"));
    }

    @Test
    public void interruptedRetriesOfABatchFallbackContinueWithTheNextPoll() throws Exception {
        failing.addAll(Arrays.asList("0:0-2", "0:1-2", "0:1-1"));
        MockConsumer<byte[], byte[]> consumer = consumerWith(P0, "a", "b", "c");
        MockProducer<byte[], byte[]> dlq = okDlqProducer();
        RecordProcessor processor = processor(retrying(batchEndpoint(true), 0), dlqHelper(dlq));

        PollProgress first = poll(processor, consumer, true, 100_000L);

        Assert.assertEquals(Arrays.asList("0:0-2", "0:0-0", "0:1-1", "0:1-1"), invoked);
        Assert.assertEquals(Long.valueOf(120_000L), first.deferredUntil(P0));
        Assert.assertEquals(1L, committed(consumer, P0));
        Assert.assertEquals(1L, consumer.position(P0));

        offer(consumer, P0, "a", "b", "c");
        poll(processor, consumer, true, 100_000L);

        Assert.assertEquals("the batch from the open record, then its last two retries",
                Arrays.asList("0:0-2", "0:0-0", "0:1-1", "0:1-1", "0:1-2", "0:1-1", "0:1-1", "0:2-2"), invoked);
        Assert.assertEquals(1, dlq.history().size());
        Assert.assertEquals("3", header(dlq.history().get(0), "CpiKafkaPlusDlqRetryCount"));
        Assert.assertEquals(3L, committed(consumer, P0));
    }

    @Test
    public void theConsumerGivesEachPollHalfOfMaxPollInterval() throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint(false);
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, exchange -> { });
        Method limit = CpiKafkaPlusConsumer.class.getDeclaredMethod("pollTimeLimitMs");
        limit.setAccessible(true);

        endpoint.setConsumptionMode("STREAMING");
        Assert.assertEquals(300_000L, limit.invoke(consumer));

        endpoint.setConsumptionMode("SCHEDULED");
        endpoint.setPollingIntervalSeconds(5);
        Assert.assertEquals(302_500L, limit.invoke(consumer));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private interface PollCall {
        PollProgress run() throws Exception;
    }

    /**
     * One poll as the consumer runs it, with a time limit of {@code limitMs} from now: the records are
     * processed and every partition is rewound to its first open record.
     */
    private PollProgress poll(RecordProcessor processor, MockConsumer<byte[], byte[]> consumer, boolean batch,
                              long limitMs) throws Exception {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ZERO);
        PollProgress progress = new PollProgress(records, now.get(), limitMs);
        if (batch) {
            processor.processBatchRecords(consumer, records, true, progress);
        } else {
            processor.processSingleRecords(consumer, records, true, progress);
        }
        processor.rewind(consumer, progress);
        return progress;
    }

    /** Runs {@code call} and adds the lines this thread logged meanwhile to {@code lines}. */
    private static PollProgress logged(List<String> lines, PollCall call) throws Exception {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, "UTF-8"));
        try {
            return call.run();
        } finally {
            System.setErr(original);
            // Kafka clients left behind by other tests keep logging on their own threads.
            String own = "[" + Thread.currentThread().getName() + "] ";
            for (String line : new String(captured.toByteArray(), StandardCharsets.UTF_8).split("\\R")) {
                if (line.startsWith(own)) {
                    lines.add(line);
                }
            }
        }
    }

    private static List<String> matching(List<String> lines, String event) {
        List<String> out = new ArrayList<>();
        for (String line : lines) {
            if (line.contains("[CPI-KAFKA-PLUS-DIAG] " + event + " ")) {
                out.add(line);
            }
        }
        return out;
    }

    private static List<String> sorted(List<String> values) {
        List<String> out = new ArrayList<>(values);
        Collections.sort(out);
        return out;
    }

    private CpiKafkaPlusEndpoint endpoint(boolean dlq) throws Exception {
        DefaultCamelContext ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
        contexts.add(ctx);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:" + TOPIC + "?bootstrapServers=localhost:9092&groupId=g"
                + "&commitStrategy=BATCH_COMPLETE");
        endpoint.setDlqEnabled(dlq);
        endpoint.setDlqTopic(dlq ? "orders-dlq" : null);
        endpoint.setDlqMaxRetries(0);
        endpoint.setRetryDelaySeconds(0);
        return endpoint;
    }

    private CpiKafkaPlusEndpoint batchEndpoint(boolean dlq) throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint(dlq);
        endpoint.setBatchMode(true);
        endpoint.setBatchSize(10);
        endpoint.setBatchOutputFormat("JSON_ARRAY");
        return endpoint;
    }

    /** Three retries before the DLQ, the default, with {@code delaySeconds} as Retry Delay. */
    private static CpiKafkaPlusEndpoint retrying(CpiKafkaPlusEndpoint endpoint, int delaySeconds) {
        endpoint.setDlqMaxRetries(3);
        endpoint.setRetryDelaySeconds(delaySeconds);
        return endpoint;
    }

    private RecordProcessor processor(CpiKafkaPlusEndpoint endpoint, DlqProducerHelper dlq) {
        return new RecordProcessor(endpoint, new AdapterTracingHelper(endpoint), null, dlq, null,
                new RecordProcessor.ConsumerCallback() {
                    @Override
                    public void processExchange(Exchange exchange) throws Exception {
                        Object offset = exchange.getIn().getHeader("CpiKafkaPlusOffset");
                        String id = exchange.getIn().getHeader("CpiKafkaPlusPartition") + ":"
                                + (offset != null ? offset
                                        : exchange.getIn().getHeader("CpiKafkaPlusFirstOffset") + "-"
                                        + exchange.getIn().getHeader("CpiKafkaPlusLastOffset"));
                        invoked.add(id);
                        now.addAndGet(routeCallMs);
                        if (failing.contains(id)) {
                            throw new IllegalStateException("backend unreachable " + id,
                                    new java.net.ConnectException("Connection refused"));
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
                }, tracker, now::get);
    }

    private static MockConsumer<byte[], byte[]> consumerWith(TopicPartition tp, String... values) {
        return consumerWith(Collections.singletonMap(tp, Arrays.asList(values)));
    }

    private static MockConsumer<byte[], byte[]> consumerWith(Map<TopicPartition, List<String>> values) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
        consumer.assign(values.keySet());
        Map<TopicPartition, Long> beginning = new HashMap<>();
        for (TopicPartition tp : values.keySet()) {
            beginning.put(tp, 0L);
        }
        consumer.updateBeginningOffsets(beginning);
        for (Map.Entry<TopicPartition, List<String>> e : values.entrySet()) {
            offer(consumer, e.getKey(), e.getValue().toArray(new String[0]));
        }
        return consumer;
    }

    /**
     * Hands the partition's records to the next poll. A mock consumer forgets the records it
     * returned, while a broker keeps them for a poll after a rewind.
     */
    private static void offer(MockConsumer<byte[], byte[]> consumer, TopicPartition tp, String... values) {
        for (int offset = 0; offset < values.length; offset++) {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, tp.partition(), offset, null,
                    values[offset].getBytes(StandardCharsets.UTF_8)));
        }
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
}
