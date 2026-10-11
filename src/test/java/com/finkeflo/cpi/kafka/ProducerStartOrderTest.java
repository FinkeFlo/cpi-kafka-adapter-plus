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
import java.util.Arrays;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The order of the receiver's start-up checks and the lines it logs between them. With more than
 * one wrong value the order decides which message the operator sees first. The checks run in
 * phases: the endpoint's shared checks and the values that can never work come before
 * {@code super.doStart()} and the start-up line, the transaction settings, the transaction pool and
 * the retry settings after it, so a rejection there still leaves the start-up line in the trace.
 */
public class ProducerStartOrderTest {

    private static final String PRODUCER = CpiKafkaPlusProducer.class.getName();
    private static final String START_UP_LINE = "INFO " + PRODUCER + " - [CPI-KAFKA-PLUS-DIAG] Starting CPI "
            + "Kafka Producer for topic 'orders'";

    private DefaultCamelContext ctx;

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        ctx.close();
    }

    /**
     * Starts with every check failing at once, then fixes one value at a time: each start has to be
     * rejected by the next check below, and the last one starts. The delivery timeout and the
     * transactional.id prefix move from one wrong value to the next.
     */
    @Test
    public void eachCheckRejectsOnlyOnceTheEarlierOnesPass() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setDiagnosticsLevel("VERBOSE");
        e.setProducerBatchMode("CSV");
        e.setDeliveryTimeoutSeconds(0);
        e.setSchemaRegistryEnabled(true);
        e.setSchemaRegistryUrl("http://localhost:8081");
        e.setAvroValueSerialization(true);
        e.setSubjectNameStrategy("RecordNameStrategy");
        e.setEnableTransactions(true);
        e.setTransactionalIdPrefix(" ");
        e.setEnableIdempotence(false);
        e.setMaxConcurrentTransactions(0);
        e.setProducerRetryMaxAttempts(6);
        e.setProducerRetryDelaySeconds(0);
        e.setProducerRetryTotalBudgetSeconds(901);

        // CpiKafkaPlusEndpoint.validateConfiguration(), shared with the sender, comes first
        assertRejected(e, "[CPI-KAFKA-PLUS-DIAG] Invalid diagnosticsLevel 'VERBOSE'. Accepted values are "
                + "STANDARD (default) or FULL.");
        e.setDiagnosticsLevel("STANDARD");
        assertRejected(e, "Unknown 'Batch Send Mode' (producerBatchMode): CSV. Supported: NONE, JSON_ARRAY, "
                + "XML_LIST");
        e.setProducerBatchMode("JSON_ARRAY");
        assertRejected(e, "'Delivery Timeout (Seconds)' (deliveryTimeoutSeconds) must be at least 1, got: 0. "
                + "With 0 every send times out before it is sent.");
        e.setDeliveryTimeoutSeconds(871);
        assertRejected(e, "'Subject Name Strategy' (subjectNameStrategy) 'RecordNameStrategy' is not supported "
                + "for Avro serialization: the subject cannot be resolved from JSON input without the record "
                + "name. Please use TopicNameStrategy.");
        e.setSubjectNameStrategy("TopicNameStrategy");
        // after super.doStart() and the start-up line
        assertRejected(e, "Please configure transactionalIdPrefix — it is required whenever transactional "
                + "batching is switched on");
        e.setTransactionalIdPrefix(repeat('x', 240));
        assertRejected(e, "Please leave idempotence switched on — it cannot be turned off while transactional "
                + "batching is enabled");
        e.setEnableIdempotence(true);
        assertRejected(e, "Please set deliveryTimeoutSeconds to at most 870 on a transactional channel "
                + "(configured value: 871s). The adapter derives transaction.timeout.ms from it, and a broker "
                + "rejects a producer asking for more than transaction.max.timeout.ms (15 minutes by default).");
        e.setDeliveryTimeoutSeconds(2);
        assertRejected(e, "Please set maxConcurrentTransactions to 1 or higher (configured value: 0) — with "
                + "transactional batching enabled, a value below 1 would leave the adapter unable to send "
                + "anything");
        e.setMaxConcurrentTransactions(1);
        // TransactionalProducerPool.start(); the member suffix depends on the node
        String tooLong = rejection(e);
        Assert.assertTrue(tooLong, tooLong.matches("The computed transactional\\.id 'x{240}-[0-9a-f]{8}-[^']+-0' "
                + "is \\d+ characters long\\. Please shorten transactionalIdPrefix to keep it under 249 "
                + "characters\\."));
        e.setTransactionalIdPrefix("orders-tx");
        assertRejected(e, "Please set producerRetryMaxAttempts between 1 and 5 (configured value: 6). This is "
                + "the total number of attempts, not the number of additional ones; 1 switches the retry off.");
        e.setProducerRetryMaxAttempts(2);
        assertRejected(e, "Please set producerRetryDelaySeconds between 1 and 30 (configured value: 0). A delay "
                + "below one second cannot help against a disconnected broker node — the Kafka client has "
                + "already backed off exponentially before giving up — and only adds load.");
        e.setProducerRetryDelaySeconds(2);
        assertRejected(e, "Please set producerRetryTotalBudgetSeconds between 5 and 900 (configured value: 901).");
        e.setProducerRetryTotalBudgetSeconds(5);
        assertRejected(e, "Producer retry is configured with a worst case of 28s, which exceeds "
                + "producerRetryTotalBudgetSeconds=5. Lower deliveryTimeoutSeconds (currently 2s) or "
                + "producerRetryMaxAttempts, or raise the budget. The budget must stay below the timeout of "
                + "the calling HTTP client. Note that one attempt costs roughly four times "
                + "deliveryTimeoutSeconds on the transactional path, because initTransactions(), the metadata "
                + "block inside send() and commitTransaction() are each bounded by max.block.ms = min(30s, "
                + "deliveryTimeoutSeconds) on top of the wait for acknowledgements.");
        e.setProducerRetryTotalBudgetSeconds(30);
        assertStarts(e);
    }

    @Test
    public void aValueThatCanNeverWorkIsRejectedBeforeTheStartUpLine() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setSchemaRegistryEnabled(true);
        e.setSchemaRegistryUrl("http://localhost:8081");
        e.setAvroValueSerialization(true);
        e.setSubjectNameStrategy("RecordNameStrategy");

        String logged = capture(() -> rejection(e));

        Assert.assertTrue(logged, logged.contains("producer.start.failed"));
        Assert.assertFalse(logged, logged.contains("Starting CPI Kafka Producer"));
    }

    @Test
    public void aTransactionSettingIsRejectedAfterTheStartUpLine() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("JSON_ARRAY");
        e.setEnableTransactions(true);
        e.setTransactionalIdPrefix(null);

        String logged = capture(() -> assertRejected(e, "Please configure transactionalIdPrefix — it is required "
                + "whenever transactional batching is switched on"));

        assertInOrder(logged, START_UP_LINE, "producer.start.failed");
    }

    @Test
    public void ineffectiveTransactionsAreLoggedEvenWhenTheRetryCheckRejects() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setEnableTransactions(true);
        e.setProducerRetryMaxAttempts(6);

        String logged = capture(() -> assertRejected(e, "Please set producerRetryMaxAttempts between 1 and 5 "
                + "(configured value: 6). This is the total number of attempts, not the number of additional "
                + "ones; 1 switches the retry off."));

        assertInOrder(logged, START_UP_LINE,
                "ERROR " + PRODUCER + " - [CPI-KAFKA-PLUS-DIAG] producer.config.ineffective "
                        + "parameter=enableTransactions producerBatchMode=NONE reason='transactions apply to "
                        + "batches only; single messages are sent by the shared, non-transactional producer'",
                "producer.start.failed");
    }

    @Test
    public void theSingleSlotWarningIsLoggedEvenWhenTheWorstCaseCheckRejects() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("JSON_ARRAY");
        e.setEnableTransactions(true);
        e.setTransactionalIdPrefix("orders-tx");
        e.setMaxConcurrentTransactions(1);
        e.setProducerRetryMaxAttempts(2);

        String logged = capture(() -> assertRejected(e, "Producer retry is configured with a worst case of 432s, "
                + "which exceeds producerRetryTotalBudgetSeconds=30. Lower deliveryTimeoutSeconds (currently "
                + "120s) or producerRetryMaxAttempts, or raise the budget. The budget must stay below the "
                + "timeout of the calling HTTP client. Note that one attempt costs roughly four times "
                + "deliveryTimeoutSeconds on the transactional path, because initTransactions(), the metadata "
                + "block inside send() and commitTransaction() are each bounded by max.block.ms = min(30s, "
                + "deliveryTimeoutSeconds) on top of the wait for acknowledgements."));

        assertInOrder(logged, START_UP_LINE,
                "INFO " + PRODUCER + " - [CPI-KAFKA-PLUS-DIAG] Transactional batching enabled with max 1 "
                        + "concurrent transactions.",
                "WARN " + PRODUCER + " - [CPI-KAFKA-PLUS-DIAG] producerRetryMaxAttempts=2 with "
                        + "maxConcurrentTransactions=1: a retry holds the single transaction slot for its whole "
                        + "duration (up to producerRetryTotalBudgetSeconds=30s), blocking every other send on "
                        + "this node meanwhile. Consider raising maxConcurrentTransactions.",
                "producer.start.failed");
    }

    /**
     * Without transactions a batch is never retried: the start warns and skips the worst case, here
     * with a budget it could never meet. The ranges are still checked first.
     */
    @Test
    public void aBatchWithoutTransactionsWarnsAfterTheRangesAndSkipsTheWorstCase() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("JSON_ARRAY");
        e.setProducerRetryMaxAttempts(2);
        e.setProducerRetryTotalBudgetSeconds(5);
        String warning = "WARN " + PRODUCER + " - [CPI-KAFKA-PLUS-DIAG] producerRetryMaxAttempts=2 has no effect "
                + "for producerBatchMode=JSON_ARRAY without enableTransactions=true, because a partially sent "
                + "batch cannot be retried without duplicating records. Enable transactions to use producer "
                + "retry for batches.";

        String started = capture(() -> assertStarts(e));
        assertInOrder(started, START_UP_LINE, warning);

        e.setProducerRetryDelaySeconds(0);
        String rejected = capture(() -> assertRejected(e, "Please set producerRetryDelaySeconds between 1 and 30 "
                + "(configured value: 0). A delay below one second cannot help against a disconnected broker "
                + "node — the Kafka client has already backed off exponentially before giving up — and only "
                + "adds load."));
        Assert.assertFalse(rejected, rejected.contains("has no effect for producerBatchMode"));
    }

    private CpiKafkaPlusEndpoint endpoint() throws Exception {
        return (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:orders?bootstrapServers=localhost:9999&securityProtocol=PLAINTEXT");
    }

    /** Each of {@code lines} is in the log, in this order. */
    private static void assertInOrder(String logged, String... lines) {
        int previous = -1;
        for (String line : lines) {
            int at = logged.indexOf(line, previous + 1);
            Assert.assertTrue("missing after position " + previous + ": " + line + "\nin:\n" + logged, at >= 0);
            previous = at;
        }
    }

    private static void assertRejected(CpiKafkaPlusEndpoint endpoint, String message) throws Exception {
        Assert.assertEquals(message, rejection(endpoint));
    }

    /** The message the start is rejected with. */
    private static String rejection(CpiKafkaPlusEndpoint endpoint) throws Exception {
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        try {
            producer.doStart();
            Assert.fail("expected the deployment to be rejected");
            return null;
        } catch (IllegalArgumentException expected) {
            return expected.getMessage();
        } finally {
            try {
                producer.doStop();
            } catch (Exception ignored) {
                // start failed, so stop may too — not what is under test
            }
        }
    }

    private static void assertStarts(CpiKafkaPlusEndpoint endpoint) throws Exception {
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        try {
            producer.doStart();
        } finally {
            producer.doStop();
        }
    }

    private static String repeat(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

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
}
