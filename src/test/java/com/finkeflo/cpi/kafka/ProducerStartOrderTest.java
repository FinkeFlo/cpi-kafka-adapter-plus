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
        e.setBootstrapServers("localhost:9999,broker2");
        e.setDiagnosticsLevel("VERBOSE");
        e.setProducerBatchMode("CSV");
        e.setDeliveryTimeoutSeconds(0);
        e.setMaxRequestSizeKb(0);
        e.setProducerBatchSizeKb(-1);
        e.setBufferMemoryKb(0);
        e.setSchemaRegistryEnabled(true);
        e.setSchemaRegistryUrl("localhost:8081");
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
        assertRejected(e, "'Bootstrap Servers' (bootstrapServers) must be a comma-separated list of host:port "
                + "addresses, got: 'localhost:9999,broker2'. Kafka cannot read 'broker2' as host:port.");
        e.setBootstrapServers("localhost:9999");
        assertRejected(e, "'Diagnostics Level' (diagnosticsLevel) must be STANDARD (default) or FULL, got: "
                + "'VERBOSE'");
        e.setDiagnosticsLevel("STANDARD");
        assertRejected(e, "'Batch Send Mode' (producerBatchMode) must be NONE, JSON_ARRAY or XML_LIST, got: "
                + "'CSV'");
        e.setProducerBatchMode("JSON_ARRAY");
        assertRejected(e, "'Delivery Timeout (Seconds)' (deliveryTimeoutSeconds) must be at least 1, got: 0. "
                + "With 0 every send times out before it is sent.");
        e.setDeliveryTimeoutSeconds(871);
        assertRejected(e, "'Max Request Size (KB)' (maxRequestSizeKb) must be at least 1, got: 0. No message "
                + "fits into a smaller request.");
        e.setMaxRequestSizeKb(1);
        assertRejected(e, "'Producer Batch Size (KB)' (producerBatchSizeKb) must be at least 0, got: -1. 0 "
                + "switches batching off.");
        e.setProducerBatchSizeKb(0);
        assertRejected(e, "'Buffer Memory (KB)' (bufferMemoryKb) must be at least 1, got: 0. No message fits "
                + "into a smaller buffer.");
        e.setBufferMemoryKb(1);
        assertRejected(e, "'Schema Registry URL' (schemaRegistryUrl) must be an http or https URL, got: "
                + "'localhost:8081'. Please start it with https:// or http://.");
        e.setSchemaRegistryUrl("http://localhost:8081");
        assertRejected(e, "'Subject Name Strategy' (subjectNameStrategy) must be TopicNameStrategy while "
                + "'Serialize Values as Avro' (avroValueSerialization) is on, got: 'RecordNameStrategy'. The "
                + "subject cannot be resolved from JSON input without the record name.");
        e.setSubjectNameStrategy("TopicNameStrategy");
        // after super.doStart() and the start-up line
        assertRejected(e, "'Transactional ID Prefix' (transactionalIdPrefix) must not be empty while 'Enable "
                + "Transactions' (enableTransactions) is on, got: ' '");
        e.setTransactionalIdPrefix(repeat('x', 240));
        assertRejected(e, "'Enable Idempotent Producer' (enableIdempotence) must be on while 'Enable "
                + "Transactions' (enableTransactions) is on, got: false. Kafka allows transactions only with "
                + "idempotence.");
        e.setEnableIdempotence(true);
        assertRejected(e, "'Delivery Timeout (Seconds)' (deliveryTimeoutSeconds) must be at most 870 while "
                + "'Enable Transactions' (enableTransactions) is on, got: 871. The adapter derives "
                + "transaction.timeout.ms from it, and a broker rejects a producer asking for more than "
                + "transaction.max.timeout.ms (15 minutes by default).");
        e.setDeliveryTimeoutSeconds(2);
        assertRejected(e, "'Max Concurrent Transactions' (maxConcurrentTransactions) must be at least 1 while "
                + "'Enable Transactions' (enableTransactions) is on, got: 0. With no transaction slot the adapter "
                + "could not send anything.");
        e.setMaxConcurrentTransactions(1);
        // TransactionalProducerPool.start(); the member suffix depends on the node
        String tooLong = rejection(e);
        Assert.assertTrue(tooLong, tooLong.matches("'Transactional ID Prefix' \\(transactionalIdPrefix\\) must be "
                + "at most \\d+ characters long, got: 'x{240}' \\(240 characters\\)\\. With 1 for 'Max Concurrent "
                + "Transactions' \\(maxConcurrentTransactions\\), the transactional\\.id of the last slot, "
                + "'x{240}-[0-9a-f]{8}-[^']+-0', would have \\d+ "
                + "characters, and the adapter allows at most 249\\. Please shorten the prefix by at least \\d+ "
                + "characters?\\."));
        e.setTransactionalIdPrefix("orders-tx");
        assertRejected(e, "'Max Send Attempts' (producerRetryMaxAttempts) must be between 1 and 5, got: 6. This "
                + "is the total number of attempts, not the number of additional ones; 1 switches the retry off.");
        e.setProducerRetryMaxAttempts(2);
        assertRejected(e, "'Delay Between Attempts (Seconds)' (producerRetryDelaySeconds) must be between 1 and "
                + "30, got: 0. A delay below one second cannot help against a disconnected broker node — the "
                + "Kafka client has already backed off exponentially before giving up — and only adds load.");
        e.setProducerRetryDelaySeconds(2);
        assertRejected(e, "'Total Retry Budget (Seconds)' (producerRetryTotalBudgetSeconds) must be between 5 "
                + "and 900, got: 901");
        e.setProducerRetryTotalBudgetSeconds(5);
        assertRejected(e, "'Total Retry Budget (Seconds)' (producerRetryTotalBudgetSeconds) must cover the worst "
                + "case of the configured retry, 28 s, got: 5. Lower 'Delivery Timeout (Seconds)' "
                + "(deliveryTimeoutSeconds) (currently 2) or 'Max Send Attempts' (producerRetryMaxAttempts), or "
                + "raise the budget. The budget must stay below the timeout of "
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

        String logged = capture(() -> assertRejected(e, "'Transactional ID Prefix' (transactionalIdPrefix) must "
                + "not be empty while 'Enable Transactions' (enableTransactions) is on, got: ''"));

        assertInOrder(logged, START_UP_LINE, "producer.start.failed");
    }

    @Test
    public void ineffectiveTransactionsAreLoggedEvenWhenTheRetryCheckRejects() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setEnableTransactions(true);
        e.setProducerRetryMaxAttempts(6);

        String logged = capture(() -> assertRejected(e, "'Max Send Attempts' (producerRetryMaxAttempts) must be "
                + "between 1 and 5, got: 6. This is the total number of attempts, not the number of additional "
                + "ones; 1 switches the retry off."));

        assertInOrder(logged, START_UP_LINE,
                "ERROR " + PRODUCER + " - [CPI-KAFKA-PLUS-DIAG] producer.config.ineffective "
                        + "parameter=enableTransactions producerBatchMode=NONE reason='transactions apply to "
                        + "batches only; single messages are sent by the shared, non-transactional producer'",
                "producer.start.failed");
    }

    @Test
    public void theSingleSlotLineIsLoggedEvenWhenTheWorstCaseCheckRejects() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("JSON_ARRAY");
        e.setEnableTransactions(true);
        e.setTransactionalIdPrefix("orders-tx");
        e.setMaxConcurrentTransactions(1);
        e.setProducerRetryMaxAttempts(2);

        String logged = capture(() -> assertRejected(e, "'Total Retry Budget (Seconds)' "
                + "(producerRetryTotalBudgetSeconds) must cover the worst case of the configured retry, 432 s, "
                + "got: 30. Lower 'Delivery Timeout (Seconds)' (deliveryTimeoutSeconds) (currently 120) or 'Max "
                + "Send Attempts' (producerRetryMaxAttempts), or raise the budget. The budget must stay below the "
                + "timeout of the calling HTTP client. Note that one attempt costs roughly four times "
                + "deliveryTimeoutSeconds on the transactional path, because initTransactions(), the metadata "
                + "block inside send() and commitTransaction() are each bounded by max.block.ms = min(30s, "
                + "deliveryTimeoutSeconds) on top of the wait for acknowledgements."));

        assertInOrder(logged, START_UP_LINE,
                "INFO " + PRODUCER + " - [CPI-KAFKA-PLUS-DIAG] Transactional batching enabled with max 1 "
                        + "concurrent transactions.",
                "ERROR " + PRODUCER + " - [CPI-KAFKA-PLUS-DIAG] producer.config.ineffective "
                        + "parameter=producerRetryMaxAttempts value=2 maxConcurrentTransactions=1 reason='a retry "
                        + "holds the single transaction slot for up to producerRetryTotalBudgetSeconds=30s and "
                        + "blocks every other send on this node meanwhile; raise maxConcurrentTransactions'",
                "producer.start.failed");
    }

    /**
     * Without transactions a batch is never retried: the start says so and skips the worst case, here
     * with a budget it could never meet. The ranges are still checked first.
     */
    @Test
    public void aBatchWithoutTransactionsLogsAfterTheRangesAndSkipsTheWorstCase() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("JSON_ARRAY");
        e.setProducerRetryMaxAttempts(2);
        e.setProducerRetryTotalBudgetSeconds(5);
        String ineffective = "ERROR " + PRODUCER + " - [CPI-KAFKA-PLUS-DIAG] producer.config.ineffective "
                + "parameter=producerRetryMaxAttempts value=2 producerBatchMode=JSON_ARRAY reason='a partially "
                + "sent batch cannot be retried without duplicating records; switch on enableTransactions to retry "
                + "batches'";

        String started = capture(() -> assertStarts(e));
        assertInOrder(started, START_UP_LINE, ineffective);

        e.setProducerRetryDelaySeconds(0);
        String rejected = capture(() -> assertRejected(e, "'Delay Between Attempts (Seconds)' "
                + "(producerRetryDelaySeconds) must be between 1 and 30, got: 0. A delay below one second cannot "
                + "help against a disconnected broker node — the Kafka client has already backed off "
                + "exponentially before giving up — and only adds load."));
        Assert.assertFalse(rejected, rejected.contains("parameter=producerRetryMaxAttempts"));
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
