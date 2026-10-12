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

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The order of the sender's start-up checks and the lines it logs between them. With more than one
 * wrong value the order decides which message the operator sees first; the {@code consumer.config.*}
 * lines in between have to reach the trace even when a later check rejects the deployment. Every
 * check runs before {@code super.doStart()}, after the endpoint's shared checks.
 */
public class ConsumerStartOrderTest {

    private static final String CONSUMER = CpiKafkaPlusConsumer.class.getName();

    private DefaultCamelContext ctx;
    private int endpointCount;

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
     * rejected by the next check below, and the last one starts. Values two checks look at (the DLQ
     * topic, the drain backlog, the polling interval) move from one wrong value to the next.
     */
    @Test
    public void eachCheckRejectsOnlyOnceTheEarlierOnesPass() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setDiagnosticsLevel("VERBOSE");
        e.setConsumptionMode("BATCH");
        e.setErrorHandling("DROP");
        e.setDlqEnabled(false);   // Error Handling is checked only without a DLQ; switched on below
        e.setDlqTopic(" ");
        e.setGroupId(" ");
        e.setCommitStrategy("MANUAL");
        e.setMaxPollRecords(0);
        e.setFetchMinBytes(-1);
        e.setFetchMaxWaitMs(-1);
        e.setBatchTimeout(-1);
        e.setBatchMode(true);
        e.setBatchOutputFormat("JSON_ARRAY");
        e.setBatchSize(0);
        e.setDlqMaxRetries(-1);
        e.setDrainEnabled(true);
        e.setMinBacklogToDrain(-1);
        e.setSchemaRegistryEnabled(true);
        e.setSchemaRegistryUrl("ftp://sr.example.com");
        e.setMaxPartitionFetchSizeKb(0);
        e.setRetryDelaySeconds(999);
        e.setPollingIntervalSeconds(0);

        // CpiKafkaPlusEndpoint.validateConfiguration(), shared with the receiver, comes first
        assertRejected(e, "'Diagnostics Level' (diagnosticsLevel) must be STANDARD (default) or FULL, got: "
                + "'VERBOSE'");
        e.setDiagnosticsLevel("STANDARD");
        assertRejected(e, "'Consumption Mode' (consumptionMode) must be SCHEDULED or STREAMING, got: 'BATCH'");
        e.setConsumptionMode("SCHEDULED");
        assertRejected(e, "'Error Handling' (errorHandling) must be RETRY (Retry Failed Message) or SKIP "
                + "(Skip Failed Message), got: 'DROP'");
        e.setErrorHandling("RETRY");
        e.setDlqEnabled(true);
        assertRejected(e, "'Dead Letter Topic' (dlqTopic) must not be empty while 'Enable Dead Letter Queue' "
                + "(dlqEnabled) is on, got: ' '");
        e.setDlqTopic(" orders-dlq ");
        assertRejected(e, "'Consumer Group ID' (groupId) must not be empty, got: ' '. The consumer cannot join a "
                + "consumer group without it.");
        e.setGroupId("g");
        assertRejected(e, "'Offset Commit Strategy' (commitStrategy) must be BATCH_COMPLETE or AUTO, got: "
                + "'MANUAL'. Any other value never commits an offset, so every message would be delivered "
                + "again on every restart and rebalance.");
        e.setCommitStrategy("AUTO");
        assertRejected(e, "'Max Poll Records' (maxPollRecords) must be at least 1, got: 0");
        e.setMaxPollRecords(500);
        assertRejected(e, "'Fetch Min Bytes' (fetchMinBytes) must not be negative, got: -1");
        e.setFetchMinBytes(1);
        assertRejected(e, "'Fetch Max Wait (ms)' (fetchMaxWaitMs) must not be negative, got: -1");
        e.setFetchMaxWaitMs(500);
        assertRejected(e, "'Poll Timeout (ms)' (batchTimeout) must not be negative, got: -1");
        e.setBatchTimeout(5000);
        assertRejected(e, "'Max Records per IFlow Run (MPL)' (batchSize) must be at least 1 in batch mode, "
                + "got: 0");
        e.setBatchSize(100);
        assertRejected(e, "'Max Retries before DLQ' (dlqMaxRetries) must not be negative, got: -1");
        e.setDlqMaxRetries(3);
        assertRejected(e, "'Dead Letter Topic' (dlqTopic) must not start or end with a space, got: "
                + "' orders-dlq '. Please remove the spaces around the topic name.");
        e.setDlqTopic("orders dlq");
        assertRejected(e, "'Dead Letter Topic' (dlqTopic) must be a valid Kafka topic name, got: 'orders dlq'. "
                + "Kafka allows 1 to 249 characters from a-z, A-Z, 0-9, '.', '_' and '-', but not '.' or '..' "
                + "alone.");
        e.setDlqTopic("orders");
        assertRejected(e, "'Dead Letter Topic' (dlqTopic) must not be a topic this channel consumes, got: "
                + "'orders'. A failed record would be dead-lettered into its own source and consumed again, "
                + "endlessly. Please use a separate DLQ topic.");
        e.setDlqTopic("orders-dlq");
        assertRejected(e, "'Schema Registry URL' (schemaRegistryUrl) must be an http or https URL, got: "
                + "'ftp://sr.example.com'. Please start it with https:// or http://.");
        e.setSchemaRegistryUrl("https://sr.example.com");
        assertRejected(e, "'Offset Commit Strategy' (commitStrategy) must be BATCH_COMPLETE while 'Drain "
                + "Backlog' (drainEnabled) is on, got: 'AUTO'. AUTO commit cannot guarantee at-least-once "
                + "delivery during drain loops.");
        e.setCommitStrategy("BATCH_COMPLETE");
        assertRejected(e, "'Min Backlog to Drain' (minBacklogToDrain) must not be negative, got: -1");
        e.setMinBacklogToDrain(1000);
        assertRejected(e, "'Min Backlog to Drain' (minBacklogToDrain) must be at most 500, the value of 'Max "
                + "Poll Records' (maxPollRecords), got: 1000. A higher value would disable drain after every "
                + "poll.");
        e.setMinBacklogToDrain(10);
        assertRejected(e, "'Max Fetch Size per Partition (KB)' (maxPartitionFetchSizeKb) must be between 1 and "
                + "51200 (50 MB), got: 0");
        e.setMaxPartitionFetchSizeKb(1024);
        assertRejected(e, "'Retry Delay (Seconds)' (retryDelaySeconds) must be between 0 and 300, got: 999");
        e.setRetryDelaySeconds(5);
        assertRejected(e, "'Polling Interval (Seconds)' (pollingIntervalSeconds) must be at least 1, got: 0");
        e.setPollingIntervalSeconds(21601);
        assertRejected(e, "'Polling Interval (Seconds)' (pollingIntervalSeconds) must be at most 21600 (6 h), "
                + "got: 21601. Higher values exceed the max.poll.interval.ms hard cap (6 h 10 min) and would "
                + "cause the consumer to leave the group between polls.");
        e.setPollingIntervalSeconds(60);
        assertStarts(e);
    }

    @Test
    public void anUnknownBatchFormatIsLoggedEvenWhenTheDrainCheckRejects() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setBatchMode(true);
        e.setBatchOutputFormat("CSV");
        e.setDrainEnabled(true);
        e.setCommitStrategy("AUTO");

        String logged = capture(() -> assertRejected(e, "'Offset Commit Strategy' (commitStrategy) must be "
                + "BATCH_COMPLETE while 'Drain Backlog' (drainEnabled) is on, got: 'AUTO'. AUTO commit cannot "
                + "guarantee at-least-once delivery during drain loops."));

        assertLoggedBeforeTheRejection(logged, "ERROR " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] "
                + "consumer.config.ineffective parameter=batchOutputFormat value=CSV reason='unknown value; "
                + "batches are formatted as JSON_ARRAY'");
    }

    @Test
    public void anUnknownAvroOutputFormatIsLoggedEvenWhenTheRetryDelayCheckRejects() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setSchemaRegistryEnabled(true);
        e.setSchemaRegistryUrl("http://localhost:8081");
        e.setAvroValueDeserialization(true);
        e.setAvroOutputFormat("CSV");
        e.setRetryDelaySeconds(500);

        String logged = capture(() -> assertRejected(e, "'Retry Delay (Seconds)' (retryDelaySeconds) must be "
                + "between 0 and 300, got: 500"));

        assertLoggedBeforeTheRejection(logged, "ERROR " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] "
                + "consumer.config.ineffective parameter=avroOutputFormat value=CSV reason='unknown value; "
                + "Avro records are converted to JSON'");
    }

    @Test
    public void clampedAutoPauseValuesAreLoggedEvenWhenThePollingIntervalCheckRejects() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setAutoPauseEnabled(true);
        e.setAutoPauseErrorThreshold(0);
        e.setPollingIntervalSeconds(0);

        String logged = capture(() -> assertRejected(e, "'Polling Interval (Seconds)' (pollingIntervalSeconds) "
                + "must be at least 1, got: 0"));

        assertLoggedBeforeTheRejection(logged, "ERROR " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] "
                + "consumer.config.clamped autoPauseErrorThreshold=0 autoPauseCooldownSeconds=60 "
                + "reason='values below 1 are used as 1'");
    }

    /** The {@code consumer.config.*} lines come after the checks of values that can never work. */
    @Test
    public void aValueThatCanNeverWorkIsRejectedBeforeTheConfigLines() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setBatchMode(true);
        e.setBatchOutputFormat("CSV");
        e.setAutoPauseEnabled(true);
        e.setAutoPauseErrorThreshold(0);
        e.setDlqEnabled(true);
        e.setDlqTopic("orders");

        String logged = capture(() -> assertRejected(e, "'Dead Letter Topic' (dlqTopic) must not be a topic "
                + "this channel consumes, got: 'orders'. A failed record would be dead-lettered into its own "
                + "source and consumed again, endlessly. Please use a separate DLQ topic."));

        Assert.assertFalse(logged, logged.contains("consumer.config."));
    }

    /** Every check runs before {@code super.doStart()}: a rejected start logs no start-up line. */
    @Test
    public void theChecksRunBeforeTheStartUpLines() throws Exception {
        CpiKafkaPlusEndpoint rejected = endpoint();
        rejected.setPollingIntervalSeconds(21601);
        String rejectedLog = capture(() -> assertRejected(rejected, "'Polling Interval (Seconds)' "
                + "(pollingIntervalSeconds) must be at most 21600 (6 h), got: 21601. Higher values exceed the "
                + "max.poll.interval.ms hard cap (6 h 10 min) and would cause the consumer to leave the group "
                + "between polls."));
        Assert.assertTrue(rejectedLog, rejectedLog.contains("consumer.start.failed"));
        Assert.assertFalse(rejectedLog, rejectedLog.contains("doStart called"));

        CpiKafkaPlusEndpoint started = endpoint();
        started.setBatchMode(true);
        started.setBatchOutputFormat("CSV");
        String startedLog = capture(() -> assertStarts(started));
        int configLine = startedLog.indexOf("consumer.config.ineffective parameter=batchOutputFormat");
        int startLine = startedLog.indexOf("ERROR " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] doStart called — "
                + "Consumer for topic='orders' group='g");
        Assert.assertTrue(startedLog, configLine >= 0 && startLine > configLine);
    }

    /** The state of the previous run is reset before the checks, so a rejected restart starts clean. */
    @Test
    public void aRejectedRestartStillClearsTheStopRequest() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(e, exchange -> { });
        consumer.doStart();
        consumer.doStop();
        Assert.assertTrue(consumer.getConsumerCallback().isStopRequested());

        e.setMaxPollRecords(0);
        try {
            consumer.doStart();
            Assert.fail("expected the restart to be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertEquals("'Max Poll Records' (maxPollRecords) must be at least 1, got: 0",
                    expected.getMessage());
        }
        Assert.assertFalse(consumer.getConsumerCallback().isStopRequested());
        consumer.doStop();
    }

    /** {@code wanted} is in the log, and before the {@code consumer.start.failed} line. */
    private static void assertLoggedBeforeTheRejection(String logged, String wanted) {
        int line = logged.indexOf(wanted);
        Assert.assertTrue("missing: " + wanted + "\nin:\n" + logged, line >= 0);
        Assert.assertTrue(logged, logged.indexOf("consumer.start.failed") > line);
    }

    /**
     * A fresh endpoint per call. Camel caches endpoints by URI, so a shared URI would hand every test
     * step the instance a previous step already modified.
     */
    private CpiKafkaPlusEndpoint endpoint() throws Exception {
        return (CpiKafkaPlusEndpoint) ctx.getEndpoint("cpi-kafka-plus:orders?bootstrapServers=localhost:9092"
                + "&groupId=g" + (++endpointCount) + "&securityProtocol=PLAINTEXT");
    }

    private static void assertRejected(CpiKafkaPlusEndpoint endpoint, String message) throws Exception {
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, exchange -> { });
        try {
            consumer.doStart();
            Assert.fail("expected the deployment to be rejected with: " + message);
        } catch (IllegalArgumentException expected) {
            Assert.assertEquals(message, expected.getMessage());
        } finally {
            consumer.doStop();
        }
    }

    private static void assertStarts(CpiKafkaPlusEndpoint endpoint) throws Exception {
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, exchange -> { });
        try {
            consumer.doStart();
        } finally {
            consumer.doStop();
        }
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
