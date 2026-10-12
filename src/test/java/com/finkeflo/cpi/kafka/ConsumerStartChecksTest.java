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
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Deployment-time checks of the sender: reject values that can never work, and never reject a
 * value in a field that has no effect with the current settings (#174).
 */
public class ConsumerStartChecksTest {

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

    @Test
    public void unknownErrorHandlingIsRejected() throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint();
        endpoint.setErrorHandling("DROP");

        assertRejected(endpoint, "errorHandling", "Error Handling");
    }

    @Test
    public void errorHandlingIsIgnoredWhileTheDlqIsOn() throws Exception {
        // Hidden with a DLQ and never used then; an externalized stale value must not block (#264).
        CpiKafkaPlusEndpoint unknown = endpoint();
        unknown.setDlqEnabled(true);
        unknown.setDlqTopic("orders-dlq");
        unknown.setErrorHandling("DROP");
        assertStarts(unknown);
        CpiKafkaPlusEndpoint missing = endpoint();
        missing.setDlqEnabled(true);
        missing.setDlqTopic("orders-dlq");
        missing.setErrorHandling(null);
        assertStarts(missing);
    }

    @Test
    public void retryDelayIsCheckedWhenFailedMessagesAreRetried() throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint();
        endpoint.setRetryDelaySeconds(999);

        String message = startFailure(endpoint);

        Assert.assertTrue(message, message.contains("retryDelaySeconds"));
    }

    @Test
    public void retryDelayIsIgnoredWhenFailedMessagesAreSkippedWithoutDlq() throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint();
        endpoint.setErrorHandling("SKIP");
        endpoint.setRetryDelaySeconds(999);

        assertStarts(endpoint);
    }

    @Test
    public void drainChecksAreIgnoredWhileDrainIsOff() throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint();
        endpoint.setDrainEnabled(false);
        endpoint.setMaxPollRecords(500);
        endpoint.setMinBacklogToDrain(1000);

        assertStarts(endpoint);
    }

    @Test
    public void drainChecksApplyWhileDrainIsOn() throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint();
        endpoint.setDrainEnabled(true);
        endpoint.setMaxPollRecords(500);
        endpoint.setMinBacklogToDrain(1000);

        String message = startFailure(endpoint);

        Assert.assertTrue(message, message.contains("minBacklogToDrain"));
    }

    @Test
    public void emptyGroupIdIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setGroupId(" ");
        assertRejected(e, "groupId", "Consumer Group ID");
    }

    @Test
    public void missingGroupIdModeAndErrorHandlingAreRejected() throws Exception {
        // A sender channel saved without a group id leaves the field null, not empty.
        CpiKafkaPlusEndpoint group = endpoint();
        group.setGroupId(null);
        assertRejected(group, "groupId", "Consumer Group ID");
        CpiKafkaPlusEndpoint mode = endpoint();
        mode.setConsumptionMode(null);
        Assert.assertEquals("'Consumption Mode' (consumptionMode) must be SCHEDULED or STREAMING, got: ''",
                startFailure(mode));
        CpiKafkaPlusEndpoint handling = endpoint();
        handling.setErrorHandling(null);
        Assert.assertEquals("'Error Handling' (errorHandling) must be RETRY (Retry Failed Message) or SKIP "
                + "(Skip Failed Message), got: ''", startFailure(handling));
    }

    @Test
    public void unknownCommitStrategyIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setCommitStrategy("MANUAL");
        assertRejected(e, "commitStrategy", "Offset Commit Strategy");
    }

    @Test
    public void commitStrategyIsCaseInsensitiveAsAtRuntime() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setCommitStrategy("auto");
        assertStarts(e);
    }

    @Test
    public void maxPollRecordsBelowOneIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setMaxPollRecords(0);
        assertRejected(e, "maxPollRecords", "Max Poll Records");
    }

    @Test
    public void negativeFetchValuesAreRejected() throws Exception {
        CpiKafkaPlusEndpoint a = endpoint();
        a.setFetchMinBytes(-1);
        assertRejected(a, "fetchMinBytes", "Fetch Min Bytes");
        CpiKafkaPlusEndpoint b = endpoint();
        b.setFetchMaxWaitMs(-1);
        assertRejected(b, "fetchMaxWaitMs", "Fetch Max Wait (ms)");
    }

    @Test
    public void negativeBatchTimeoutIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setBatchTimeout(-1);
        assertRejected(e, "batchTimeout", "Poll Timeout (ms)");
    }

    @Test
    public void batchSizeBelowOneIsRejectedInBatchMode() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setBatchMode(true);
        e.setBatchOutputFormat("JSON_ARRAY");
        e.setBatchSize(0);
        assertRejected(e, "batchSize", "Max Records per IFlow Run (MPL)");
    }

    @Test
    public void batchSizeIsIgnoredOutsideTheBatchPath() throws Exception {
        CpiKafkaPlusEndpoint single = endpoint();
        single.setBatchMode(false);
        single.setBatchSize(0);
        assertStarts(single);
        // Legacy 1.0/1.1 sender: SPLIT_EXCHANGES bypasses the batch path.
        CpiKafkaPlusEndpoint legacy = endpoint();
        legacy.setBatchMode(true);
        legacy.setBatchOutputFormat("SPLIT_EXCHANGES");
        legacy.setBatchSize(0);
        assertStarts(legacy);
    }

    @Test
    public void negativeDlqRetriesAreRejectedOnlyWithDlq() throws Exception {
        CpiKafkaPlusEndpoint withDlq = endpoint();
        withDlq.setDlqEnabled(true);
        withDlq.setDlqTopic("orders-dlq");
        withDlq.setDlqMaxRetries(-1);
        assertRejected(withDlq, "dlqMaxRetries", "Max Retries before DLQ");
        CpiKafkaPlusEndpoint withoutDlq = endpoint();
        withoutDlq.setDlqMaxRetries(-1);
        assertStarts(withoutDlq);
    }

    @Test
    public void dlqTopicThatIsAlsoASourceTopicIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setDlqEnabled(true);
        e.setDlqTopic("orders");
        assertRejected(e, "dlqTopic", "Dead Letter Topic");
    }

    @Test
    public void dlqTopicWithSurroundingWhitespaceIsRejectedAsSuch() throws Exception {
        // The DLQ producer sends to the untrimmed name, which Kafka rejects as an invalid topic.
        CpiKafkaPlusEndpoint e = endpoint();
        e.setDlqEnabled(true);
        e.setDlqTopic(" orders-dlq ");
        String message = startFailure(e);
        Assert.assertTrue(message, message.contains("must not start or end with a space, got: ' orders-dlq '"));
        Assert.assertTrue(message, message.contains("'Dead Letter Topic'"));
    }

    @Test
    public void aDlqTopicKafkaRefusesIsRejected() throws Exception {
        // Each deployed and failed at the first dead letter, when the record had nowhere else to go (#261).
        for (String value : new String[] {"orders dlq", "orders/dlq", "orders:dlq", "orders-dlq-ä", ".", "..",
                repeat('x', 250)}) {
            CpiKafkaPlusEndpoint e = endpoint();
            e.setDlqEnabled(true);
            e.setDlqTopic(value);
            String message = startFailure(e);
            Assert.assertTrue(message, message.startsWith("'Dead Letter Topic' (dlqTopic) must be a valid Kafka "
                    + "topic name, got: '" + value + "'"));
        }
    }

    @Test
    public void theDlqTopicCheckAgreesWithKafka() throws Exception {
        for (String value : new String[] {"orders-dlq", "orders.dlq", "orders_dlq", "ORDERS-DLQ", "a", "...",
                repeat('x', 249), repeat('x', 250), "orders dlq", ".", "..", "orders/dlq", "orders:dlq", "orders-ä"}) {
            boolean kafkaTakesIt;
            try {
                Topic.validate(value);
                kafkaTakesIt = true;
            } catch (InvalidTopicException e) {
                kafkaTakesIt = false;
            }
            CpiKafkaPlusEndpoint e = endpoint();
            e.setDlqEnabled(true);
            e.setDlqTopic(value);
            CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(e, exchange -> { });
            boolean starts;
            try {
                consumer.doStart();
                starts = true;
            } catch (IllegalArgumentException rejected) {
                starts = false;
            } finally {
                consumer.doStop();
            }
            Assert.assertEquals(value, kafkaTakesIt, starts);
        }
    }

    @Test
    public void aSchemaRegistryUrlWithoutHttpIsRejectedWithAvroDeserialization() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setSchemaRegistryEnabled(true);
        e.setSchemaRegistryUrl("sr.example.com:8081");
        assertRejected(e, "schemaRegistryUrl", "Schema Registry URL");

        CpiKafkaPlusEndpoint off = endpoint();
        off.setSchemaRegistryEnabled(true);
        off.setSchemaRegistryUrl("sr.example.com:8081");
        off.setAvroValueDeserialization(false);
        assertStarts(off);

        CpiKafkaPlusEndpoint upperCase = endpoint();
        upperCase.setSchemaRegistryEnabled(true);
        upperCase.setSchemaRegistryUrl("HTTPS://sr:8081");
        assertStarts(upperCase);
    }

    @Test
    public void bootstrapServersKafkaCannotReadAreRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setBootstrapServers("broker1");
        String message = startFailure(e);
        Assert.assertEquals("'Bootstrap Servers' (bootstrapServers) must be a comma-separated list of host:port "
                + "addresses, got: 'broker1'. Kafka cannot read 'broker1' as host:port.", message);
    }

    @Test
    public void dlqTopicComparisonIsCaseSensitiveLikeKafka() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setDlqEnabled(true);
        e.setDlqTopic("Orders");
        assertStarts(e);
    }

    @Test
    public void unknownOutputFormatsAndBadAutoPauseValuesStillStart() throws Exception {
        // Each has a working fallback today: ERROR log, no rejection.
        CpiKafkaPlusEndpoint e = endpoint();
        e.setBatchMode(true);
        e.setBatchOutputFormat("CSV");
        e.setAutoPauseEnabled(true);
        e.setAutoPauseErrorThreshold(0);
        e.setAutoPauseCooldownSeconds(0);
        assertStarts(e);
    }

    @Test
    public void fallbackLinesAreLoggedOnlyWhereTheValueIsInEffect() throws Exception {
        CpiKafkaPlusEndpoint known = endpoint();
        known.setSchemaRegistryEnabled(true);
        known.setSchemaRegistryUrl("http://localhost:8081");
        known.setAvroOutputFormat("xml");   // case-insensitive, like the conversion
        known.setBatchOutputFormat("XML_LIST");
        known.setAutoPauseEnabled(true);
        known.setAutoPauseErrorThreshold(1);
        known.setAutoPauseCooldownSeconds(1);
        String knownLog = capture(() -> assertStarts(known));
        Assert.assertFalse(knownLog, knownLog.contains("consumer.config."));

        CpiKafkaPlusEndpoint notInEffect = endpoint();
        notInEffect.setSchemaRegistryEnabled(true);
        notInEffect.setSchemaRegistryUrl("http://localhost:8081");
        notInEffect.setAvroValueDeserialization(false);
        notInEffect.setAvroOutputFormat("CSV");
        notInEffect.setBatchOutputFormat("SPLIT_EXCHANGES");   // legacy path, nothing is formatted
        notInEffect.setAutoPauseEnabled(false);
        notInEffect.setAutoPauseErrorThreshold(0);
        String notInEffectLog = capture(() -> assertStarts(notInEffect));
        Assert.assertFalse(notInEffectLog, notInEffectLog.contains("consumer.config."));

        CpiKafkaPlusEndpoint cooldown = endpoint();
        cooldown.setAutoPauseEnabled(true);
        cooldown.setAutoPauseCooldownSeconds(0);
        String cooldownLog = capture(() -> assertStarts(cooldown));
        Assert.assertTrue(cooldownLog, cooldownLog.contains("ERROR " + CpiKafkaPlusConsumer.class.getName()
                + " - [CPI-KAFKA-PLUS-DIAG] consumer.config.clamped autoPauseErrorThreshold=5 "
                + "autoPauseCooldownSeconds=0 reason='values below 1 are used as 1'"));
    }

    /** The message names both the UI label and the parameter, so it is findable from either side. */
    private void assertRejected(CpiKafkaPlusEndpoint endpoint, String field, String uiLabel) throws Exception {
        String message = startFailure(endpoint);
        Assert.assertTrue(message, message.contains(field));
        Assert.assertTrue(message, message.contains("'" + uiLabel + "'"));
    }

    @Test
    public void emptyBootstrapServersAreRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setBootstrapServers("");
        String message = startFailure(e);
        Assert.assertTrue(message, message.contains("bootstrapServers"));
        Assert.assertTrue(message, message.contains("'Bootstrap Servers'"));
    }

    @Test
    public void malformedJsonSchemaIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setJsonSchemaValidation(true);
        e.setJsonSchema("{\"type\": \"object\"} {\"type\": \"string\"}");
        String message = startFailure(e);
        Assert.assertTrue(message, message.contains("'JSON Schema' (jsonSchema)"));
    }

    private int endpointCount;

    /**
     * A fresh endpoint per call. Camel caches endpoints by URI, so a shared URI would hand every test
     * step the instance a previous step already modified.
     */
    private CpiKafkaPlusEndpoint endpoint() throws Exception {
        return (CpiKafkaPlusEndpoint) ctx.getEndpoint("cpi-kafka-plus:orders?bootstrapServers=localhost:9092"
                + "&groupId=g" + (++endpointCount) + "&securityProtocol=PLAINTEXT");
    }

    private static String startFailure(CpiKafkaPlusEndpoint endpoint) throws Exception {
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, exchange -> { });
        try {
            consumer.doStart();
            Assert.fail("expected the deployment to be rejected");
            return null;
        } catch (IllegalArgumentException expected) {
            return expected.getMessage();
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

    private static String repeat(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
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
