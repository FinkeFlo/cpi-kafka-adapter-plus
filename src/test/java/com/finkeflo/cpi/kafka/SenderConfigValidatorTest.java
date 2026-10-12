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

import org.junit.Assert;
import org.junit.Test;

/**
 * {@link SenderConfigValidator} without a Camel context: the fallback lines log under the consumer's
 * category, also when a later check rejects the deployment, and STREAMING skips the checks that only
 * apply to SCHEDULED. The order of all checks is pinned in {@link ConsumerStartOrderTest}.
 */
public class SenderConfigValidatorTest {

    private static final String CONSUMER = CpiKafkaPlusConsumer.class.getName();
    private static final String AVRO_FALLBACK = "ERROR " + CONSUMER + " - [CPI-KAFKA-PLUS-DIAG] "
            + "consumer.config.ineffective parameter=avroOutputFormat value=YAML reason='unknown value; Avro "
            + "records are converted to JSON'";

    @Test
    public void theDefaultsPassWithoutALogLine() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();

        Assert.assertEquals("", capture(() -> SenderConfigValidator.validate(e)));
    }

    @Test
    public void aFallbackIsLoggedUnderTheConsumerCategory() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setSchemaRegistryEnabled(true);
        e.setAvroOutputFormat("YAML");

        assertLoggedOnce(capture(() -> SenderConfigValidator.validate(e)), AVRO_FALLBACK);
    }

    @Test
    public void aFallbackIsLoggedEvenWhenALaterCheckRejects() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setSchemaRegistryEnabled(true);
        e.setAvroOutputFormat("YAML");
        e.setRetryDelaySeconds(500);

        String logged = capture(() -> assertRejected(() -> SenderConfigValidator.validate(e),
                "'Retry Delay (Seconds)' (retryDelaySeconds) must be between 0 and 300, got: 500"));

        assertLoggedOnce(logged, AVRO_FALLBACK);
    }

    @Test
    public void aDlqWithoutATopicIsRejected() {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setDlqEnabled(true);
        e.setDlqTopic(null);
        assertRejected(() -> SenderConfigValidator.validate(e), "'Dead Letter Topic' (dlqTopic) must not be empty "
                + "while 'Enable Dead Letter Queue' (dlqEnabled) is on, got: ''");
        e.setDlqTopic(" ");
        assertRejected(() -> SenderConfigValidator.validate(e), "'Dead Letter Topic' (dlqTopic) must not be empty "
                + "while 'Enable Dead Letter Queue' (dlqEnabled) is on, got: ' '");
    }

    /** Every pause is capped at 900 s; a longer configured one used to be cut without a word (#255). */
    @Test
    public void aCooldownAboveTheCapIsLoggedAsClamped() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setAutoPauseEnabled(true);
        e.setAutoPauseCooldownSeconds(3600);

        assertLoggedOnce(capture(() -> SenderConfigValidator.validate(e)), "ERROR " + CONSUMER
                + " - [CPI-KAFKA-PLUS-DIAG] consumer.config.clamped autoPauseCooldownSeconds=3600 "
                + "reason='values above 900 are used as 900'");

        e.setAutoPauseCooldownSeconds(900);
        Assert.assertEquals("", capture(() -> SenderConfigValidator.validate(e)));

        e.setAutoPauseCooldownSeconds(3600);
        e.setAutoPauseEnabled(false);
        Assert.assertEquals("", capture(() -> SenderConfigValidator.validate(e)));
    }

    /** STREAMING ignores the drain fields and the polling interval, so stale values must not block it. */
    @Test
    public void streamingSkipsTheDrainAndPollingIntervalChecks() {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setConsumptionMode("STREAMING");
        e.setDrainEnabled(true);
        e.setCommitStrategy("AUTO");
        e.setPollingIntervalSeconds(0);

        SenderConfigValidator.validate(e);

        e.setConsumptionMode("SCHEDULED");
        assertRejected(() -> SenderConfigValidator.validate(e), "'Offset Commit Strategy' (commitStrategy) must be "
                + "BATCH_COMPLETE while 'Drain Backlog' (drainEnabled) is on, got: 'AUTO'. AUTO commit cannot "
                + "guarantee at-least-once delivery during drain loops.");
    }

    @Test(expected = IllegalArgumentException.class)
    public void validatePollingIntervalSeconds_zero_throws() {
        SenderConfigValidator.validatePollingIntervalSeconds(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void validatePollingIntervalSeconds_negative_throws() {
        SenderConfigValidator.validatePollingIntervalSeconds(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void validatePollingIntervalSeconds_above21600_throws() {
        SenderConfigValidator.validatePollingIntervalSeconds(21601);
    }

    @Test
    public void validatePollingIntervalSeconds_21600_doesNotThrow() {
        // 21600 (6 h) is the inclusive upper bound — must not throw.
        SenderConfigValidator.validatePollingIntervalSeconds(21600);
    }

    @Test
    public void validatePollingIntervalSeconds_3600_doesNotThrow() {
        // 60 min must still be accepted (was previously the upper bound).
        SenderConfigValidator.validatePollingIntervalSeconds(3600);
    }

    @Test
    public void validatePollingIntervalSeconds_5_doesNotThrow() {
        // Default value, must not throw.
        SenderConfigValidator.validatePollingIntervalSeconds(5);
    }

    private static CpiKafkaPlusEndpoint endpoint() {
        CpiKafkaPlusEndpoint e = new CpiKafkaPlusEndpoint();
        e.setTopic("orders");
        e.setGroupId("orders-group");
        return e;
    }

    private static void assertRejected(Runnable check, String message) {
        try {
            check.run();
            Assert.fail("expected the deployment to be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertEquals(message, expected.getMessage());
        }
    }

    /** The one line logged is {@code line}, with its level and logger category. */
    private static void assertLoggedOnce(String logged, String line) {
        Assert.assertEquals("[" + Thread.currentThread().getName() + "] " + line + "\n", logged);
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * The lines this thread logs while {@code action} runs. Clients left by earlier tests keep
     * logging from their own threads meanwhile.
     */
    private static String capture(ThrowingRunnable action) throws Exception {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, "UTF-8"));
        try {
            action.run();
        } finally {
            System.setErr(original);
        }
        String thisThread = "[" + Thread.currentThread().getName() + "] ";
        StringBuilder lines = new StringBuilder();
        for (String line : new String(captured.toByteArray(), StandardCharsets.UTF_8).split("\\R")) {
            if (line.startsWith(thisThread)) {
                lines.append(line).append('\n');
            }
        }
        return lines.toString();
    }
}
