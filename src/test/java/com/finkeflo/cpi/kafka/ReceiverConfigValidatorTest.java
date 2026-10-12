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

import org.junit.Assert;
import org.junit.Test;

/**
 * {@link ReceiverConfigValidator} without a Camel context: which phase checks which settings, and
 * that its lines log under the producer's category. The order of the checks across the phases, and
 * the lines the producer logs between them, are pinned in {@link ProducerStartOrderTest}.
 */
public class ReceiverConfigValidatorTest {

    private static final String PRODUCER = CpiKafkaPlusProducer.class.getName();

    @Test
    public void theDefaultsPassEveryPhaseWithoutALogLine() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();

        String logged = capture(() -> {
            ReceiverConfigValidator.validateBeforeStart(e);
            ReceiverConfigValidator.validateTransactional(e);
            ReceiverConfigValidator.validateRetry(e);
        });

        Assert.assertEquals("", logged);
    }

    @Test
    public void theFirstPhaseChecksTheValuesThatCanNeverWork() {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("CSV");

        ReceiverConfigValidator.validateTransactional(e);
        ReceiverConfigValidator.validateRetry(e);
        assertRejected(() -> ReceiverConfigValidator.validateBeforeStart(e),
                "'Batch Send Mode' (producerBatchMode) must be NONE, JSON_ARRAY or XML_LIST, got: 'CSV'");
    }

    @Test
    public void theSecondPhaseChecksTheTransactionSettings() {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("JSON_ARRAY");
        e.setEnableTransactions(true);

        ReceiverConfigValidator.validateBeforeStart(e);
        ReceiverConfigValidator.validateRetry(e);
        assertRejected(() -> ReceiverConfigValidator.validateTransactional(e),
                "'Transactional ID Prefix' (transactionalIdPrefix) must not be empty while 'Enable Transactions' "
                        + "(enableTransactions) is on, got: ''");
    }

    @Test
    public void theThirdPhaseChecksTheRetrySettings() {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerRetryMaxAttempts(6);

        ReceiverConfigValidator.validateBeforeStart(e);
        ReceiverConfigValidator.validateTransactional(e);
        assertRejected(() -> ReceiverConfigValidator.validateRetry(e),
                "'Max Send Attempts' (producerRetryMaxAttempts) must be between 1 and 5, got: 6. This is the total "
                        + "number of attempts, not the number of additional ones; 1 switches the retry off.");
    }

    /**
     * The length of the {@code transactional.id} is checked by {@link TransactionalProducerPool#start},
     * which resolves the member suffix the id ends with.
     */
    @Test
    public void theTransactionalIdLengthIsLeftToThePool() {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("JSON_ARRAY");
        e.setEnableTransactions(true);
        e.setTransactionalIdPrefix(repeat('x', 300));

        ReceiverConfigValidator.validateTransactional(e);
    }

    @Test
    public void ineffectiveTransactionsAreLoggedUnderTheProducerCategory() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setEnableTransactions(true);

        String logged = capture(() -> ReceiverConfigValidator.validateTransactional(e));

        assertLoggedOnce(logged, "ERROR " + PRODUCER + " - [CPI-KAFKA-PLUS-DIAG] producer.config.ineffective "
                + "parameter=enableTransactions producerBatchMode=NONE reason='transactions apply to batches "
                + "only; single messages are sent by the shared, non-transactional producer'");
    }

    @Test
    public void ineffectiveJsonSchemaValidationIsLoggedOnceAtStart() throws Exception {
        CpiKafkaPlusEndpoint batch = endpoint();
        batch.setProducerBatchMode("JSON_ARRAY");
        batch.setJsonSchemaValidation(true);

        String logged = capture(() -> ReceiverConfigValidator.validateBeforeStart(batch));

        assertLoggedOnce(logged, "ERROR " + PRODUCER + " - [CPI-KAFKA-PLUS-DIAG] producer.config.ineffective "
                + "parameter=jsonSchemaValidation producerBatchMode=JSON_ARRAY reason='the JSON Schema applies "
                + "to single messages only; batches are sent without validation'");

        CpiKafkaPlusEndpoint single = endpoint();
        single.setJsonSchemaValidation(true);
        single.setJsonSchema("{\"type\": \"object\"}");
        Assert.assertEquals("", capture(() -> ReceiverConfigValidator.validateBeforeStart(single)));
    }

    /** ERROR, because nothing below WARN reaches the tenant trace (#255). */
    @Test
    public void theRetryLinesAreErrorsUnderTheProducerCategory() throws Exception {
        CpiKafkaPlusEndpoint batch = endpoint();
        batch.setProducerBatchMode("XML_LIST");
        batch.setProducerRetryMaxAttempts(3);

        assertLoggedOnce(capture(() -> ReceiverConfigValidator.validateRetry(batch)),
                "ERROR " + PRODUCER + " - [CPI-KAFKA-PLUS-DIAG] producer.config.ineffective "
                        + "parameter=producerRetryMaxAttempts value=3 producerBatchMode=XML_LIST reason='a partially "
                        + "sent batch cannot be retried without duplicating records; switch on enableTransactions "
                        + "to retry batches'");

        CpiKafkaPlusEndpoint singleSlot = endpoint();
        singleSlot.setProducerBatchMode("JSON_ARRAY");
        singleSlot.setEnableTransactions(true);
        singleSlot.setTransactionalIdPrefix("orders-tx");
        singleSlot.setMaxConcurrentTransactions(1);
        singleSlot.setProducerRetryMaxAttempts(2);
        singleSlot.setProducerRetryTotalBudgetSeconds(900);

        assertLoggedOnce(capture(() -> ReceiverConfigValidator.validateRetry(singleSlot)),
                "ERROR " + PRODUCER + " - [CPI-KAFKA-PLUS-DIAG] producer.config.ineffective "
                        + "parameter=producerRetryMaxAttempts value=2 maxConcurrentTransactions=1 reason='a retry "
                        + "holds the single transaction slot for up to producerRetryTotalBudgetSeconds=900s and "
                        + "blocks every other send on this node meanwhile; raise maxConcurrentTransactions'");
    }

    private static CpiKafkaPlusEndpoint endpoint() {
        CpiKafkaPlusEndpoint e = new CpiKafkaPlusEndpoint();
        e.setTopic("orders");
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

    private static String repeat(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
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
