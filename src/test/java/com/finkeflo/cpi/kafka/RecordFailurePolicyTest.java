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

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.finkeflo.cpi.kafka.RecordFailurePolicy.Action;
import com.finkeflo.cpi.kafka.RecordFailurePolicy.Decision;

/**
 * The failure table of {@link RecordFailurePolicy}, without Camel or a broker: every path with and
 * without a DLQ, under both settings of {@code errorHandling} and {@code retryOnlyTransientErrors}.
 * What the record processor does with each decision is pinned in {@link RecordProcessorProgressTest}.
 */
public class RecordFailurePolicyTest {

    private static final boolean DLQ = true;
    private static final boolean NO_DLQ = false;
    private static final boolean ONLY_TRANSIENT = true;
    private static final boolean ALL_ERRORS = false;
    private static final boolean SKIP = true;
    private static final boolean RETRY = false;

    private static final Exception TRANSIENT_FAILURE =
            new IllegalStateException("backend unreachable", new ConnectException("Connection refused"));
    private static final Exception PERMANENT_FAILURE = new IllegalStateException("backend rejected the record");
    private static final Exception UNDESERIALIZABLE = new IllegalArgumentException("Unknown magic byte!");
    private static final Exception REGISTRY_UNAVAILABLE =
            new SchemaRegistryHttpClient.SchemaRegistryException("GET /schemas/ids/7", 503, "");

    // --- route failure of a record: retried within the poll only with a DLQ ---

    @Test
    public void maxRetriesApplyOnlyWithADlq() {
        Assert.assertEquals(3, policy(DLQ, 3, ONLY_TRANSIENT, RETRY).maxRetries());
        Assert.assertEquals(0, policy(DLQ, 0, ONLY_TRANSIENT, RETRY).maxRetries());
        Assert.assertEquals("without a DLQ the blocked partition retries the record",
                0, policy(NO_DLQ, 3, ONLY_TRANSIENT, RETRY).maxRetries());
    }

    @Test
    public void onlyAFailureThatIsNotTransientSkipsTheRetries() {
        Assert.assertTrue(policy(DLQ, 3, ONLY_TRANSIENT, RETRY).skipsRetries(PERMANENT_FAILURE));
        Assert.assertFalse(policy(DLQ, 3, ONLY_TRANSIENT, RETRY).skipsRetries(TRANSIENT_FAILURE));
        Assert.assertFalse("retryOnlyTransientErrors off retries every failure",
                policy(DLQ, 3, ALL_ERRORS, RETRY).skipsRetries(PERMANENT_FAILURE));
        Assert.assertFalse(policy(DLQ, 3, ALL_ERRORS, RETRY).skipsRetries(TRANSIENT_FAILURE));
    }

    @Test
    public void afterTheRetriesARecordIsDeadLetteredOrLeftToErrorHandling() {
        // dlq, retryOnlyTransientErrors, errorHandling, retries skipped -> action, error type
        assertAfterRetries(DLQ, ONLY_TRANSIENT, RETRY, true, Action.DEAD_LETTER, "PERMANENT");
        assertAfterRetries(DLQ, ONLY_TRANSIENT, RETRY, false, Action.DEAD_LETTER, "TRANSIENT");
        assertAfterRetries(DLQ, ONLY_TRANSIENT, SKIP, true, Action.DEAD_LETTER, "PERMANENT");
        assertAfterRetries(DLQ, ONLY_TRANSIENT, SKIP, false, Action.DEAD_LETTER, "TRANSIENT");
        assertAfterRetries(DLQ, ALL_ERRORS, RETRY, true, Action.DEAD_LETTER, null);
        assertAfterRetries(DLQ, ALL_ERRORS, RETRY, false, Action.DEAD_LETTER, null);
        assertAfterRetries(DLQ, ALL_ERRORS, SKIP, true, Action.DEAD_LETTER, null);
        assertAfterRetries(DLQ, ALL_ERRORS, SKIP, false, Action.DEAD_LETTER, null);
        assertAfterRetries(NO_DLQ, ONLY_TRANSIENT, RETRY, true, Action.BLOCK, "PERMANENT");
        assertAfterRetries(NO_DLQ, ONLY_TRANSIENT, RETRY, false, Action.BLOCK, "TRANSIENT");
        assertAfterRetries(NO_DLQ, ONLY_TRANSIENT, SKIP, true, Action.SKIP, "PERMANENT");
        assertAfterRetries(NO_DLQ, ONLY_TRANSIENT, SKIP, false, Action.SKIP, "TRANSIENT");
        assertAfterRetries(NO_DLQ, ALL_ERRORS, RETRY, true, Action.BLOCK, null);
        assertAfterRetries(NO_DLQ, ALL_ERRORS, RETRY, false, Action.BLOCK, null);
        assertAfterRetries(NO_DLQ, ALL_ERRORS, SKIP, true, Action.SKIP, null);
        assertAfterRetries(NO_DLQ, ALL_ERRORS, SKIP, false, Action.SKIP, null);
    }

    // --- failures that are not retried within the poll ---

    @Test
    public void anUndeserializableRecordIsDeadLetteredOrLeftToErrorHandling() {
        Assert.assertEquals(Action.DEAD_LETTER,
                policy(DLQ, 3, ONLY_TRANSIENT, RETRY).forDeserializationFailure(UNDESERIALIZABLE));
        Assert.assertEquals(Action.DEAD_LETTER,
                policy(DLQ, 3, ONLY_TRANSIENT, SKIP).forDeserializationFailure(UNDESERIALIZABLE));
        Assert.assertEquals(Action.BLOCK,
                policy(NO_DLQ, 3, ONLY_TRANSIENT, RETRY).forDeserializationFailure(UNDESERIALIZABLE));
        Assert.assertEquals(Action.SKIP,
                policy(NO_DLQ, 3, ONLY_TRANSIENT, SKIP).forDeserializationFailure(UNDESERIALIZABLE));
        Assert.assertEquals("a schema ID the registry does not know can be the record's",
                Action.DEAD_LETTER, policy(DLQ, 3, ONLY_TRANSIENT, RETRY).forDeserializationFailure(
                        new SchemaRegistryHttpClient.SchemaRegistryException("GET /schemas/ids/7", 404, "")));
    }

    @Test
    public void aRecordOrBatchThatCannotBeDeserializedForATransientCauseBlocksWhateverTheSettings() {
        // #188: a Schema Registry that does not answer says nothing about the record.
        Exception unreachable = new RuntimeException("Avro deserialization failed",
                new ConnectException("Connection refused"));
        for (boolean dlq : new boolean[] {DLQ, NO_DLQ}) {
            for (boolean skip : new boolean[] {SKIP, RETRY}) {
                for (boolean onlyTransient : new boolean[] {ONLY_TRANSIENT, ALL_ERRORS}) {
                    RecordFailurePolicy policy = policy(dlq, 3, onlyTransient, skip);
                    String row = "dlq=" + dlq + " skip=" + skip + " retryOnlyTransientErrors=" + onlyTransient;
                    Assert.assertEquals(row, Action.BLOCK, policy.forDeserializationFailure(REGISTRY_UNAVAILABLE));
                    Assert.assertEquals(row, Action.BLOCK, policy.forDeserializationFailure(unreachable));
                    Assert.assertEquals(row, Action.BLOCK, policy.forBatchFormatFailure(REGISTRY_UNAVAILABLE));
                    Assert.assertEquals(row, Action.BLOCK, policy.forBatchFormatFailure(unreachable));
                }
            }
        }
    }

    @Test
    public void aSchemaViolationIsDeadLetteredOrDroppedWhateverErrorHandlingSays() {
        Assert.assertEquals(Action.DEAD_LETTER, policy(DLQ, 3, ONLY_TRANSIENT, RETRY).forSchemaViolation());
        Assert.assertEquals(Action.DEAD_LETTER, policy(DLQ, 3, ONLY_TRANSIENT, SKIP).forSchemaViolation());
        Assert.assertEquals(Action.SKIP, policy(NO_DLQ, 3, ONLY_TRANSIENT, RETRY).forSchemaViolation());
        Assert.assertEquals(Action.SKIP, policy(NO_DLQ, 3, ONLY_TRANSIENT, SKIP).forSchemaViolation());
    }

    // --- batches ---

    @Test
    public void aBatchThatCannotBeFormattedIsProcessedRecordByRecordWithADlq() {
        Exception formatFailure = new IllegalArgumentException("Unknown magic byte!");
        Error error = new NoClassDefFoundError("io/confluent/kafka/schemaregistry/ParsedSchema");
        Assert.assertEquals(Action.PROCESS_INDIVIDUALLY,
                policy(DLQ, 3, ONLY_TRANSIENT, RETRY).forBatchFormatFailure(formatFailure));
        Assert.assertEquals(Action.PROCESS_INDIVIDUALLY,
                policy(DLQ, 3, ONLY_TRANSIENT, SKIP).forBatchFormatFailure(formatFailure));
        Assert.assertEquals("the Error escapes later, from the record it comes from",
                Action.PROCESS_INDIVIDUALLY, policy(DLQ, 3, ONLY_TRANSIENT, RETRY).forBatchFormatFailure(error));
        Assert.assertEquals(Action.PROCESS_INDIVIDUALLY,
                policy(DLQ, 3, ONLY_TRANSIENT, SKIP).forBatchFormatFailure(error));
    }

    @Test
    public void withoutADlqAnErrorWhileFormattingEscapesAndAnyOtherFailureIsLeftToErrorHandling() {
        Exception formatFailure = new IllegalArgumentException("Unknown magic byte!");
        Error error = new NoClassDefFoundError("io/confluent/kafka/schemaregistry/ParsedSchema");
        Assert.assertEquals(Action.BLOCK,
                policy(NO_DLQ, 3, ONLY_TRANSIENT, RETRY).forBatchFormatFailure(formatFailure));
        Assert.assertEquals(Action.SKIP, policy(NO_DLQ, 3, ONLY_TRANSIENT, SKIP).forBatchFormatFailure(formatFailure));
        Assert.assertEquals(Action.ESCAPE, policy(NO_DLQ, 3, ONLY_TRANSIENT, RETRY).forBatchFormatFailure(error));
        Assert.assertEquals("an Error is not skipped",
                Action.ESCAPE, policy(NO_DLQ, 3, ONLY_TRANSIENT, SKIP).forBatchFormatFailure(error));
    }

    @Test
    public void aBatchTheRouteFailedForIsProcessedRecordByRecordOrLeftToErrorHandlingAsAWhole() {
        Assert.assertEquals(Action.PROCESS_INDIVIDUALLY, policy(DLQ, 3, ONLY_TRANSIENT, RETRY).forBatchRouteFailure());
        Assert.assertEquals(Action.PROCESS_INDIVIDUALLY, policy(DLQ, 3, ONLY_TRANSIENT, SKIP).forBatchRouteFailure());
        Assert.assertEquals(Action.BLOCK, policy(NO_DLQ, 3, ONLY_TRANSIENT, RETRY).forBatchRouteFailure());
        Assert.assertEquals(Action.SKIP, policy(NO_DLQ, 3, ONLY_TRANSIENT, SKIP).forBatchRouteFailure());
    }

    // --- the settings are read per decision ---

    @Test
    public void theSettingsAreReadWhenTheDecisionIsMade() {
        AtomicInteger dlqMaxRetries = new AtomicInteger(1);
        AtomicBoolean onlyTransient = new AtomicBoolean(true);
        AtomicBoolean skip = new AtomicBoolean(false);
        RecordFailurePolicy policy = new RecordFailurePolicy(false, dlqMaxRetries::get, onlyTransient::get, skip::get);
        RecordFailurePolicy withDlq = new RecordFailurePolicy(true, dlqMaxRetries::get, onlyTransient::get, skip::get);

        dlqMaxRetries.set(5);
        onlyTransient.set(false);
        skip.set(true);

        Assert.assertEquals(5, withDlq.maxRetries());
        Assert.assertFalse(withDlq.skipsRetries(PERMANENT_FAILURE));
        Assert.assertEquals(Action.SKIP, policy.forDeserializationFailure(UNDESERIALIZABLE));
        Assert.assertNull(policy.afterRetries(false).errorType());
    }

    @Test
    public void aDecisionNamesItsActionAndErrorType() {
        Assert.assertEquals("DEAD_LETTER(PERMANENT)",
                policy(DLQ, 3, ONLY_TRANSIENT, RETRY).afterRetries(true).toString());
        Assert.assertEquals("BLOCK", policy(NO_DLQ, 3, ALL_ERRORS, RETRY).afterRetries(false).toString());
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static RecordFailurePolicy policy(boolean dlq, int dlqMaxRetries, boolean retryOnlyTransientErrors,
                                              boolean skipFailedMessages) {
        return new RecordFailurePolicy(dlq, () -> dlqMaxRetries, () -> retryOnlyTransientErrors,
                () -> skipFailedMessages);
    }

    private static void assertAfterRetries(boolean dlq, boolean retryOnlyTransientErrors, boolean skipFailedMessages,
                                           boolean permanent, Action action, String errorType) {
        Decision decision = policy(dlq, 3, retryOnlyTransientErrors, skipFailedMessages).afterRetries(permanent);
        String row = "dlq=" + dlq + " retryOnlyTransientErrors=" + retryOnlyTransientErrors
                + " skip=" + skipFailedMessages + " permanent=" + permanent + ": " + decision;
        Assert.assertEquals(row, action, decision.action());
        Assert.assertEquals(row, errorType, decision.errorType());
    }
}
