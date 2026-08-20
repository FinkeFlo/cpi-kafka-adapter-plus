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

import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Assert;
import org.junit.Test;

import com.finkeflo.cpi.kafka.ProducerRetryPolicy.Decision;
import com.finkeflo.cpi.kafka.ProducerRetryPolicy.StopReason;
import com.finkeflo.cpi.kafka.ProducerRetryPolicy.TxnPhase;

/**
 * Pins the rules that decide whether a failed send may be repeated.
 *
 * <p>These are the rules that can silently duplicate records, so they live in a pure function and
 * are asserted without a broker. The case that matters most is
 * {@link #retriableErrorInsideCommitIsNeverRetried()}: it is the one where the classification says
 * "transient" and the correct answer is still "stop".
 */
public class ProducerRetryPolicyTest {

    private static final long NOW = 1_000_000L;
    private static final long FAR_DEADLINE = NOW + 600_000L;
    private static final long DELAY_MS = 2_000L;

    private static Decision txn(Throwable error, TxnPhase phase, int attempt, int maxAttempts,
                                boolean onlyTransient) {
        return ProducerRetryPolicy.decideTransactional(error, phase, attempt, maxAttempts,
                DELAY_MS, FAR_DEADLINE, NOW, onlyTransient);
    }

    private static void assertStop(StopReason expected, Decision actual) {
        Assert.assertFalse("expected no retry but got " + actual, actual.isRetry());
        Assert.assertEquals(expected, actual.stopReason());
    }

    @Test
    public void retriableErrorBeforeCommitIsRetried() {
        for (TxnPhase phase : new TxnPhase[] {TxnPhase.CREATE, TxnPhase.INIT, TxnPhase.BEGIN, TxnPhase.SEND}) {
            Decision d = txn(new NetworkException("Disconnected from node 3"), phase, 1, 3, true);
            Assert.assertTrue("phase " + phase + " must be retryable", d.isRetry());
        }
    }

    @Test
    public void retriableErrorInsideCommitIsNeverRetried() {
        // The production shape of this: commitTransaction() times out, the record may or may not be
        // committed broker-side, and Kafka's own javadoc says a timeout "does not mean the request
        // did not actually reach the broker" (KafkaProducer.java:769-773).
        assertStop(StopReason.COMMIT_OUTCOME_UNKNOWN,
                txn(new TimeoutException("CommitTransaction timed out"), TxnPhase.COMMIT, 1, 5, true));
        assertStop(StopReason.COMMIT_OUTCOME_UNKNOWN,
                txn(new NetworkException("boom"), TxnPhase.COMMITTED, 1, 5, true));
    }

    @Test
    public void commitPhaseIsNotRetriedEvenWithTransientOnlyDisabled() {
        assertStop(StopReason.COMMIT_OUTCOME_UNKNOWN,
                txn(new NetworkException("boom"), TxnPhase.COMMIT, 1, 5, false));
    }

    @Test
    public void dataErrorIsNeverRetried() {
        assertStop(StopReason.PERMANENT,
                txn(new RecordTooLargeException("too big"), TxnPhase.SEND, 1, 5, true));
        assertStop(StopReason.PERMANENT,
                txn(new RecordTooLargeException("too big"), TxnPhase.SEND, 1, 5, false));
    }

    @Test
    public void unknownFatalIsNeverRetried() {
        assertStop(StopReason.PERMANENT,
                txn(new IllegalStateException("nothing the classifier has heard of"), TxnPhase.SEND, 1, 5, true));
        assertStop(StopReason.PERMANENT,
                txn(new IllegalStateException("nothing the classifier has heard of"), TxnPhase.SEND, 1, 5, false));
    }

    @Test
    public void producerUnusableIsRetriedOnlyWhenTransientOnlyIsDisabled() {
        assertStop(StopReason.PERMANENT,
                txn(new ProducerFencedException("fenced"), TxnPhase.SEND, 1, 5, true));
        Assert.assertTrue(txn(new ProducerFencedException("fenced"), TxnPhase.SEND, 1, 5, false).isRetry());
    }

    @Test
    public void unclassifiedClassificationIsNeverRetried() {
        // classify() does not currently return null; the policy still has to refuse rather than
        // fall through to a retry if that ever changes.
        assertStop(StopReason.UNCLASSIFIED, ProducerRetryPolicy.decideTransactional(
                (KafkaErrorHelper.Classification) null, TxnPhase.SEND, 1, 5, DELAY_MS,
                FAR_DEADLINE, NOW, true));
    }

    @Test
    public void lastAttemptStops() {
        assertStop(StopReason.ATTEMPTS_EXHAUSTED,
                txn(new NetworkException("boom"), TxnPhase.SEND, 3, 3, true));
    }

    @Test
    public void exhaustedBudgetStopsEvenWithAttemptsAndARetriableError() {
        Decision d = ProducerRetryPolicy.decideTransactional(new NetworkException("boom"),
                TxnPhase.SEND, 1, 5, DELAY_MS, NOW + 1_000L, NOW, true);
        assertStop(StopReason.BUDGET_EXHAUSTED, d);
    }

    @Test
    public void featureOffNeverRetries() {
        assertStop(StopReason.RETRY_DISABLED,
                txn(new NetworkException("boom"), TxnPhase.SEND, 1, 1, true));
    }

    @Test
    public void singlePathRequiresIdempotence() {
        assertStop(StopReason.IDEMPOTENCE_DISABLED, ProducerRetryPolicy.decideSingle(
                new NetworkException("boom"), 1, 3, DELAY_MS, FAR_DEADLINE, NOW, false));
        Assert.assertTrue(ProducerRetryPolicy.decideSingle(
                new NetworkException("boom"), 1, 3, DELAY_MS, FAR_DEADLINE, NOW, true).isRetry());
    }

    @Test
    public void singlePathNeverRetriesAnUnusableSharedProducer() {
        // The shared producer belongs in the existing rebuild path, not in a retry loop that would
        // keep sending through the broken instance.
        assertStop(StopReason.PERMANENT, ProducerRetryPolicy.decideSingle(
                new ProducerFencedException("fenced"), 1, 3, DELAY_MS, FAR_DEADLINE, NOW, true));
    }

    @Test
    public void singlePathFeatureOffNeverRetries() {
        assertStop(StopReason.RETRY_DISABLED, ProducerRetryPolicy.decideSingle(
                new NetworkException("boom"), 1, 1, DELAY_MS, FAR_DEADLINE, NOW, true));
    }

    @Test
    public void theMonitorFaultIsRetriedDespiteBeingUnclassifiable() {
        // KAFKA-10902 matches no Kafka exception hierarchy, so KafkaErrorHelper.classify() reports
        // UNKNOWN_FATAL — which under the default onlyTransient=true would refuse to retry the exact
        // failure observed in production on 2026-08-20.
        Assert.assertEquals(KafkaErrorHelper.Classification.UNKNOWN_FATAL,
                KafkaErrorHelper.classify(monitorFault()));
        Assert.assertTrue("the monitor fault must be retried before the commit",
                txn(monitorFault(), TxnPhase.SEND, 1, 3, true).isRetry());
        Assert.assertTrue("the single path deduplicates on (PID, sequence)",
                ProducerRetryPolicy.decideSingle(monitorFault(), 1, 3, DELAY_MS, FAR_DEADLINE,
                        NOW, true).isRetry());
    }

    @Test
    public void theMonitorFaultIsStillNotRetriedInsideTheCommit() {
        // The exemption is about the classification only; it must not reach past the phase rule,
        // which is what keeps the batch from being written twice.
        assertStop(StopReason.COMMIT_OUTCOME_UNKNOWN,
                txn(monitorFault(), TxnPhase.COMMIT, 1, 3, true));
    }

    @Test
    public void theMonitorFaultExemptionNeedsAKafkaOrigin() {
        // An IllegalMonitorStateException raised by application code with the same message is not
        // KAFKA-10902 and gets no exemption.
        IllegalMonitorStateException notKafka = new IllegalMonitorStateException("current thread is not owner");
        notKafka.setStackTrace(new StackTraceElement[] {
            new StackTraceElement("com.example.Whatever", "run", "Whatever.java", 7),
        });
        assertStop(StopReason.PERMANENT, txn(notKafka, TxnPhase.SEND, 1, 3, true));
    }

    /** The KAFKA-10902 signature as it arrives from a synchronous {@code send()}. */
    private static Exception monitorFault() {
        IllegalMonitorStateException imse = new IllegalMonitorStateException("current thread is not owner");
        imse.setStackTrace(new StackTraceElement[] {
            new StackTraceElement("org.apache.kafka.common.utils.SystemTime", "waitObject", "SystemTime.java", 62),
            new StackTraceElement("org.apache.kafka.clients.producer.internals.ProducerMetadata",
                    "awaitUpdate", "ProducerMetadata.java", 119),
            new StackTraceElement("org.apache.kafka.clients.producer.KafkaProducer",
                    "waitOnMetadata", "KafkaProducer.java", 1120),
        });
        return new RuntimeException("Batch send failed at record index 0 (phase=SYNC_SEND): "
                + imse.getMessage(), imse);
    }

    @Test
    public void worstCaseCoversEveryBlockingPhaseOfATransactionalAttempt() {
        // deliveryTimeoutSeconds=2 => max.block.ms = min(30s, 2s) = 2s, applied three times
        // (initTransactions, metadata block in send, commitTransaction) plus 2s of acks plus a 5s
        // close = 13s per attempt; two attempts with a 2s delay = 28s.
        Assert.assertEquals(28L, ProducerRetryPolicy.worstCaseSeconds(2, 2, 2, 5, true));
        // The default delivery timeout blows the default 30s budget by an order of magnitude —
        // this is the arithmetic that makes the start-up check refuse it.
        Assert.assertTrue(ProducerRetryPolicy.worstCaseSeconds(2, 120, 2, 5, true) > 300);
    }

    @Test
    public void worstCaseOmitsTransactionalPhasesForTheSinglePath() {
        // No initTransactions, no commit, no throw-away producer to close.
        Assert.assertEquals(2L * (2 + 2) + 2, ProducerRetryPolicy.worstCaseSeconds(2, 2, 2, 5, false));
    }
}
