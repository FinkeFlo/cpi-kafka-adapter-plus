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

import com.finkeflo.cpi.kafka.KafkaErrorHelper.Classification;

/**
 * Decides whether a failed producer send may be attempted again with a fresh producer.
 *
 * <p>This is deliberately a pure function on its own class rather than a branch inside
 * {@link CpiKafkaPlusProducer}: the rules below are the part of the feature that can silently
 * produce duplicate records, and they must be testable without a broker.
 *
 * <h2>Why an outer retry exists at all</h2>
 * Kafka's own {@code retries} / {@code delivery.timeout.ms} only repeat <em>inside</em> one producer
 * instance. Once a network disconnect has invalidated the transactional state, no amount of internal
 * retrying helps — only a throw-away producer does. That is what the caller builds; this class only
 * decides when building one is safe.
 *
 * <h2>The duplicate rule</h2>
 * An outer retry is permitted only where the previous attempt provably wrote nothing durable:
 * <ul>
 *   <li><b>Transactional, failure before {@code commitTransaction()}</b> — safe. A transaction that
 *       was never committed is invisible to {@code read_committed} consumers, and the successor
 *       producer with the same {@code transactional.id} fences the old epoch. Two independent
 *       clean-up mechanisms cover this, both documented in kafka-clients 4.3.1:
 *       {@code KafkaProducer.close(Duration)} ("It will also abort the ongoing transaction if it's
 *       not already completing", {@code KafkaProducer.java:1366-1372}) and
 *       {@code KafkaProducer.initTransactions()} ("If the previous instance had failed with a
 *       transaction in progress, it will be aborted", {@code KafkaProducer.java:637-643}).</li>
 *   <li><b>Transactional, failure during {@code commitTransaction()}</b> — never retried. Both
 *       clean-up mechanisms above carry the same exemption: a commit that has begun completion is
 *       <em>not</em> aborted, {@code initTransactions()} "awaits its completion"
 *       ({@code KafkaProducer.java:640-641}). The Javadoc of {@code commitTransaction()} says the
 *       same from the caller's side: a {@code TimeoutException} "does not mean the request did not
 *       actually reach the broker" ({@code KafkaProducer.java:769-773}). Retrying here would commit
 *       the same batch twice. This is documented behaviour, not a residual risk.</li>
 *   <li><b>Non-transactional single message</b> — retried only while the broker can still deduplicate
 *       it, i.e. with idempotence enabled <em>and</em> the same producer instance, because the
 *       broker deduplicates on {@code (PID, sequence)} and a new producer gets a new PID.</li>
 *   <li><b>Non-transactional batch</b> — never retried; the caller does not even ask. Records
 *       {@code 0..k-1} of a batch can already be committed when {@code k} fails, so resending the
 *       batch duplicates them.</li>
 * </ul>
 */
final class ProducerRetryPolicy {

    private ProducerRetryPolicy() {
    }

    /**
     * Where a transactional attempt stood when it failed. Set immediately before each blocking call
     * and advanced immediately after it returns, so the commit window is bracketed exactly rather
     * than inferred from the exception type.
     */
    enum TxnPhase {
        /** Constructing the {@code KafkaProducer}. */
        CREATE,
        /** Inside {@code initTransactions()}. */
        INIT,
        /** Inside {@code beginTransaction()}. */
        BEGIN,
        /** Inside the batch send, i.e. {@code send()} plus waiting for the acknowledgements. */
        SEND,
        /** Inside {@code commitTransaction()} — the one phase whose outcome is unknowable. */
        COMMIT,
        /** {@code commitTransaction()} returned normally. */
        COMMITTED
    }

    /**
     * Why no further attempt was made. Carried into the diagnostic line and the MPL, because
     * "was not retried" is useless to support without the reason: a permanent error, an exhausted
     * budget and a disabled feature look identical from the outside.
     */
    enum StopReason {
        /** {@code producerRetryMaxAttempts} is 1 — the feature is switched off. */
        RETRY_DISABLED,
        /** Every configured attempt has been used. */
        ATTEMPTS_EXHAUSTED,
        /** The next attempt plus its delay would exceed {@code producerRetryTotalBudgetSeconds}. */
        BUDGET_EXHAUSTED,
        /** The failure happened in or after {@code commitTransaction()}; a retry could duplicate. */
        COMMIT_OUTCOME_UNKNOWN,
        /** The error will fail identically on a fresh producer (data error / unknown fatal). */
        PERMANENT,
        /** Non-transactional single path with {@code enableIdempotence=false}. */
        IDEMPOTENCE_DISABLED,
        /** The error could not be classified, so no retry promise can be made. */
        UNCLASSIFIED,
        /** The thread was interrupted while waiting between attempts. */
        INTERRUPTED
    }

    /** Outcome of a retry decision: either "try again" or "stop, and here is why". */
    static final class Decision {

        private static final Decision RETRY = new Decision(true, null);

        private final boolean retry;
        private final StopReason stopReason;

        private Decision(boolean retry, StopReason stopReason) {
            this.retry = retry;
            this.stopReason = stopReason;
        }

        static Decision retry() {
            return RETRY;
        }

        static Decision stop(StopReason reason) {
            return new Decision(false, reason);
        }

        boolean isRetry() {
            return retry;
        }

        /** {@code null} when {@link #isRetry()} is true. */
        StopReason stopReason() {
            return stopReason;
        }

        @Override
        public String toString() {
            return retry ? "RETRY" : "STOP(" + stopReason + ")";
        }
    }

    /**
     * Decides whether the transactional batch path may run another attempt.
     *
     * @param error              the failure of the attempt that just ended
     * @param phase              the phase the failed attempt had reached
     * @param attempt            1-based number of the attempt that just failed
     * @param maxAttempts        total attempts allowed, not additional ones ({@code 1} = feature off)
     * @param delayMs            wait before the next attempt
     * @param budgetDeadlineMs   wall-clock deadline for all attempts together
     * @param nowMs              current wall-clock time
     * @param onlyTransient      when true, only {@code RETRIABLE} is retried
     */
    static Decision decideTransactional(Throwable error, TxnPhase phase, int attempt, int maxAttempts,
                                        long delayMs, long budgetDeadlineMs, long nowMs,
                                        boolean onlyTransient) {
        return decideTransactional(classifyForRetry(error), phase, attempt, maxAttempts,
                delayMs, budgetDeadlineMs, nowMs, onlyTransient);
    }

    /**
     * Classification-level variant. Exists so a {@code null} classification — which
     * {@link KafkaErrorHelper#classify} does not currently produce, but which any future change to
     * it would — is a covered, tested case rather than an accidental retry.
     */
    static Decision decideTransactional(Classification classification, TxnPhase phase, int attempt,
                                        int maxAttempts, long delayMs, long budgetDeadlineMs,
                                        long nowMs, boolean onlyTransient) {
        if (maxAttempts <= 1) {
            return Decision.stop(StopReason.RETRY_DISABLED);
        }
        if (attempt >= maxAttempts) {
            return Decision.stop(StopReason.ATTEMPTS_EXHAUSTED);
        }
        if (nowMs + delayMs > budgetDeadlineMs) {
            return Decision.stop(StopReason.BUDGET_EXHAUSTED);
        }
        // The core rule. Checked before the classification, because it is the classification that
        // would otherwise wave a TimeoutException through — and a TimeoutException is exactly how a
        // commit whose acknowledgement was lost presents itself.
        if (phase == TxnPhase.COMMIT || phase == TxnPhase.COMMITTED) {
            return Decision.stop(StopReason.COMMIT_OUTCOME_UNKNOWN);
        }
        return classify(classification, onlyTransient, true);
    }

    /**
     * Decides whether the non-transactional single-message path may run another attempt.
     *
     * <p>Unlike the transactional path this reuses the <em>same</em> producer, so the broker's
     * {@code (PID, sequence)} deduplication still covers the "record was written, acknowledgement
     * lost" case — which is why idempotence is a precondition rather than a nicety.
     *
     * @param idempotenceEnabled value of {@code enableIdempotence}
     */
    static Decision decideSingle(Throwable error, int attempt, int maxAttempts, long delayMs,
                                 long budgetDeadlineMs, long nowMs, boolean idempotenceEnabled) {
        return decideSingle(classifyForRetry(error), attempt, maxAttempts, delayMs,
                budgetDeadlineMs, nowMs, idempotenceEnabled);
    }

    /**
     * Classifies a failure for the purpose of this retry loop, which differs from
     * {@link KafkaErrorHelper#classify} in exactly one case: the KAFKA-10902 monitor fault.
     *
     * <p>{@code IllegalMonitorStateException: current thread is not owner} matches none of the Kafka
     * exception hierarchies and is therefore reported as {@code UNKNOWN_FATAL} — which, under the
     * default {@code producerRetryOnlyTransientErrors=true}, would refuse to retry the one failure
     * this feature was built after. It is a transient JVM-level monitor state, not a broker verdict:
     * a production trace of 2026-08-20 shows it arriving in bursts on individual worker nodes while
     * the broker was serving every other channel on the same tenant. Treating it as
     * {@code RETRIABLE} is safe here for the same reason the inner {@code MonitorFaultRetry} is
     * safe, and independently of it: in the transactional path the phase check has already stopped
     * everything from {@code COMMIT} onwards, so the failed attempt's transaction is aborted and
     * invisible to {@code read_committed} consumers; in the single path idempotence is a
     * precondition, so the broker deduplicates on {@code (PID, sequence)}.
     *
     * <p>This is a second line of defence, not the fix. The fix is the metadata pre-warm and the
     * raised {@code metadata.max.idle.ms}, which keep the client out of the defective code path.
     */
    private static Classification classifyForRetry(Throwable error) {
        if (KafkaErrorHelper.isMetadataMonitorFault(error)) {
            return Classification.RETRIABLE;
        }
        return KafkaErrorHelper.classify(error);
    }

    /** Classification-level variant, see {@link #decideTransactional(Classification, TxnPhase, int, int, long, long, long, boolean)}. */
    static Decision decideSingle(Classification classification, int attempt, int maxAttempts,
                                 long delayMs, long budgetDeadlineMs, long nowMs,
                                 boolean idempotenceEnabled) {
        if (maxAttempts <= 1) {
            return Decision.stop(StopReason.RETRY_DISABLED);
        }
        if (!idempotenceEnabled) {
            // Without broker-side deduplication a lost acknowledgement is indistinguishable from a
            // lost record, so retrying would silently duplicate. Refusing loudly is the point.
            return Decision.stop(StopReason.IDEMPOTENCE_DISABLED);
        }
        if (attempt >= maxAttempts) {
            return Decision.stop(StopReason.ATTEMPTS_EXHAUSTED);
        }
        if (nowMs + delayMs > budgetDeadlineMs) {
            return Decision.stop(StopReason.BUDGET_EXHAUSTED);
        }
        // FATAL_PRODUCER_UNUSABLE is never retried here regardless of configuration: this path
        // reuses the shared producer, and a broken shared producer belongs in the existing rebuild
        // path (handleSendFailure), not in a retry loop that would keep using it.
        return classify(classification, true, false);
    }

    private static Decision classify(Classification classification, boolean onlyTransient,
                                     boolean producerUnusableRetryable) {
        if (classification == null) {
            return Decision.stop(StopReason.UNCLASSIFIED);
        }
        switch (classification) {
            case RETRIABLE:
                return Decision.retry();
            case FATAL_PRODUCER_UNUSABLE:
                // Worth retrying only where the producer really is a throw-away object, and even
                // there not by default: a ProducerFencedException usually means another instance
                // took over the transactional.id, and retrying turns that into a zombie fight both
                // sides lose.
                return producerUnusableRetryable && !onlyTransient
                        ? Decision.retry()
                        : Decision.stop(StopReason.PERMANENT);
            case FATAL_DATA_ERROR:
            case UNKNOWN_FATAL:
                // A too-large record or a serialisation failure fails identically on the second
                // attempt, and UNKNOWN_FATAL by definition lacks the ground for a retry promise.
                return Decision.stop(StopReason.PERMANENT);
            default:
                return Decision.stop(StopReason.UNCLASSIFIED);
        }
    }

    /**
     * Worst-case wall-clock duration of a fully configured retry run, in seconds.
     *
     * <p>Used by the start-up fail-fast check. The arithmetic must cover <em>every</em> blocking
     * phase, not just the wait for acknowledgements:
     * {@code delivery.timeout.ms} bounds only the acks, while {@code initTransactions()},
     * the metadata block inside {@code send()} and {@code commitTransaction()} are each bounded by
     * {@code max.block.ms} — see the timeout messages in {@code KafkaProducer.java:250-258} and
     * {@code ProducerConfigFactory} line 86, which pins
     * {@code max.block.ms = min(30_000, delivery.timeout.ms)}.
     *
     * <p>For the transactional path this makes the worst case roughly four times
     * {@code deliveryTimeoutSeconds} per attempt, not once.
     *
     * @param transactional whether the transactional phases (init, commit, producer close) apply
     */
    static long worstCaseSeconds(int maxAttempts, int deliveryTimeoutSeconds, int delaySeconds,
                                 int closeTimeoutSeconds, boolean transactional) {
        long blockSeconds = Math.min(30, deliveryTimeoutSeconds);
        long perAttempt = blockSeconds                 // send(): metadata block
                + deliveryTimeoutSeconds;              // waiting for the acknowledgements
        if (transactional) {
            perAttempt += blockSeconds                 // initTransactions()
                    + blockSeconds                     // commitTransaction()
                    + closeTimeoutSeconds;             // bounded close of the throw-away producer
        }
        return (long) maxAttempts * perAttempt + (long) (maxAttempts - 1) * delaySeconds;
    }
}
