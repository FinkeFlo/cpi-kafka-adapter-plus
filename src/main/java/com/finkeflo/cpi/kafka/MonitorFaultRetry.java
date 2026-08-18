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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded retry for the one Kafka client defect this adapter can recover from in flight:
 * KAFKA-10902, an {@link IllegalMonitorStateException} thrown out of the producer's metadata wait.
 * See {@link KafkaErrorHelper#isMetadataMonitorFault} for how the fault is recognised and
 * {@link KafkaErrorHelper#isTransientMetadataMonitorFaultRetryable} for why retrying it cannot
 * duplicate a record.
 *
 * <p>Retrying a JVM-level fault is only defensible if it provably terminates, so three independent
 * bounds apply and the first one reached stops the retry:
 *
 * <ol>
 *   <li><b>Per record</b> — at most {@link #MAX_RETRIES_PER_RECORD} retries for a single call.</li>
 *   <li><b>Per batch</b> — at most {@link #MAX_RETRIES_PER_BATCH} retries across an entire batch,
 *       however many records it contains. Without this, a 1,000-record batch would permit 3,000
 *       retries: the per-record limit would simply be multiplied by the record count.</li>
 *   <li><b>Wall clock</b> — never retry once the batch deadline the send guard already maintains has
 *       passed.</li>
 * </ol>
 *
 * <p>The worst case for a batch is therefore five extra attempts and roughly 250 ms of added
 * latency, after which the failure is reported instead of retried.
 *
 * <p>The batch allowance is deliberately small. In the incident this addresses, the fault occurred
 * five times in eighteen minutes across all threads of a node, so at most one occurrence per batch
 * is expected. A batch that burns through five of them is not experiencing a transient glitch, and
 * failing loudly with {@code stopReason=BATCH_BUDGET_EXHAUSTED} is the more useful outcome than
 * continuing to retry.
 */
final class MonitorFaultRetry {

    private static final Logger LOG = LoggerFactory.getLogger(MonitorFaultRetry.class);

    /** Retries granted to a single call. */
    static final int MAX_RETRIES_PER_RECORD = 3;
    /** Retries granted to a whole batch, shared by all its records. */
    static final int MAX_RETRIES_PER_BATCH = 5;
    /** Pause between attempts, long enough for a transient monitor state to clear. */
    static final long BACKOFF_MS = 50L;

    private MonitorFaultRetry() {}

    /** The call being guarded, typically a synchronous {@code KafkaProducer.send(...)}. */
    interface Call<T> {
        T invoke() throws Exception;
    }

    /** Sleep hook, so tests can assert the bounds without spending real time. */
    interface Backoff {
        void pause(long millis) throws InterruptedException;
    }

    private static final Backoff REAL_SLEEP = Thread::sleep;

    /** The retry allowance shared by every record of one batch. Not thread-safe by design. */
    static final class Budget {
        private int remaining = MAX_RETRIES_PER_BATCH;

        boolean tryConsume() {
            if (remaining <= 0) {
                return false;
            }
            remaining--;
            return true;
        }

        int remaining() {
            return remaining;
        }
    }

    /**
     * Invokes {@code call}, retrying only on the KAFKA-10902 signature and only within the bounds
     * documented on this class. Any other failure is rethrown untouched and unretried.
     *
     * <p>Outcomes are logged at ERROR, including the successful ones. That is deliberate: only ERROR
     * reaches the CPI tenant trace file, and a JVM-level fault recovered in flight is rare enough
     * that a line is worth it — it is what turns "the batch simply worked" into evidence that the
     * fault occurred at all.
     *
     * @param batchBudget allowance shared across the batch; see {@link Budget}
     * @param deadlineMs  absolute batch deadline in {@code System.currentTimeMillis()} terms
     */
    static <T> T execute(Call<T> call, Budget batchBudget, long deadlineMs,
                         String topic, int recordIndex) throws Exception {
        return execute(call, batchBudget, deadlineMs, topic, recordIndex, REAL_SLEEP);
    }

    static <T> T execute(Call<T> call, Budget batchBudget, long deadlineMs,
                         String topic, int recordIndex, Backoff backoff) throws Exception {
        int attempt = 0;
        while (true) {
            try {
                T result = call.invoke();
                if (attempt > 0) {
                    AdapterDiagnostics.error(LOG, event(topic, recordIndex, batchBudget, attempt)
                            .with("retryOutcome", "SUCCESS"));
                }
                return result;
            } catch (Exception e) {
                if (!KafkaErrorHelper.isTransientMetadataMonitorFaultRetryable(e)) {
                    throw e;
                }
                String stopReason = stopReason(attempt, batchBudget, deadlineMs);
                if (stopReason != null) {
                    AdapterDiagnostics.error(LOG, event(topic, recordIndex, batchBudget, attempt)
                            .with("retryOutcome", "FAILED")
                            .with("stopReason", stopReason), e);
                    throw e;
                }
                attempt++;
                try {
                    backoff.pause(BACKOFF_MS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    AdapterDiagnostics.error(LOG, event(topic, recordIndex, batchBudget, attempt)
                            .with("retryOutcome", "FAILED")
                            .with("stopReason", "INTERRUPTED"), e);
                    throw e;
                }
            }
        }
    }

    /**
     * Returns why retrying must stop, or {@code null} to retry. Evaluated in order of cost: the two
     * cheap limits are checked before the batch allowance is consumed, so a retry that could not
     * happen anyway does not spend budget another record might still use.
     */
    private static String stopReason(int attempt, Budget batchBudget, long deadlineMs) {
        if (attempt >= MAX_RETRIES_PER_RECORD) {
            return "RECORD_BUDGET_EXHAUSTED";
        }
        if (System.currentTimeMillis() >= deadlineMs) {
            return "BATCH_DEADLINE_REACHED";
        }
        if (!batchBudget.tryConsume()) {
            return "BATCH_BUDGET_EXHAUSTED";
        }
        return null;
    }

    private static AdapterDiagnostics.Event event(String topic, int recordIndex,
                                                  Budget batchBudget, int attempt) {
        return AdapterDiagnostics.event("producer.send.monitorFaultRetry")
                .with("topic", topic)
                .with("recordIndex", recordIndex)
                .with("retryCount", attempt)
                .with("recordRetryLimit", MAX_RETRIES_PER_RECORD)
                .with("batchRetriesLeft", batchBudget.remaining())
                .with("thread", Thread.currentThread().getName());
    }
}
