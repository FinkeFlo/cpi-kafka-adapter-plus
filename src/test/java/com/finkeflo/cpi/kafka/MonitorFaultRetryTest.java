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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

/**
 * Proves that the KAFKA-10902 retry terminates.
 *
 * <p>Retrying a JVM-level fault is only acceptable if it provably cannot run away, so every bound is
 * asserted here with an exact call count rather than an upper estimate: per record, per batch, and
 * against the wall clock. The backoff is stubbed out, so these assertions cost no real time.
 */
public class MonitorFaultRetryTest {

    /** The real fault: the bare message only {@code Object.wait()} produces, from a Kafka frame. */
    private static Exception monitorFault() {
        IllegalMonitorStateException imse = new IllegalMonitorStateException("current thread is not owner");
        imse.setStackTrace(new StackTraceElement[] {
            new StackTraceElement("org.apache.kafka.common.utils.SystemTime", "waitObject", "SystemTime.java", 62),
            new StackTraceElement("org.apache.kafka.clients.producer.internals.ProducerMetadata",
                    "awaitUpdate", "ProducerMetadata.java", 119),
            new StackTraceElement("org.apache.kafka.clients.producer.KafkaProducer",
                    "waitOnMetadata", "KafkaProducer.java", 1120),
        });
        return new RuntimeException("Batch send failed at record index 0: " + imse.getMessage(), imse);
    }

    private static final MonitorFaultRetry.Backoff NO_SLEEP = millis -> { };

    private static long farFutureDeadline() {
        return System.currentTimeMillis() + 600_000L;
    }

    private static <T> T execute(MonitorFaultRetry.Call<T> call, MonitorFaultRetry.Budget budget,
                                 long deadlineMs) throws Exception {
        return MonitorFaultRetry.execute(call, budget, deadlineMs, "test-topic", 0, NO_SLEEP);
    }

    @Test
    public void returnsImmediatelyWhenTheCallSucceeds() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        String result = execute(() -> {
            calls.incrementAndGet();
            return "ok";
        }, new MonitorFaultRetry.Budget(), farFutureDeadline());

        Assert.assertEquals("ok", result);
        Assert.assertEquals(1, calls.get());
    }

    @Test
    public void stopsAfterThePerRecordLimit() {
        AtomicInteger calls = new AtomicInteger();
        MonitorFaultRetry.Budget budget = new MonitorFaultRetry.Budget();

        try {
            execute(() -> {
                calls.incrementAndGet();
                throw monitorFault();
            }, budget, farFutureDeadline());
            Assert.fail("expected the fault to be rethrown once the record limit is reached");
        } catch (Exception expected) {
            Assert.assertTrue(KafkaErrorHelper.isMetadataMonitorFault(expected));
        }

        Assert.assertEquals("one initial attempt plus exactly MAX_RETRIES_PER_RECORD retries",
                MonitorFaultRetry.MAX_RETRIES_PER_RECORD + 1, calls.get());
    }

    @Test
    public void aBatchCannotMultiplyThePerRecordLimitByItsRecordCount() throws Exception {
        // Each record fails once and then succeeds, which is the only way retries accumulate across
        // a batch. Without the shared allowance a 1,000-record batch would permit 3,000 retries.
        MonitorFaultRetry.Budget budget = new MonitorFaultRetry.Budget();
        AtomicInteger totalCalls = new AtomicInteger();
        List<Integer> succeeded = new ArrayList<>();

        int recordCount = 1000;
        int failedAtRecord = -1;
        for (int record = 0; record < recordCount; record++) {
            AtomicInteger attemptsForThisRecord = new AtomicInteger();
            try {
                MonitorFaultRetry.execute(() -> {
                    totalCalls.incrementAndGet();
                    if (attemptsForThisRecord.getAndIncrement() == 0) {
                        throw monitorFault();
                    }
                    return "ok";
                }, budget, farFutureDeadline(), "test-topic", record, NO_SLEEP);
                succeeded.add(record);
            } catch (Exception e) {
                failedAtRecord = record;
                break;
            }
        }

        Assert.assertEquals("the batch allowance must run out after MAX_RETRIES_PER_BATCH records",
                MonitorFaultRetry.MAX_RETRIES_PER_BATCH, succeeded.size());
        Assert.assertEquals(MonitorFaultRetry.MAX_RETRIES_PER_BATCH, failedAtRecord);
        Assert.assertEquals(0, budget.remaining());
        // Five recovered records at two calls each, plus the single call of the one that gave up.
        Assert.assertEquals(2 * MonitorFaultRetry.MAX_RETRIES_PER_BATCH + 1, totalCalls.get());
    }

    @Test
    public void neverExceedsTheBatchAllowanceInTotal() {
        // Whatever the failure pattern, the number of retries a batch can spend is capped.
        MonitorFaultRetry.Budget budget = new MonitorFaultRetry.Budget();
        AtomicInteger totalCalls = new AtomicInteger();

        for (int record = 0; record < 50; record++) {
            try {
                MonitorFaultRetry.execute(() -> {
                    totalCalls.incrementAndGet();
                    throw monitorFault();
                }, budget, farFutureDeadline(), "test-topic", record, NO_SLEEP);
            } catch (Exception ignored) {
                // Each record is expected to give up; the batch allowance is the point here.
            }
        }

        Assert.assertEquals(0, budget.remaining());
        // 50 initial attempts that can never be skipped, plus at most the batch allowance in
        // retries. Nothing about the record count can raise that ceiling.
        Assert.assertTrue("total calls were " + totalCalls.get(),
                totalCalls.get() <= 50 + MonitorFaultRetry.MAX_RETRIES_PER_BATCH);
    }

    @Test
    public void doesNotRetryPastTheBatchDeadline() {
        AtomicInteger calls = new AtomicInteger();
        MonitorFaultRetry.Budget budget = new MonitorFaultRetry.Budget();
        long alreadyPassed = System.currentTimeMillis() - 1L;

        try {
            execute(() -> {
                calls.incrementAndGet();
                throw monitorFault();
            }, budget, alreadyPassed);
            Assert.fail("expected the fault to be rethrown");
        } catch (Exception expected) {
            Assert.assertTrue(KafkaErrorHelper.isMetadataMonitorFault(expected));
        }

        Assert.assertEquals("no retry may happen once the deadline has passed", 1, calls.get());
        Assert.assertEquals("a retry that cannot happen must not consume the batch allowance",
                MonitorFaultRetry.MAX_RETRIES_PER_BATCH, budget.remaining());
    }

    @Test
    public void doesNotRetryAnyOtherFailure() {
        AtomicInteger calls = new AtomicInteger();
        MonitorFaultRetry.Budget budget = new MonitorFaultRetry.Budget();

        try {
            execute(() -> {
                calls.incrementAndGet();
                throw new org.apache.kafka.common.errors.RecordTooLargeException("too large");
            }, budget, farFutureDeadline());
            Assert.fail("expected the exception to be rethrown");
        } catch (Exception expected) {
            Assert.assertTrue(expected instanceof org.apache.kafka.common.errors.RecordTooLargeException);
        }

        Assert.assertEquals("only the KAFKA-10902 signature may be retried", 1, calls.get());
        Assert.assertEquals(MonitorFaultRetry.MAX_RETRIES_PER_BATCH, budget.remaining());
    }

    @Test
    public void doesNotRetryAnUnrelatedIllegalMonitorStateException() {
        // A ReentrantLock misuse throws IllegalMonitorStateException with a null message and no
        // Kafka frames. Retrying that would only mask a defect of our own.
        AtomicInteger calls = new AtomicInteger();

        try {
            execute(() -> {
                calls.incrementAndGet();
                throw new IllegalMonitorStateException();
            }, new MonitorFaultRetry.Budget(), farFutureDeadline());
            Assert.fail("expected the exception to be rethrown");
        } catch (Exception expected) {
            Assert.assertTrue(expected instanceof IllegalMonitorStateException);
        }

        Assert.assertEquals(1, calls.get());
    }

    @Test
    public void stopsRetryingWhenTheThreadIsInterrupted() {
        AtomicInteger calls = new AtomicInteger();
        MonitorFaultRetry.Backoff interrupting = millis -> {
            throw new InterruptedException("shutting down");
        };

        try {
            MonitorFaultRetry.execute(() -> {
                calls.incrementAndGet();
                throw monitorFault();
            }, new MonitorFaultRetry.Budget(), farFutureDeadline(), "test-topic", 0, interrupting);
            Assert.fail("expected the fault to be rethrown");
        } catch (Exception expected) {
            Assert.assertTrue(KafkaErrorHelper.isMetadataMonitorFault(expected));
        }

        Assert.assertEquals("an interrupt must end the retry loop at once", 1, calls.get());
        Assert.assertTrue("the interrupt flag must be restored for the caller",
                Thread.interrupted());
    }

    @Test
    public void recoversWithinTheRecordLimit() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        MonitorFaultRetry.Budget budget = new MonitorFaultRetry.Budget();

        String result = execute(() -> {
            if (calls.getAndIncrement() < 2) {
                throw monitorFault();
            }
            return "ok";
        }, budget, farFutureDeadline());

        Assert.assertEquals("ok", result);
        Assert.assertEquals(3, calls.get());
        Assert.assertEquals(MonitorFaultRetry.MAX_RETRIES_PER_BATCH - 2, budget.remaining());
    }
}
