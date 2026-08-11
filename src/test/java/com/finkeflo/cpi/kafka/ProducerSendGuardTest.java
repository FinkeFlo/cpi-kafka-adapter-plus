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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Assert;
import org.junit.Test;

/**
 * Guards the fix for the {@code Node Crashed} failure mode: Kafka enforces
 * {@code delivery.timeout.ms} from its sender thread, so a sender thread that died leaves the
 * record futures uncompleted forever. An unbounded wait then pins the CPI worker thread until the
 * node is declared dead — the symptom reported for a plaintext producer against a TLS-only broker.
 */
public class ProducerSendGuardTest {

    private static final long SHORT_BUDGET_MS = 150L;

    private static RecordMetadata metadata() {
        return new RecordMetadata(new TopicPartition("t", 0), 0L, 0, 0L, 0, 0);
    }

    @Test
    public void returnsResultWhenKafkaCompletesTheSend() throws Exception {
        ProducerSendGuard guard = ProducerSendGuard.of(SHORT_BUDGET_MS, "SASL_SSL");
        RecordMetadata expected = metadata();

        RecordMetadata actual = guard.await(
                CompletableFuture.completedFuture(expected), guard.newDeadline(), "Send");

        Assert.assertSame(expected, actual);
    }

    @Test
    public void stalledSendFailsWithinTheBudgetInsteadOfBlockingForever() {
        ProducerSendGuard guard = ProducerSendGuard.of(SHORT_BUDGET_MS, "SASL_PLAINTEXT");
        Future<RecordMetadata> neverCompletes = new CompletableFuture<>();

        long startMs = System.currentTimeMillis();
        try {
            guard.await(neverCompletes, guard.newDeadline(), "Send to topic 'orders'");
            Assert.fail("Expected the guard to give up on a future Kafka never completes");
        } catch (ProducerSendGuard.SendStalledException e) {
            long elapsedMs = System.currentTimeMillis() - startMs;
            Assert.assertTrue("Waited " + elapsedMs + " ms, budget was " + SHORT_BUDGET_MS + " ms",
                    elapsedMs < SHORT_BUDGET_MS * 10);
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Send to topic 'orders'"));
            Assert.assertTrue("Plaintext protocols must name the TLS mismatch: " + e.getMessage(),
                    e.getMessage().contains("SASL_SSL"));
        } catch (Exception e) {
            Assert.fail("Expected SendStalledException but got " + e);
        }
        Assert.assertTrue("The abandoned future must be cancelled", neverCompletes.isCancelled());
    }

    @Test
    public void tlsProtocolsGetABrokerHintInsteadOfTheTlsMismatchHint() {
        String message = ProducerSendGuard.of(SHORT_BUDGET_MS, "SASL_SSL").stalledMessage("Send");

        Assert.assertTrue(message, message.contains("broker availability"));
        Assert.assertFalse(message, message.contains("Use SASL_SSL"));
    }

    @Test
    public void batchSharesOneDeadlineSoRecordCountCannotMultiplyTheWait() {
        ProducerSendGuard guard = ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT");
        List<Future<RecordMetadata>> futures = Arrays.<Future<RecordMetadata>>asList(
                new CompletableFuture<RecordMetadata>(),
                new CompletableFuture<RecordMetadata>(),
                new CompletableFuture<RecordMetadata>());

        long startMs = System.currentTimeMillis();
        guard.awaitAllQuietly(futures, guard.newDeadline());
        long elapsedMs = System.currentTimeMillis() - startMs;

        Assert.assertTrue("Waited " + elapsedMs + " ms for " + futures.size()
                + " records on a " + SHORT_BUDGET_MS + " ms budget",
                elapsedMs < SHORT_BUDGET_MS * futures.size());
        for (Future<RecordMetadata> future : futures) {
            Assert.assertTrue("Every record of a stalled batch must be released",
                    future.isCancelled());
        }
    }

    @Test
    public void reportedSendFailuresKeepTheirOwnCause() {
        ProducerSendGuard guard = ProducerSendGuard.of(SHORT_BUDGET_MS, "PLAINTEXT");
        CompletableFuture<RecordMetadata> failed = new CompletableFuture<>();
        IllegalStateException cause = new IllegalStateException("broker said no");
        failed.completeExceptionally(cause);

        try {
            guard.await(failed, guard.newDeadline(), "Send");
            Assert.fail("Expected the original Kafka failure to propagate");
        } catch (ExecutionException e) {
            Assert.assertSame(cause, e.getCause());
        } catch (Exception e) {
            Assert.fail("Expected ExecutionException but got " + e);
        }
    }

    @Test
    public void budgetLeavesKafkasOwnDeliveryTimeoutRoomToReportFirst() {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setDeliveryTimeoutSeconds(60);

        ProducerSendGuard guard = ProducerSendGuard.forEndpoint(endpoint);

        Assert.assertEquals(60_000L + ProducerSendGuard.GUARD_MARGIN_MS, guard.getBudgetMs());
    }
}
