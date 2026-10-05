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

import org.apache.camel.impl.DefaultCamelContext;
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

        String message = startFailure(endpoint);

        Assert.assertTrue(message, message.contains("errorHandling"));
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
        assertRejected(e, "groupId");
    }

    @Test
    public void unknownCommitStrategyIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setCommitStrategy("MANUAL");
        assertRejected(e, "commitStrategy");
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
        assertRejected(e, "maxPollRecords");
    }

    @Test
    public void negativeFetchValuesAreRejected() throws Exception {
        CpiKafkaPlusEndpoint a = endpoint();
        a.setFetchMinBytes(-1);
        assertRejected(a, "fetchMinBytes");
        CpiKafkaPlusEndpoint b = endpoint();
        b.setFetchMinBytes(1);
        b.setFetchMaxWaitMs(-1);
        assertRejected(b, "fetchMaxWaitMs");
    }

    @Test
    public void negativeBatchTimeoutIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setBatchTimeout(-1);
        assertRejected(e, "batchTimeout");
    }

    @Test
    public void batchSizeBelowOneIsRejectedInBatchMode() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setBatchMode(true);
        e.setBatchOutputFormat("JSON_ARRAY");
        e.setBatchSize(0);
        assertRejected(e, "batchSize");
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
        assertRejected(withDlq, "dlqMaxRetries");
        CpiKafkaPlusEndpoint withoutDlq = endpoint();
        withoutDlq.setDlqEnabled(false);
        withoutDlq.setDlqMaxRetries(-1);
        assertStarts(withoutDlq);
    }

    @Test
    public void dlqTopicThatIsAlsoASourceTopicIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setDlqEnabled(true);
        e.setDlqMaxRetries(3);
        e.setDlqTopic(" orders ");
        assertRejected(e, "dlqTopic");
    }

    @Test
    public void dlqTopicComparisonIsCaseSensitiveLikeKafka() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setDlqEnabled(true);
        e.setDlqMaxRetries(3);
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

    private void assertRejected(CpiKafkaPlusEndpoint endpoint, String field) throws Exception {
        String message = startFailure(endpoint);
        Assert.assertTrue(message, message.contains(field));
    }

    private CpiKafkaPlusEndpoint endpoint() throws Exception {
        return (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:orders?bootstrapServers=localhost:9092&groupId=g&securityProtocol=PLAINTEXT");
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
}
