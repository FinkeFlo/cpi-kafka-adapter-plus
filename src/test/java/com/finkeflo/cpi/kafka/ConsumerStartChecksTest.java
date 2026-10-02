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
