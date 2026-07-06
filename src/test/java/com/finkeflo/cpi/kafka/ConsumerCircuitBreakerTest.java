/*-
 * #%L
 * SAP CPI Kafka Adapter Plus
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

public class ConsumerCircuitBreakerTest {

    private DefaultCamelContext ctx;

    @Before
    public void setUp() throws Exception {
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", component);
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        if (ctx != null) {
            ctx.stop();
        }
    }

    @Test
    public void testRecordFailureReturnsFalseBeforeThreshold() throws Exception {
        ConsumerCircuitBreaker cb = createCircuitBreaker(3, 10);
        Assert.assertFalse("First failure should not trigger pause", cb.recordFailure());
        Assert.assertFalse("Second failure should not trigger pause", cb.recordFailure());
    }

    @Test
    public void testRecordFailureReturnsTrueAtThreshold() throws Exception {
        ConsumerCircuitBreaker cb = createCircuitBreaker(3, 10);
        cb.recordFailure();
        cb.recordFailure();
        Assert.assertTrue("Third failure should trigger pause", cb.recordFailure());
    }

    @Test
    public void testRecordSuccessResetsFailureCount() throws Exception {
        ConsumerCircuitBreaker cb = createCircuitBreaker(3, 10);
        cb.recordFailure();
        cb.recordFailure();
        cb.recordSuccess();
        Assert.assertFalse("After reset, first failure should not trigger pause", cb.recordFailure());
        Assert.assertFalse("After reset, second failure should not trigger pause", cb.recordFailure());
    }

    @Test
    public void testResetClearsAllState() throws Exception {
        ConsumerCircuitBreaker cb = createCircuitBreaker(2, 10);
        cb.recordFailure();
        cb.recordFailure(); // triggers pause
        cb.reset();
        Assert.assertFalse("After reset, first failure should not trigger pause", cb.recordFailure());
    }

    @Test
    public void testRecordFailureReturnsFalseWhenDisabled() throws Exception {
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=test-group"
                + "&autoPauseEnabled=false");
        AdapterTracingHelper tracingHelper = new AdapterTracingHelper(endpoint);
        ConsumerCircuitBreaker cb = new ConsumerCircuitBreaker(endpoint, tracingHelper);
        Assert.assertFalse(cb.recordFailure());
        Assert.assertFalse(cb.recordFailure());
        Assert.assertFalse(cb.recordFailure());
    }

    @Test
    public void testHandlePausedStateReturnsFalseWhenNotPaused() throws Exception {
        ConsumerCircuitBreaker cb = createCircuitBreaker(3, 10);
        Assert.assertFalse("Should not be paused when no failures",
                cb.handlePausedState(null));
    }

    private ConsumerCircuitBreaker createCircuitBreaker(int threshold, int cooldown) throws Exception {
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=test-group"
                + "&autoPauseEnabled=true&autoPauseErrorThreshold=" + threshold
                + "&autoPauseCooldownSeconds=" + cooldown);
        AdapterTracingHelper tracingHelper = new AdapterTracingHelper(endpoint);
        return new ConsumerCircuitBreaker(endpoint, tracingHelper);
    }
}
