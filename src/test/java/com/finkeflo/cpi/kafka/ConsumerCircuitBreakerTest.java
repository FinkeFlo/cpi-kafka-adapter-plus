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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConsumerCircuitBreakerTest {

    private static final java.util.function.Consumer<Throwable> NO_FAILURE = failure -> {
        throw new AssertionError("unexpected keep-alive failure", failure);
    };

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
    public void failuresOfOnePollAddUpTowardsTheThreshold() throws Exception {
        ConsumerCircuitBreaker cb = createCircuitBreaker(5, 10);
        Assert.assertFalse(cb.recordFailures(3));
        Assert.assertTrue("3 + 2 failures reach the threshold of 5", cb.recordFailures(2));
    }

    @Test
    public void zeroFailuresChangeNothing() throws Exception {
        ConsumerCircuitBreaker cb = createCircuitBreaker(1, 10);
        Assert.assertFalse(cb.recordFailures(0));
        Assert.assertTrue(cb.recordFailures(1));
    }

    @Test
    public void testHandlePausedStateReturnsFalseWhenNotPaused() throws Exception {
        ConsumerCircuitBreaker cb = createCircuitBreaker(3, 10);
        Assert.assertFalse("Should not be paused when no failures",
                cb.handlePausedState(null, NO_FAILURE));
    }

    @Test
    public void aCooldownBelowOneSecondIsClampedToOneSecond() throws Exception {
        ConsumerCircuitBreaker cb = createCircuitBreaker(1, 0);
        long before = System.currentTimeMillis();
        Assert.assertTrue(cb.recordFailure());
        java.lang.reflect.Field f = ConsumerCircuitBreaker.class.getDeclaredField("pausedUntil");
        f.setAccessible(true);
        Assert.assertTrue("a zero cooldown must still pause", (long) f.get(cb) >= before + 1000L);
    }

    @Test
    public void aThresholdBelowOneIsClampedToOne() throws Exception {
        Assert.assertEquals(1, createCircuitBreaker(-5, 10).effectiveThreshold());
        Assert.assertEquals(1L, createCircuitBreaker(1, -3).effectiveCooldownSeconds());
    }

    /**
     * N-D-1 (#230): a partition assigned while the auto-pause keep-alive polls is not paused yet, so
     * the poll returns its records. They must be rewound, otherwise the position is already past them
     * and the next emit poll never reads them.
     */
    @Test
    public void autoPauseKeepAliveRewindsRecordsOfAPartitionAssignedDuringThePoll() throws Exception {
        ConsumerCircuitBreaker cb = createCircuitBreaker(1, 60);
        Assert.assertTrue(cb.recordFailure());

        TopicPartition p0 = new TopicPartition("test-topic", 0);
        TopicPartition p1 = new TopicPartition("test-topic", 1);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
        consumer.subscribe(Collections.singleton("test-topic"));
        consumer.rebalance(Collections.singleton(p0));
        Map<TopicPartition, Long> beginning = new HashMap<>();
        beginning.put(p0, 0L);
        beginning.put(p1, 0L);
        consumer.updateBeginningOffsets(beginning);
        consumer.schedulePollTask(() -> {
            consumer.rebalance(Arrays.asList(p0, p1));
            consumer.addRecord(new ConsumerRecord<>("test-topic", 1, 0L, null, "a".getBytes()));
            consumer.addRecord(new ConsumerRecord<>("test-topic", 1, 1L, null, "b".getBytes()));
        });

        Assert.assertTrue("still in cooldown", cb.handlePausedState(consumer, NO_FAILURE));
        Assert.assertEquals("the records of the new partition must be read again after the pause",
                0L, consumer.position(p1));
    }

    /**
     * #243: a failed keep-alive poll during the cooldown goes to the consumer, which classifies it
     * like a failed emit poll, instead of only to DEBUG. The consumer stays paused.
     */
    @Test
    public void autoPauseKeepAliveHandsAFailedPollToTheConsumer() throws Exception {
        ConsumerCircuitBreaker cb = createCircuitBreaker(1, 60);
        Assert.assertTrue(cb.recordFailure());
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
        consumer.subscribe(Collections.singleton("test-topic"));
        SaslAuthenticationException failure = new SaslAuthenticationException("bad credentials");
        consumer.setPollException(failure);
        List<Throwable> handled = new ArrayList<>();

        Assert.assertTrue("still in cooldown", cb.handlePausedState(consumer, handled::add));
        Assert.assertEquals(Collections.singletonList(failure), handled);
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
