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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The retry of a blocked partition is announced ({@code consumer.partition.retry}) and backed off
 * from the offset the partition resumes at, its first open record, which can lie before the record
 * that blocked it (#244).
 */
public class ConsumerPartitionRetryTest {

    private static final TopicPartition P0 = new TopicPartition("orders", 0);

    private DefaultCamelContext ctx;
    private CpiKafkaPlusConsumer consumer;

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:orders?bootstrapServers=localhost:9092&groupId=g&securityProtocol=PLAINTEXT");
        // Never started: without a Kafka consumer the rewind and the pause fail and are only logged.
        // What this test checks, the retry line and the backoff, comes after them.
        consumer = new CpiKafkaPlusConsumer(endpoint, exchange -> { });
        Field backoff = CpiKafkaPlusConsumer.class.getDeclaredField("retryBackoff");
        backoff.setAccessible(true);
        backoff.set(consumer, new PartitionBackoff(1000L, 300_000L));
    }

    @After
    public void tearDown() throws Exception {
        ctx.close();
    }

    @Test
    public void theRetryIsAnnouncedAtTheOffsetThePartitionResumesAt() throws Exception {
        // A failed batch of 100-103 in front of the record 104, whose DLQ write failed.
        String line = settle(progress(100, 105, 104));

        Assert.assertTrue(line, line.contains(" offset=100 blockedAt=104 retryInMs=1000 "));
    }

    @Test
    public void theBackoffGrowsWhileThePartitionStaysAtTheSameOffset() throws Exception {
        settle(progress(100, 105, 104));
        String again = settle(progress(100, 105, 103));

        Assert.assertTrue(again, again.contains(" offset=100 blockedAt=103 retryInMs=2000 "));
    }

    @Test
    public void theBackoffStartsOverOnceThePartitionMoved() throws Exception {
        settle(progress(100, 105, 104));
        PollProgress moved = progress(100, 105, 104);
        for (long offset = 100; offset < 104; offset++) {
            moved.resolved(P0, offset);
        }
        String line = settle(moved);

        Assert.assertTrue(line, line.contains(" offset=104 retryInMs=1000 "));
        Assert.assertFalse("blocked where it resumes: " + line, line.contains("blockedAt="));
    }

    /** One poll of the records {@code first..last} of P0, blocked at {@code blockedAt}. */
    private static PollProgress progress(long first, long last, long blockedAt) {
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (long offset = first; offset <= last; offset++) {
            records.add(new ConsumerRecord<>("orders", 0, offset, (byte[]) null, new byte[0]));
        }
        PollProgress progress = new PollProgress(new ConsumerRecords<>(Collections.singletonMap(P0, records),
                Collections.emptyMap()));
        progress.blocked(P0, blockedAt);
        return progress;
    }

    /** Settles the poll as the consumer does after processing it, and returns its retry line. */
    private String settle(PollProgress progress) throws Exception {
        Method settle = CpiKafkaPlusConsumer.class.getDeclaredMethod("settlePoll", PollProgress.class);
        settle.setAccessible(true);
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, "UTF-8"));
        try {
            settle.invoke(consumer, progress);
        } finally {
            System.setErr(original);
        }
        // Kafka clients left behind by other tests keep logging on their own threads.
        String own = "[" + Thread.currentThread().getName() + "] ";
        for (String line : new String(captured.toByteArray(), StandardCharsets.UTF_8).split("\\R")) {
            if (line.startsWith(own) && line.contains("consumer.partition.retry")) {
                return line;
            }
        }
        throw new AssertionError("no consumer.partition.retry line in:\n" + captured);
    }
}
