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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;

/**
 * Reading the trace IDs from W3C {@code traceparent} headers (#168).
 */
public class TraceContextTest {

    private static final String ID = "4bf92f3577b34da6a3ce929d0e0e4736";

    @Test
    public void aValidTraceparentGivesItsTraceId() {
        Assert.assertEquals(ID, TraceContext.traceId("00-" + ID + "-00f067aa0ba902b7-01"));
        Assert.assertEquals(ID, TraceContext.traceId(" 00-" + ID + "-00f067aa0ba902b7-00 "));
        Assert.assertEquals(ID, TraceContext.traceId("01-" + ID + "-00f067aa0ba902b7-01-future"));
    }

    @Test
    public void anInvalidTraceparentGivesNoTraceId() {
        Assert.assertNull(TraceContext.traceId(null));
        Assert.assertNull(TraceContext.traceId(""));
        Assert.assertNull(TraceContext.traceId("00-" + ID.toUpperCase() + "-00f067aa0ba902b7-01"));
        Assert.assertNull(TraceContext.traceId("ff-" + ID + "-00f067aa0ba902b7-01"));
        Assert.assertNull(TraceContext.traceId("00-00000000000000000000000000000000-00f067aa0ba902b7-01"));
        Assert.assertNull(TraceContext.traceId("00-" + ID + "-0000000000000000-01"));
        Assert.assertNull(TraceContext.traceId("00-" + ID + "-00f067aa0ba902b7-01-extra"));
    }

    @Test
    public void theTraceIdsOfRecordsAreDistinctValidAndBounded() {
        String other = "0af7651916cd43dd8448eb211c80319c";
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (String traceparent : new String[] {"00-" + ID + "-00f067aa0ba902b7-01", "garbage",
                "00-" + other + "-00f067aa0ba902b7-01", "00-" + ID + "-b7ad6b7169203331-01"}) {
            records.add(record(records.size(), "TraceParent", traceparent));
        }
        records.add(new ConsumerRecord<>("t", 0, records.size(), null, new byte[0]));

        Assert.assertEquals(Arrays.asList(ID, other), new ArrayList<>(TraceContext.traceIds(records)));

        List<ConsumerRecord<byte[], byte[]>> many = new ArrayList<>();
        for (int i = 1; i <= TraceContext.MAX_MPL_TRACE_IDS + 5; i++) {
            many.add(record(i, "traceparent", "00-" + String.format("%032x", i) + "-00f067aa0ba902b7-01"));
        }
        Assert.assertEquals(TraceContext.MAX_MPL_TRACE_IDS, TraceContext.traceIds(many).size());
    }

    private static ConsumerRecord<byte[], byte[]> record(long offset, String header, String value) {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("t", 0, offset, null, new byte[0]);
        record.headers().add(header, value.getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
