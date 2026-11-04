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

import org.junit.Assert;
import org.junit.Test;

/**
 * Reading the trace ID from a W3C {@code traceparent} (#168).
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
}
