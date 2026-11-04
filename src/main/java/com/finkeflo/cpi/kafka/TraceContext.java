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
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.camel.Exchange;
import org.apache.camel.Message;

/**
 * The headers that carry a trace across systems: W3C {@code traceparent} and {@code tracestate},
 * and the SAP Passport (#168).
 */
public final class TraceContext {

    /** The MPL custom header property that carries the W3C trace ID of a consumed record. */
    public static final String MPL_TRACE_ID = "KafkaAdapterTraceId";

    /**
     * SAP's exchange property that switches off the W3C trace context for outbound calls. Set to
     * {@code true}, the receiver no longer forwards the trace context headers on its own.
     */
    static final String DISABLE_PROPERTY = "SAP_DisableExternalW3CTraceContextTransmission";

    private static final Set<String> HEADERS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList("traceparent", "tracestate", "sap-passport")));

    /** version-traceid-parentid-flags, lower-case hex, as in W3C Trace Context level 1. */
    private static final Pattern TRACEPARENT =
            Pattern.compile("([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})(-.*)?");
    private static final String INVALID_TRACE_ID = "00000000000000000000000000000000";
    private static final String INVALID_PARENT_ID = "0000000000000000";

    private TraceContext() {
        // static utility class
    }

    /**
     * @return whether {@code headerName} is {@code traceparent}, {@code tracestate} or
     *         {@code sap-passport}, in any letter case
     */
    public static boolean isTraceContextHeader(String headerName) {
        return headerName != null && HEADERS.contains(headerName.toLowerCase(Locale.ROOT));
    }

    /**
     * @return the 32 hex digits of the trace ID in {@code traceparent}, or {@code null} if it is
     *         not a valid W3C {@code traceparent}
     */
    public static String traceId(String traceparent) {
        if (traceparent == null) {
            return null;
        }
        Matcher m = TRACEPARENT.matcher(traceparent.trim());
        if (!m.matches() || "ff".equals(m.group(1))
                || INVALID_TRACE_ID.equals(m.group(2)) || INVALID_PARENT_ID.equals(m.group(3))) {
            return null;
        }
        // Version 00 has exactly four fields; only later versions may append more.
        if ("00".equals(m.group(1)) && m.group(5) != null) {
            return null;
        }
        return m.group(2);
    }

    /**
     * @return whether the receiver forwards the trace context headers whatever
     *         <em>Allowed Headers</em> says: unless the flow set {@link #DISABLE_PROPERTY}
     */
    static boolean forwardAlways(Message message) {
        Exchange exchange = message.getExchange();
        if (exchange == null) {
            return true;
        }
        Object disabled = exchange.getProperty(DISABLE_PROPERTY);
        return disabled == null || !"true".equalsIgnoreCase(disabled.toString().trim());
    }
}
