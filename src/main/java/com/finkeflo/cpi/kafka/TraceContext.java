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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * The headers that carry a trace across systems: W3C {@code traceparent} and {@code tracestate},
 * and the SAP Passport {@code sap-passport} (#168).
 */
public final class TraceContext {

    /** The MPL custom header property that carries the W3C trace ID of a consumed record. */
    public static final String MPL_TRACE_ID = "KafkaAdapterTraceId";

    /** Bounds the MPL custom header properties a batch of many traces writes. */
    static final int MAX_MPL_TRACE_IDS = 50;

    private static final String TRACEPARENT = "traceparent";

    /**
     * W3C Trace Context carries only IDs and flags. The SAP Passport can carry the user and
     * transaction of the original call, so it is not among these and is left to Allowed Headers.
     */
    private static final Set<String> W3C_HEADERS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList(TRACEPARENT, "tracestate")));

    private static final Set<String> HEADERS;
    static {
        Set<String> all = new HashSet<>(W3C_HEADERS);
        all.add("sap-passport");
        HEADERS = Collections.unmodifiableSet(all);
    }

    /** version-traceid-parentid-flags, lower-case hex, as in W3C Trace Context level 1. */
    private static final Pattern TRACEPARENT_FORMAT =
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
     * @return whether {@code headerName} is {@code traceparent} or {@code tracestate}, in any
     *         letter case: the headers the receiver forwards whatever a non-empty
     *         <em>Allowed Headers</em> says
     */
    public static boolean isW3cHeader(String headerName) {
        return headerName != null && W3C_HEADERS.contains(headerName.toLowerCase(Locale.ROOT));
    }

    /**
     * @return the 32 hex digits of the trace ID in {@code traceparent}, or {@code null} if it is
     *         not a valid W3C {@code traceparent}
     */
    public static String traceId(String traceparent) {
        if (traceparent == null) {
            return null;
        }
        Matcher m = TRACEPARENT_FORMAT.matcher(traceparent.trim());
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
     * @return the distinct trace IDs of the valid {@code traceparent} headers of {@code records},
     *         in record order, at most {@link #MAX_MPL_TRACE_IDS}
     */
    static Set<String> traceIds(Iterable<ConsumerRecord<byte[], byte[]>> records) {
        Set<String> ids = new LinkedHashSet<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            if (record.headers() == null) {
                continue;
            }
            for (Header header : record.headers()) {
                if (ids.size() >= MAX_MPL_TRACE_IDS) {
                    return ids;
                }
                if (TRACEPARENT.equalsIgnoreCase(header.key()) && header.value() != null) {
                    String id = traceId(new String(header.value(), StandardCharsets.UTF_8));
                    if (id != null) {
                        ids.add(id);
                    }
                }
            }
        }
        return ids;
    }

    /**
     * Removes every header named {@code name} in any letter case: Kafka compares header names
     * exactly, so {@code TraceParent} would otherwise sit next to {@code traceparent}.
     */
    static void removeIgnoringCase(Headers headers, String name) {
        List<String> keys = new ArrayList<>();
        for (Header header : headers) {
            if (header.key().equalsIgnoreCase(name)) {
                keys.add(header.key());
            }
        }
        for (String key : keys) {
            headers.remove(key);
        }
    }
}
