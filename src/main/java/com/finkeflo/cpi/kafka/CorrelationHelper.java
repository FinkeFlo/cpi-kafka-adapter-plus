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

import org.apache.camel.Exchange;

/**
 * Extracts the identifiers that tie an adapter log line to one concrete integration-flow message.
 *
 * <p>Without them an adapter line cannot be connected to anything. The CPI tenant trace format
 * reserves four correlation fields per record, and in the trace that motivated this work all four
 * were empty on every single adapter line, across the whole corpus. A failure could be seen, but not
 * attributed to a message, a sender or a payload — which is most of what an investigation needs.
 *
 * <p>{@code SAP_MessageProcessingLogID} is the identifier the Message Processing Log itself is keyed
 * by, so it is what makes a log line searchable in the monitor. The Camel exchange id is always
 * available and serves as the fallback that at least groups all lines belonging to one exchange.
 */
final class CorrelationHelper {

    /** Header carrying the Message Processing Log id on the CPI runtime. */
    private static final String MPL_ID_HEADER = "SAP_MessageProcessingLogID";
    /** Header carrying the application message id, when the flow sets one. */
    private static final String APPLICATION_ID_HEADER = "SAP_ApplicationID";

    private CorrelationHelper() {}

    /**
     * Adds {@code mplId}, {@code exchangeId} and, when present, {@code applicationId} to an event.
     * Null-safe in every direction: a missing exchange, a missing header or a runtime that does not
     * set these at all simply yields fewer fields, never an exception on a failure path.
     */
    static AdapterDiagnostics.Event correlate(AdapterDiagnostics.Event event, Exchange exchange) {
        if (exchange == null) {
            return event;
        }
        return event
                .withOptional("mplId", header(exchange, MPL_ID_HEADER))
                .withOptional("applicationId", header(exchange, APPLICATION_ID_HEADER))
                .withOptional("exchangeId", exchange.getExchangeId());
    }

    /** The same identifiers as key/value pairs, for the Message Processing Log context maps. */
    static void addTo(java.util.Map<String, String> context, Exchange exchange) {
        if (context == null || exchange == null) {
            return;
        }
        putIfPresent(context, "mplId", header(exchange, MPL_ID_HEADER));
        putIfPresent(context, "applicationId", header(exchange, APPLICATION_ID_HEADER));
        putIfPresent(context, "exchangeId", exchange.getExchangeId());
    }

    private static void putIfPresent(java.util.Map<String, String> context, String key, String value) {
        if (value != null && !value.isEmpty()) {
            context.put(key, value);
        }
    }

    /**
     * Reads a header without ever throwing. This runs on failure paths, where a secondary exception
     * would replace the diagnostic it was meant to enrich.
     */
    private static String header(Exchange exchange, String name) {
        try {
            if (exchange.getIn() == null) {
                return null;
            }
            String value = exchange.getIn().getHeader(name, String.class);
            if (value == null) {
                Object property = exchange.getProperty(name);
                value = property != null ? String.valueOf(property) : null;
            }
            return value == null || value.isEmpty() ? null : value;
        } catch (Exception unavailable) {
            return null;
        }
    }
}
