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

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper for CPI adapter tracing, message processing log enrichment, and connection monitoring.
 * Uses reflection to access CPI runtime APIs that are only available at runtime.
 *
 * <p>Diagnostic enrichment uses three complementary channels, each with different visibility rules:
 * <ol>
 *   <li><b>Custom Header Properties</b> ({@code addCustomHeaderProperty}): Trace-independent,
 *       always written regardless of whether trace is active. Searchable via Monitor UI and OData
 *       (MessageProcessingLogCustomHeaderProperties). This is the PRIMARY channel for structured
 *       error fields.</li>
 *   <li><b>Adapter Attributes</b> ({@code putAdapterAttribute}): Also trace-independent. Called
 *       unconditionally per SAP blog examples. Secondary channel for the same fields.</li>
 *   <li><b>Trace Messages</b> ({@code writeTrace}): Only written when {@code isTraceActive()}
 *       returns true (auto-reverts after 10 minutes). Used for full payload/error block dumps.</li>
 *   <li><b>Attachments</b> ({@code addAttachmentAsString}): Always available. Used for the full
 *       serialised error block so the diagnostic line stays bounded.</li>
 *   <li><b>Status Events</b> ({@code fireStatusEvent(FAILED)}): Marks the message as failed in
 *       the MPL without requiring trace level.</li>
 * </ol>
 *
 * <p>Design rationale: MPL tracing was never actually active in analysed production traces, so
 * trace-only enrichment is not sufficient. Custom header properties and adapter attributes are
 * the trace-independent channels that survive regardless.
 */
public class AdapterTracingHelper {

    private static final Logger LOG = LoggerFactory.getLogger(AdapterTracingHelper.class);
    private static final String COMPONENT_ID = "ctype::Adapter/cname::kafkaAdapterPlus/vendor::FinkeFlo/version::0.0.1";
    private static final int MAX_TRACE_PAYLOAD_BYTES = 25 * 1024 * 1024;
    /** Attachment name for the full error diagnostic block. */
    private static final String ERROR_ATTACHMENT_NAME = "KafkaAdapterError";

    private final CpiKafkaPlusEndpoint endpoint;
    /**
     * Whether the ADK message-log classes exist at all. False off-platform (unit tests, local runs),
     * where the absence is expected and must stay quiet.
     */
    private final boolean adkMessageLogPresent;
    /** Guards the unavailability report so each distinct binding defect is stated once, not per message. */
    private final java.util.Set<String> unavailabilityReported =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    public AdapterTracingHelper(CpiKafkaPlusEndpoint endpoint) {
        this.endpoint = endpoint;
        this.adkMessageLogPresent = isAdkMessageLogPresent();
    }

    private static boolean isAdkMessageLogPresent() {
        try {
            Class.forName("com.sap.it.api.ITApiFactory");
            Class.forName("com.sap.it.api.msglog.adapter.AdapterMessageLogFactory");
            return true;
        } catch (Throwable notOnPlatform) {
            return false;
        }
    }

    /**
     * Resolves the ADK message-log factory for one exchange.
     *
     * <p>This replaces a lookup that could never have worked: the factory was fetched with
     * {@code camelContext.getRegistry().lookupByName("com.sap.it.api.msglog.adapter.AdapterMessageLogFactory")}.
     * ADK APIs are not Camel registry beans. {@code ITApiFactory} resolves them through an OSGi
     * Declarative Services component that binds {@code ITApiHandler} services by their {@code apiType}
     * service property — a different mechanism entirely, with no named bean involved. The lookup
     * therefore always returned {@code null}, tracing was permanently disabled, and the fact was
     * reported at DEBUG, which never reaches the tenant trace file. The dead lookup is removed
     * rather than kept as a fallback, because it cannot succeed.
     *
     * <p>{@code CredentialHelper} already uses the correct {@code ITApiFactory} pattern, so the
     * mechanism is proven in this codebase.
     *
     * <p>Resolution happens per exchange by design: {@code getApi} takes the exchange as its context
     * argument, so the handle cannot be cached at construction time the way the old code assumed.
     */
    private Object resolveMessageLogFactory(Exchange exchange) {
        if (!adkMessageLogPresent || exchange == null) {
            return null;
        }
        try {
            Class<?> itApiFactoryClass = Class.forName("com.sap.it.api.ITApiFactory");
            Class<?> factoryInterface =
                    Class.forName("com.sap.it.api.msglog.adapter.AdapterMessageLogFactory");
            Method getApi = itApiFactoryClass.getMethod("getApi", Class.class, Object.class);
            Object factory = getApi.invoke(null, factoryInterface, exchange);
            if (factory == null) {
                reportUnavailableOnce("ITApiFactory.getApi returned null for AdapterMessageLogFactory", null);
            }
            return factory;
        } catch (Exception e) {
            reportUnavailableOnce("ITApiFactory.getApi failed for AdapterMessageLogFactory", e);
            return null;
        }
    }

    /**
     * Reports a dead Message Processing Log binding once per distinct failure, at ERROR.
     *
     * <p>ERROR because only ERROR reaches the CPI tenant trace file: at DEBUG — where this used to
     * be — a broken binding is indistinguishable from a working one, which is precisely how the
     * registry lookup above stayed broken unnoticed. Once per key, because the alternative is one
     * line per message, but we still want each distinct binding defect to be reported.
     */
    private void reportUnavailableOnce(String what, Throwable cause) {
        if (!unavailabilityReported.add(what)) {
            return;  // already reported this exact failure
        }
        AdapterDiagnostics.Event event = AdapterDiagnostics.event("adapter.mpl.unavailable")
                .with("detail", what)
                .with("consequence", "no MPL traces for this endpoint");
        if (cause != null) {
            AdapterDiagnostics.error(LOG, event, cause);
        } else {
            AdapterDiagnostics.error(LOG, event);
        }
    }

    /**
     * Trace an inbound message (before transformation).
     */
    public void traceInbound(Exchange exchange, String body) {
        if (body == null) return;
        writeTrace(exchange, body.getBytes(StandardCharsets.UTF_8),
                "SENDER_INBOUND", "Receiving CPI Kafka Connector message");
    }

    /**
     * Trace an outbound message (Receiver direction - sending to Kafka).
     */
    public void traceOutbound(Exchange exchange, byte[] body) {
        if (body == null) return;
        writeTrace(exchange, body, "RECEIVER_OUTBOUND", "Sending CPI Kafka Connector message");
    }

    /**
     * Writes a structured error trace to the CPI Message Processing Log (visible when trace level
     * is active). Includes the exception type, message, cause chain, and any caller-supplied
     * context entries (e.g. topic, transactionalId, slotId).
     *
     * <p>This is intentionally generic — callers supply only the context they know. No hint text
     * is hardcoded here; that belongs in higher-level helpers if needed.
     *
     * @param exchange  the current Camel exchange (used to look up the MPL log handle)
     * @param e         the exception that triggered the failure
     * @param context   additional key-value pairs to include in the trace (may be empty, not null)
     */
    public void traceError(Exchange exchange, Exception e, Map<String, String> context) {
        traceError(exchange, e, context, false);
    }

    /**
     * Writes a structured error trace for a sender (consumer) direction failure.
     *
     * @param exchange  the current Camel exchange
     * @param e         the exception that triggered the failure
     * @param context   additional key-value pairs (may be empty or null)
     * @param senderDirection  true for consumer/sender direction (SENDER_OUTBOUND_FAULT),
     *                         false for producer/receiver direction (RECEIVER_INBOUND_FAULT)
     */
    public void traceError(Exchange exchange, Exception e, Map<String, String> context,
                           boolean senderDirection) {
        // ADR 0004: diagnostic code must never throw — guard entire body
        try {
            Map<String, String> safeContext = context != null ? context : java.util.Collections.emptyMap();
            StringBuilder sb = new StringBuilder();
            sb.append("ERROR: ").append(AdapterDiagnostics.describeThrowable(e));

            if (!safeContext.isEmpty()) {
                sb.append("\n\n--- Context ---");
                for (Map.Entry<String, String> entry : safeContext.entrySet()) {
                    sb.append('\n').append(entry.getKey()).append(": ").append(entry.getValue());
                }
            }

            String traceType = senderDirection ? "SENDER_OUTBOUND_FAULT" : "RECEIVER_INBOUND_FAULT";
            String logMessage = senderDirection ? "Kafka consume failed" : "Kafka send failed";
            writeTrace(exchange, sb.toString().getBytes(StandardCharsets.UTF_8), traceType, logMessage);
        } catch (Exception traceError) {
            // Never let diagnostic code replace the exception it was meant to enrich
            reportUnavailableOnce("traceError failed", traceError);
        }
    }

    /**
     * Reports a failure to the MPL with full diagnostic enrichment: custom header properties,
     * adapter attributes, an attachment with the full error block, and optionally a FAILED status
     * event. This is the central failure-reporting method that surfaces structured fields to the
     * MPL independently of whether trace is on.
     *
     * <p>Enrichment channels used (f1/f4/f5):
     * <ul>
     *   <li>{@code addCustomHeaderProperty}: errorCode, topic, producerPath, retryable — PRIMARY
     *       channel, trace-independent, searchable via Monitor UI and OData.</li>
     *   <li>{@code putAdapterAttribute}: Same fields — secondary channel, also trace-independent.</li>
     *   <li>{@code addAttachmentAsString}: Full serialised error block as attachment.</li>
     *   <li>{@code fireStatusEvent(FAILED)}: Marks the message as failed in the MPL when
     *       {@code fireStatusEvent} is true.</li>
     * </ul>
     *
     * @param exchange         the current Camel exchange
     * @param e                the exception that triggered the failure
     * @param errorCode        a structured error code string (e.g. "KAFKA_SEND_TIMEOUT") — may be
     *                         null if no error code is available yet (e.g. before CpiKafkaPlusErrorCode
     *                         is created by another agent)
     * @param context          additional key-value pairs for the diagnostic (may be empty)
     * @param fireStatusEvent  true to also fire {@code AdapterStatusEvent.FAILED}, which marks the
     *                         message as failed in the MPL (requires getMessageLogWithStatus)
     */
    public void reportFailure(Exchange exchange, Exception e, String errorCode,
                               Map<String, String> context, boolean fireStatusEvent) {
        // Always emit ERROR log with errorCode — this is the primary diagnostic channel because
        // only ERROR reaches the CPI tenant trace file, and MPL tracing is often inactive.
        // The errorCode enables grep-based triage on the trace file.
        Map<String, String> safeContext = context != null ? context : java.util.Collections.emptyMap();
        String topic = safeContext.containsKey("topic")
                ? safeContext.get("topic")
                : getEffectiveTopicSafe();
        AdapterDiagnostics.Event errorEvent = AdapterDiagnostics.event("adapter.failure.reported")
                .with("topic", topic);
        if (errorCode != null) {
            errorEvent.with("errorCode", errorCode);
        }
        if (safeContext.containsKey("partition")) {
            errorEvent.with("partition", safeContext.get("partition"));
        }
        if (safeContext.containsKey("offset")) {
            errorEvent.with("offset", safeContext.get("offset"));
        }
        errorEvent.with("retryable", isRetryableSafe(e) ? "true" : "false");
        AdapterDiagnostics.error(LOG, errorEvent, e);

        if (!adkMessageLogPresent || exchange == null) {
            return;
        }
        Object factory = resolveMessageLogFactory(exchange);
        if (factory == null) {
            return;
        }

        // ADR 0004: all preparation must be inside the try — nothing before can throw
        // and replace the original exception this diagnostic was meant to enrich.
        Object mplLog = null;
        boolean useStatusVariant = fireStatusEvent;

        try {
            // Build the full diagnostic block for the attachment (null-safe)
            String fullDiagnostic = buildFullDiagnosticSafe(e, errorCode, safeContext);

            // Determine retryability (null-safe)
            String retryable = isRetryableSafe(e) ? "true" : "false";

            // Extract producerPath from context if present
            String producerPath = safeContext.get("producerPath");

            Class<?> factoryInterface =
                    Class.forName("com.sap.it.api.msglog.adapter.AdapterMessageLogFactory");
            Class<?> messageLogInterface =
                    Class.forName("com.sap.it.api.msglog.adapter.AdapterMessageLog");
            Class<?> baseMessageLogInterface =
                    Class.forName("com.sap.it.api.msglog.MessageLog");

            if (useStatusVariant) {
                // f5: Use getMessageLogWithStatus for firing the FAILED event
                Method getMessageLogWithStatusMethod = factoryInterface.getMethod(
                        "getMessageLogWithStatus",
                        Object.class, String.class, String.class, String.class);
                mplLog = getMessageLogWithStatusMethod.invoke(factory, exchange,
                        "Kafka adapter failure", COMPONENT_ID, UUID.randomUUID().toString());
            } else {
                Method getMessageLogMethod = factoryInterface.getMethod("getMessageLog",
                        Object.class, String.class, String.class, String.class);
                mplLog = getMessageLogMethod.invoke(factory, exchange,
                        "Kafka adapter failure", COMPONENT_ID, UUID.randomUUID().toString());
            }

            if (mplLog == null) {
                return;
            }

            try {
                // f1: PRIMARY channel — addCustomHeaderProperty (trace-independent)
                Method addCustomHeaderProperty = baseMessageLogInterface.getMethod(
                        "addCustomHeaderProperty", String.class, String.class);
                if (errorCode != null) {
                    addCustomHeaderProperty.invoke(mplLog, "KafkaAdapterErrorCode", errorCode);
                }
                addCustomHeaderProperty.invoke(mplLog, "KafkaAdapterTopic", topic);
                if (producerPath != null) {
                    addCustomHeaderProperty.invoke(mplLog, "KafkaAdapterProducerPath", producerPath);
                }
                addCustomHeaderProperty.invoke(mplLog, "KafkaAdapterRetryable", retryable);

                // f1: Secondary channel — putAdapterAttribute (also trace-independent)
                Method putAdapterAttribute = messageLogInterface.getMethod(
                        "putAdapterAttribute", String.class, String.class);
                if (errorCode != null) {
                    putAdapterAttribute.invoke(mplLog, "errorCode", errorCode);
                }
                putAdapterAttribute.invoke(mplLog, "topic", topic);
                if (producerPath != null) {
                    putAdapterAttribute.invoke(mplLog, "producerPath", producerPath);
                }
                putAdapterAttribute.invoke(mplLog, "retryable", retryable);

                // f4: Attachment with full error block
                Method addAttachmentAsString = baseMessageLogInterface.getMethod(
                        "addAttachmentAsString", String.class, String.class, String.class);
                addAttachmentAsString.invoke(mplLog, ERROR_ATTACHMENT_NAME, fullDiagnostic, "text/plain");

                // f5: Fire FAILED status event if requested
                if (useStatusVariant) {
                    Class<?> statusEventClass =
                            Class.forName("com.sap.it.api.msglog.adapter.AdapterStatusEvent");
                    Class<?> logWithStatusInterface =
                            Class.forName("com.sap.it.api.msglog.adapter.AdapterMessageLogWithStatus");
                    @SuppressWarnings("unchecked")
                    Object failedEvent = Enum.valueOf((Class<Enum>) statusEventClass, "FAILED");
                    String statusMessage = buildStatusMessage(errorCode, e);
                    Method fireStatusEventMethod = logWithStatusInterface.getMethod(
                            "fireStatusEvent", statusEventClass, String.class);
                    fireStatusEventMethod.invoke(mplLog, failedEvent, statusMessage);
                }

                LOG.debug("MPL failure reported: errorCode={} topic={} retryable={} fireStatusEvent={}",
                        errorCode, topic, retryable, useStatusVariant);
            } finally {
                // f5: Always close the log with status variant.
                // CRITICAL: Guard the close so a failure here cannot mask a primary exception.
                // An exception in finally replaces any in-flight exception from the try body.
                if (useStatusVariant && mplLog != null) {
                    try {
                        Class<?> logWithStatusInterface =
                                Class.forName("com.sap.it.api.msglog.adapter.AdapterMessageLogWithStatus");
                        Method closeMethod = logWithStatusInterface.getMethod("close");
                        closeMethod.invoke(mplLog);
                    } catch (Exception closeError) {
                        // Report separately — do not let this displace the actual binding defect
                        reportUnavailableOnce("reportFailure close failed", closeError);
                    }
                }
            }
        } catch (Exception reflectionError) {
            reportUnavailableOnce("reportFailure reflection failed", reflectionError);
        }
    }

    /** Builds a status message from errorCode and exception, null-safe, with length limit. */
    private String buildStatusMessage(String errorCode, Exception e) {
        StringBuilder sb = new StringBuilder();
        if (errorCode != null) {
            sb.append(errorCode).append(": ");
        }
        if (e != null && e.getMessage() != null) {
            sb.append(e.getMessage());
        } else if (e != null) {
            sb.append(e.getClass().getSimpleName());
        } else {
            sb.append("Unknown error");
        }
        String statusMessage = sb.toString();
        // Truncate status message to avoid exceeding any platform limits
        if (statusMessage.length() > 500) {
            statusMessage = statusMessage.substring(0, 497) + "...";
        }
        return statusMessage;
    }

    /** Null-safe wrapper for RecordProcessor.isRetryable. */
    private boolean isRetryableSafe(Exception e) {
        if (e == null) {
            return false;
        }
        try {
            return RecordProcessor.isRetryable(e);
        } catch (Exception ignored) {
            return false;
        }
    }

    /** Null-safe wrapper for endpoint.getEffectiveTopic(). */
    private String getEffectiveTopicSafe() {
        try {
            return endpoint != null ? endpoint.getEffectiveTopic() : "unknown";
        } catch (Exception ignored) {
            return "unknown";
        }
    }

    /** Null-safe wrapper for building the full diagnostic. */
    private String buildFullDiagnosticSafe(Exception e, String errorCode, Map<String, String> context) {
        try {
            return buildFullDiagnostic(e, errorCode, context);
        } catch (Exception ignored) {
            return "Failed to build diagnostic for: " + (errorCode != null ? errorCode : "unknown error");
        }
    }

    /**
     * Builds the full diagnostic block for attachment (null-safe for context).
     */
    private String buildFullDiagnostic(Exception e, String errorCode, Map<String, String> context) {
        Map<String, String> safeContext = context != null ? context : java.util.Collections.emptyMap();
        StringBuilder sb = new StringBuilder();
        sb.append("=== Kafka Adapter Plus Diagnostic ===\n\n");
        if (errorCode != null) {
            sb.append("Error Code: ").append(errorCode).append('\n');
        }
        sb.append("Exception: ").append(AdapterDiagnostics.describeThrowable(e)).append("\n\n");

        if (!safeContext.isEmpty()) {
            sb.append("--- Context ---\n");
            for (Map.Entry<String, String> entry : safeContext.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
            }
            sb.append('\n');
        }

        // Add correlation IDs from CorrelationHelper patterns (all null-safe)
        sb.append("--- Correlation ---\n");
        sb.append("endpoint.topic: ").append(getEffectiveTopicSafe()).append('\n');
        sb.append("endpoint.groupId: ").append(getGroupIdSafe()).append('\n');
        sb.append("endpoint.bootstrapServers: ").append(getBootstrapServersSafe()).append('\n');

        return sb.toString();
    }

    /** Null-safe wrapper for endpoint.getGroupId(). */
    private String getGroupIdSafe() {
        try {
            if (endpoint != null && endpoint.getGroupId() != null) {
                return endpoint.getGroupId();
            }
        } catch (Exception ignored) {
            // Fall through to default
        }
        return "N/A";
    }

    /** Null-safe wrapper for endpoint.getBootstrapServers(). */
    private String getBootstrapServersSafe() {
        try {
            return endpoint != null ? endpoint.getBootstrapServers() : "unknown";
        } catch (Exception ignored) {
            return "unknown";
        }
    }

    @SuppressWarnings("unchecked")
    private void writeTrace(Exchange exchange, byte[] traceData, String enumValue, String logMessage) {
        if (traceData == null || traceData.length == 0) {
            return;
        }
        Object adapterMessageLogFactory = resolveMessageLogFactory(exchange);
        if (adapterMessageLogFactory == null) {
            return;
        }

        try {
            // Every method below is resolved on the ADK *interface*, never on the implementation
            // class. Two reasons, both of which had already broken this code silently:
            //
            //  - getMethod requires an exact parameter-type match. The factory declares
            //    getMessageLog(Object, ...), so looking it up with Exchange.class threw
            //    NoSuchMethodException — the same defect class as the historical
            //    setException(Exception.class) bug, and equally invisible because the failure was
            //    swallowed at DEBUG.
            //  - The implementation classes behind these interfaces are internal to the platform.
            //    A Method obtained from a non-public class fails at invoke() with
            //    IllegalAccessException even when the method itself is public.
            Class<?> factoryInterface =
                    Class.forName("com.sap.it.api.msglog.adapter.AdapterMessageLogFactory");
            Class<?> messageLogInterface =
                    Class.forName("com.sap.it.api.msglog.adapter.AdapterMessageLog");
            Class<?> traceMessageInterface =
                    Class.forName("com.sap.it.api.msglog.adapter.AdapterTraceMessage");
            Class<?> traceMessageTypeClass =
                    Class.forName("com.sap.it.api.msglog.adapter.AdapterTraceMessageType");

            Method getMessageLogMethod = factoryInterface.getMethod("getMessageLog",
                    Object.class, String.class, String.class, String.class);

            Object mplLog = getMessageLogMethod.invoke(adapterMessageLogFactory,
                    exchange, logMessage, COMPONENT_ID, UUID.randomUUID().toString());

            if (mplLog == null) return;

            Method isTraceActiveMethod = messageLogInterface.getMethod("isTraceActive");
            Boolean isActive = (Boolean) isTraceActiveMethod.invoke(mplLog);
            if (!Boolean.TRUE.equals(isActive)) return;

            Object traceType = Enum.valueOf((Class<Enum>) traceMessageTypeClass, enumValue);

            boolean isTruncated = traceData.length > MAX_TRACE_PAYLOAD_BYTES;
            if (isTruncated) {
                byte[] truncated = new byte[MAX_TRACE_PAYLOAD_BYTES];
                System.arraycopy(traceData, 0, truncated, 0, truncated.length);
                traceData = truncated;
            }

            Method createTraceMethod = messageLogInterface.getMethod("createTraceMessage",
                    traceMessageTypeClass, byte[].class, boolean.class);
            Object traceMessage = createTraceMethod.invoke(mplLog, traceType, traceData, isTruncated);

            Method setEncodingMethod = traceMessageInterface.getMethod("setEncoding", String.class);
            setEncodingMethod.invoke(traceMessage, "UTF-8");

            Method writeTraceMethod = messageLogInterface.getMethod("writeTrace", traceMessageInterface);
            writeTraceMethod.invoke(mplLog, traceMessage);

            LOG.debug("Trace written for {} ({} bytes)", logMessage, traceData.length);
        } catch (Exception e) {
            // Reported once at ERROR rather than per message at DEBUG: a broken ADK binding is
            // invisible at DEBUG, which is how the factory lookup stayed broken unnoticed.
            reportUnavailableOnce("writing an MPL trace failed", e);
        }
    }

    /**
     * Publish connection status event to CPI monitoring.
     */
    public void publishConnectionStatus(boolean success, Throwable error) {
        Object monitorService;
        try {
            monitorService = lookupMonitorService();
        } catch (Exception e) {
            // Not running on CPI (ITApiFactory / IFlowMonitorService absent) — expected off-platform.
            LOG.debug("IFlow monitor service not available: {}", e.toString());
            return;
        }
        if (monitorService == null) {
            return;
        }
        try {
            Class<?> monitorServiceClass =
                    Class.forName("com.sap.it.api.adapter.iflowmonitoring.IFlowMonitorService");
            Object eventDetails = buildEventDetails(success, error);
            Method publishMethod = monitorServiceClass.getMethod("publishEvent",
                    Endpoint.class, eventDetails.getClass());
            publishMethod.invoke(monitorService, endpoint, eventDetails);
            LOG.debug("Published connection status: {}", success ? "OK" : "ERROR");
        } catch (Exception e) {
            // WARN, not DEBUG: a silent failure here previously masked a wrong reflection signature
            // (setException lookup) so ERROR events were dropped for the entire deployment lifetime.
            // Keep this visible so a future ADK API change surfaces instead of hiding.
            LOG.warn("Could not publish connection status ({}): {}",
                    success ? "OK" : "ERROR", e.toString());
        }
    }

    private Object lookupMonitorService() throws Exception {
        Class<?> itApiFactoryClass = Class.forName("com.sap.it.api.ITApiFactory");
        Method getServiceMethod = itApiFactoryClass.getMethod("getService", Class.class, Object.class);
        Class<?> monitorServiceClass =
                Class.forName("com.sap.it.api.adapter.iflowmonitoring.IFlowMonitorService");
        return getServiceMethod.invoke(null, monitorServiceClass, null);
    }

    /**
     * Builds the ADK {@code EventDetails} object via reflection. Package-private so it can be
     * unit-tested against the real ADK API classes (provided scope, present on the test classpath)
     * without needing a live CPI {@code IFlowMonitorService}.
     *
     * <p>The exception is attached via {@code setException(Throwable)} — the ADK declares the
     * parameter as {@link Throwable}, NOT {@link Exception}. Looking the method up with
     * {@code Exception.class} throws {@link NoSuchMethodException} ({@code getMethod} requires an
     * exact parameter-type match), which silently dropped every ERROR event before this fix.
     */
    @SuppressWarnings("unchecked")
    Object buildEventDetails(boolean success, Throwable error) throws Exception {
        Class<?> eventDetailsClass =
                Class.forName("com.sap.it.api.adapter.iflowmonitoring.EventDetails");
        Object eventDetails = eventDetailsClass.getDeclaredConstructor().newInstance();

        Class<?> eventStatusClass =
                Class.forName("com.sap.it.api.adapter.iflowmonitoring.EventStatus");
        Object status = success
                ? Enum.valueOf((Class<Enum>) eventStatusClass, "OK")
                : Enum.valueOf((Class<Enum>) eventStatusClass, "ERROR");
        eventDetailsClass.getMethod("setEventStatus", eventStatusClass).invoke(eventDetails, status);

        if (!success && error != null) {
            Method setExceptionMethod = eventDetailsClass.getMethod("setException", Throwable.class);
            setExceptionMethod.invoke(eventDetails, error);
        }
        return eventDetails;
    }
}
