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

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper for CPI adapter tracing and connection monitoring.
 * Uses reflection to access CPI runtime APIs that are only available at runtime.
 */
public class AdapterTracingHelper {

    private static final Logger LOG = LoggerFactory.getLogger(AdapterTracingHelper.class);
    private static final String COMPONENT_ID = "ctype::Adapter/cname::kafkaAdapterPlus/vendor::FinkeFlo/version::0.0.1";
    private static final int MAX_TRACE_PAYLOAD_BYTES = 25 * 1024 * 1024;

    private final CpiKafkaPlusEndpoint endpoint;
    private Object adapterMessageLogFactory;
    private boolean tracingAvailable;

    public AdapterTracingHelper(CpiKafkaPlusEndpoint endpoint) {
        this.endpoint = endpoint;
        initTracingFactory();
    }

    private void initTracingFactory() {
        try {
            String factoryClassName = "com.sap.it.api.msglog.adapter.AdapterMessageLogFactory";
            adapterMessageLogFactory = endpoint.getCamelContext().getRegistry().lookupByName(factoryClassName);
            tracingAvailable = (adapterMessageLogFactory != null);
            if (tracingAvailable) {
                LOG.debug("Adapter tracing factory initialized");
            } else {
                LOG.debug("Adapter tracing not available (not running on CPI)");
            }
        } catch (Exception e) {
            LOG.debug("Adapter tracing not available: {}", e.getMessage());
            tracingAvailable = false;
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

    @SuppressWarnings("unchecked")
    private void writeTrace(Exchange exchange, byte[] traceData, String enumValue, String logMessage) {
        if (!tracingAvailable || traceData == null || traceData.length == 0) return;

        try {
            Class<?> factoryClass = adapterMessageLogFactory.getClass();
            Method getMessageLogMethod = factoryClass.getMethod("getMessageLog",
                    Exchange.class, String.class, String.class, String.class);

            Object mplLog = getMessageLogMethod.invoke(adapterMessageLogFactory,
                    exchange, logMessage, COMPONENT_ID, UUID.randomUUID().toString());

            if (mplLog == null) return;

            Method isTraceActiveMethod = mplLog.getClass().getMethod("isTraceActive");
            Boolean isActive = (Boolean) isTraceActiveMethod.invoke(mplLog);
            if (!Boolean.TRUE.equals(isActive)) return;

            Class<?> traceMessageTypeClass = Class.forName("com.sap.it.api.msglog.adapter.AdapterTraceMessageType");
            Object traceType = Enum.valueOf((Class<Enum>) traceMessageTypeClass, enumValue);

            boolean isTruncated = traceData.length > MAX_TRACE_PAYLOAD_BYTES;
            if (isTruncated) {
                byte[] truncated = new byte[MAX_TRACE_PAYLOAD_BYTES];
                System.arraycopy(traceData, 0, truncated, 0, truncated.length);
                traceData = truncated;
            }

            Method createTraceMethod = mplLog.getClass().getMethod("createTraceMessage",
                    traceMessageTypeClass, byte[].class, boolean.class);
            Object traceMessage = createTraceMethod.invoke(mplLog, traceType, traceData, isTruncated);

            Method setEncodingMethod = traceMessage.getClass().getMethod("setEncoding", String.class);
            setEncodingMethod.invoke(traceMessage, "UTF-8");

            Class<?> traceMessageInterface = Class.forName("com.sap.it.api.msglog.adapter.AdapterTraceMessage");
            Method writeTraceMethod = mplLog.getClass().getMethod("writeTrace", traceMessageInterface);
            writeTraceMethod.invoke(mplLog, traceMessage);

            LOG.debug("Trace written for {} ({} bytes)", logMessage, traceData.length);
        } catch (Exception e) {
            LOG.debug("Failed to write trace: {}", e.getMessage());
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
