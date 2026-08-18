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

import com.sap.it.api.adapter.iflowmonitoring.EventDetails;
import com.sap.it.api.adapter.iflowmonitoring.EventStatus;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Guards {@link AdapterTracingHelper#buildEventDetails(boolean, Throwable)} — the reflective
 * construction of the ADK {@code EventDetails} that feeds the integration-flow Connection/
 * Consumption status. Runs against the real ADK API classes (provided scope, on the test
 * classpath); it does not need a live CPI {@code IFlowMonitorService}.
 */
public class AdapterTracingHelperTest {

    private DefaultCamelContext ctx;
    private AdapterTracingHelper helper;

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9092&groupId=test-group");
        helper = new AdapterTracingHelper(endpoint);
    }

    @After
    public void tearDown() throws Exception {
        if (ctx != null) {
            ctx.stop();
        }
    }

    /**
     * Regression for the silent-ERROR bug: the ADK declares {@code EventDetails.setException(Throwable)}.
     * Looking the setter up with {@code Exception.class} throws {@code NoSuchMethodException}, which was
     * swallowed — so a failing consumer never reported ERROR and the monitor stayed on "Successful".
     */
    @Test
    public void buildEventDetails_errorWithException_attachesExceptionAndErrorStatus() throws Exception {
        GroupAuthorizationException ex =
                new GroupAuthorizationException("Not authorized to access group: g");

        EventDetails details = (EventDetails) helper.buildEventDetails(false, ex);

        Assert.assertEquals(EventStatus.ERROR, details.getEventStatus());
        Assert.assertSame("the original exception must be attached", ex, details.getException());
    }

    @Test
    public void buildEventDetails_success_hasOkStatusAndNoException() throws Exception {
        EventDetails details = (EventDetails) helper.buildEventDetails(true, null);

        Assert.assertEquals(EventStatus.OK, details.getEventStatus());
        Assert.assertNull(details.getException());
    }

    /**
     * Every reflective lookup in {@link AdapterTracingHelper} resolved against the real ADK
     * interfaces.
     *
     * <p>This is the test the codebase was missing. Two defects of exactly this shape had already
     * shipped: {@code setException} looked up with {@code Exception.class} where the ADK declares
     * {@code Throwable}, and {@code getMessageLog} looked up with {@code Exchange.class} where the
     * ADK declares {@code Object}. {@code Class.getMethod} requires an exact parameter-type match,
     * so both threw {@code NoSuchMethodException} — and both were swallowed, so the adapter simply
     * produced no Message Processing Log output for its entire deployment lifetime.
     */
    @Test
    public void everyReflectiveAdkSignatureExists() throws Exception {
        Class<?> factory = Class.forName("com.sap.it.api.msglog.adapter.AdapterMessageLogFactory");
        Class<?> messageLog = Class.forName("com.sap.it.api.msglog.adapter.AdapterMessageLog");
        Class<?> traceMessage = Class.forName("com.sap.it.api.msglog.adapter.AdapterTraceMessage");
        Class<?> traceType = Class.forName("com.sap.it.api.msglog.adapter.AdapterTraceMessageType");

        // The exact lookups performed by writeTrace(...). getMethod throws if any is wrong.
        Assert.assertNotNull(factory.getMethod("getMessageLog",
                Object.class, String.class, String.class, String.class));
        Assert.assertNotNull(messageLog.getMethod("isTraceActive"));
        Assert.assertNotNull(messageLog.getMethod("createTraceMessage",
                traceType, byte[].class, boolean.class));
        Assert.assertNotNull(messageLog.getMethod("writeTrace", traceMessage));
        Assert.assertNotNull(traceMessage.getMethod("setEncoding", String.class));

        // ITApiFactory.getApi is the documented resolution path and replaced a Camel registry
        // lookup that could never have worked.
        Class<?> itApiFactory = Class.forName("com.sap.it.api.ITApiFactory");
        Assert.assertNotNull(itApiFactory.getMethod("getApi", Class.class, Object.class));
        Assert.assertTrue("the factory must be an ITApi for getApi to resolve it",
                Class.forName("com.sap.it.api.ITApi").isAssignableFrom(factory));

        // The historical setException defect.
        Class<?> eventDetails = Class.forName("com.sap.it.api.adapter.iflowmonitoring.EventDetails");
        Assert.assertNotNull(eventDetails.getMethod("setException", Throwable.class));
    }

    /**
     * f1/f4/f5: Verify the ADK signatures for the new MPL enrichment methods.
     *
     * <p>These methods are used by {@code reportFailure}:
     * <ul>
     *   <li>f1: {@code addCustomHeaderProperty(String, String)} on MessageLog interface</li>
     *   <li>f1: {@code putAdapterAttribute(String, String)} on AdapterMessageLog interface</li>
     *   <li>f4: {@code addAttachmentAsString(String, String, String)} on MessageLog interface</li>
     *   <li>f5: {@code getMessageLogWithStatus(Object, String, String, String)} on factory</li>
     *   <li>f5: {@code fireStatusEvent(AdapterStatusEvent, String)} on log-with-status interface</li>
     *   <li>f5: {@code close()} on log-with-status interface (AutoCloseable)</li>
     * </ul>
     */
    @Test
    public void newMplEnrichmentSignaturesExist() throws Exception {
        Class<?> factory = Class.forName("com.sap.it.api.msglog.adapter.AdapterMessageLogFactory");
        Class<?> messageLog = Class.forName("com.sap.it.api.msglog.adapter.AdapterMessageLog");
        Class<?> baseMessageLog = Class.forName("com.sap.it.api.msglog.MessageLog");
        Class<?> logWithStatus = Class.forName("com.sap.it.api.msglog.adapter.AdapterMessageLogWithStatus");
        Class<?> statusEvent = Class.forName("com.sap.it.api.msglog.adapter.AdapterStatusEvent");

        // f1: addCustomHeaderProperty on MessageLog (base interface)
        Assert.assertNotNull("f1: addCustomHeaderProperty must exist",
                baseMessageLog.getMethod("addCustomHeaderProperty", String.class, String.class));

        // f1: putAdapterAttribute on AdapterMessageLog
        Assert.assertNotNull("f1: putAdapterAttribute must exist",
                messageLog.getMethod("putAdapterAttribute", String.class, String.class));

        // f4: addAttachmentAsString on MessageLog (base interface)
        Assert.assertNotNull("f4: addAttachmentAsString must exist",
                baseMessageLog.getMethod("addAttachmentAsString", String.class, String.class, String.class));

        // f5: getMessageLogWithStatus on factory
        Assert.assertNotNull("f5: getMessageLogWithStatus must exist",
                factory.getMethod("getMessageLogWithStatus",
                        Object.class, String.class, String.class, String.class));

        // f5: fireStatusEvent(AdapterStatusEvent, String) on log-with-status interface
        Assert.assertNotNull("f5: fireStatusEvent must exist",
                logWithStatus.getMethod("fireStatusEvent", statusEvent, String.class));

        // f5: close() on log-with-status interface (AutoCloseable)
        Assert.assertNotNull("f5: close must exist",
                logWithStatus.getMethod("close"));

        // f5: AdapterStatusEvent.FAILED must be the only enum value
        @SuppressWarnings("unchecked")
        Object failedEvent = Enum.valueOf((Class<Enum>) statusEvent, "FAILED");
        Assert.assertNotNull("f5: AdapterStatusEvent.FAILED must exist", failedEvent);
    }

    /** Every trace-type name used as a string in the helper must exist in the ADK enum. */
    @Test
    public void everyTraceTypeNameResolves() throws Exception {
        @SuppressWarnings("unchecked")
        Class<? extends Enum> traceType = (Class<? extends Enum>)
                Class.forName("com.sap.it.api.msglog.adapter.AdapterTraceMessageType");

        // f3: Verify all trace types used in the helper including the fault types
        for (String name : new String[] {
                "SENDER_INBOUND",
                "SENDER_OUTBOUND",
                "SENDER_OUTBOUND_FAULT",  // f3: Used for consumer/sender direction errors
                "RECEIVER_OUTBOUND",
                "RECEIVER_INBOUND",
                "RECEIVER_INBOUND_FAULT"  // f3: Used for producer/receiver direction errors
        }) {
            Assert.assertNotNull(name, Enum.valueOf(traceType, name));
        }

        // Error traces use the fault variant. The receiver direction has no RECEIVER_OUTBOUND_FAULT,
        // so RECEIVER_INBOUND_FAULT is the correct value and this asserts the assumption holds.
        boolean receiverOutboundFaultExists = true;
        try {
            Enum.valueOf(traceType, "RECEIVER_OUTBOUND_FAULT");
        } catch (IllegalArgumentException absent) {
            receiverOutboundFaultExists = false;
        }
        Assert.assertFalse("if the ADK gains RECEIVER_OUTBOUND_FAULT, error traces should use it",
                receiverOutboundFaultExists);
    }

    /** Off-platform the helper must stay silent and must never fail an exchange. */
    @Test
    public void traceErrorIsHarmlessWhenTheMplBindingIsUnavailable() {
        org.apache.camel.Exchange exchange = new org.apache.camel.support.DefaultExchange(ctx);
        java.util.Map<String, String> context = new java.util.LinkedHashMap<>();
        context.put("topic", "test-topic");

        helper.traceError(exchange, new IllegalStateException("boom"), context);
    }

    /**
     * Off-platform (ADK absent) the traceError with direction parameter must stay silent.
     */
    @Test
    public void traceErrorWithDirectionIsHarmlessWhenTheMplBindingIsUnavailable() {
        org.apache.camel.Exchange exchange = new org.apache.camel.support.DefaultExchange(ctx);
        java.util.Map<String, String> context = new java.util.LinkedHashMap<>();
        context.put("topic", "test-topic");

        // Sender direction (consumer side)
        helper.traceError(exchange, new IllegalStateException("consumer fail"), context, true);

        // Receiver direction (producer side)
        helper.traceError(exchange, new IllegalStateException("producer fail"), context, false);
    }

    /**
     * Off-platform (ADK absent) the reportFailure method must stay silent and not throw.
     */
    @Test
    public void reportFailureIsHarmlessWhenTheMplBindingIsUnavailable() {
        org.apache.camel.Exchange exchange = new org.apache.camel.support.DefaultExchange(ctx);
        java.util.Map<String, String> context = new java.util.LinkedHashMap<>();
        context.put("topic", "test-topic");
        context.put("partition", "0");
        context.put("offset", "12345");

        // Without firing status event
        helper.reportFailure(exchange, new IllegalStateException("test error"),
                "TEST_ERROR_CODE", context, false);

        // With firing status event
        helper.reportFailure(exchange, new IllegalStateException("test error"),
                "TEST_ERROR_CODE", context, true);

        // With null error code (should not throw)
        helper.reportFailure(exchange, new IllegalStateException("test error"),
                null, context, false);
    }
}
