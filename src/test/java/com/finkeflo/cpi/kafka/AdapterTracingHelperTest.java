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
}
