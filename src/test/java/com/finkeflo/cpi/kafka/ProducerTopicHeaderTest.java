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
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A batch receiver answers with a {@code CamelKafkaTopic} header. A second Kafka receiver in the same
 * exchange read it as a topic override and wrote to the first receiver's topic (#185).
 */
public class ProducerTopicHeaderTest {

    private DefaultCamelContext ctx;

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        ctx.close();
    }

    @Test
    public void aBatchResponseTopicDoesNotRedirectTheNextReceiver() {
        Exchange exchange = afterBatchSendTo("orders");

        Assert.assertEquals("invoices", producerFor("invoices").targetTopic(exchange));
    }

    @Test
    public void aTopicTheFlowSetsExplicitlyStillWins() {
        Exchange exchange = afterBatchSendTo("orders");
        exchange.getIn().setHeader("CamelKafkaTopic", "audit");

        Assert.assertEquals("audit", producerFor("invoices").targetTopic(exchange));
    }

    @Test
    public void aReceiverWithoutItsOwnTopicStillFollowsTheHeader() {
        // Flows that rely on the header to route the next receiver keep working.
        Exchange exchange = afterBatchSendTo("orders");

        Assert.assertEquals("orders", producerFor(null).targetTopic(exchange));
    }

    @Test
    public void aHeaderNotSetByTheAdapterIsAnOverride() {
        Exchange exchange = new DefaultExchange(ctx);
        exchange.getIn().setHeader("CamelKafkaTopic", "orders");

        Assert.assertEquals("orders", producerFor("invoices").targetTopic(exchange));
    }

    private Exchange afterBatchSendTo(String topic) {
        Exchange exchange = new DefaultExchange(ctx);
        ProducerBatchHelper.setResponseHeadersAndBody(exchange.getIn(), topic, "JSON_ARRAY",
                new ProducerBatchHelper.BatchSendResult(1, 0L, 0L, "0", 5L));
        Assert.assertEquals("precondition: the response header is still set for flows that read it",
                topic, exchange.getIn().getHeader("CamelKafkaTopic"));
        return exchange;
    }

    private CpiKafkaPlusProducer producerFor(String topic) {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(ctx);
        endpoint.setBootstrapServers("localhost:9092");
        endpoint.setTopic(topic);
        return new CpiKafkaPlusProducer(endpoint);
    }
}
