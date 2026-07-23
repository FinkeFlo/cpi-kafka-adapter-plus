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
import org.junit.Assert;
import org.junit.Test;

public class CpiKafkaPlusProducerTopicResolutionTest {

    @Test
    public void testLiteralTopicStaysUnchanged() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            Exchange exchange = new DefaultExchange(ctx);

            String resolved = CpiKafkaPlusProducer.resolveTopic(exchange, "my-literal-topic");
            Assert.assertEquals("my-literal-topic", resolved);
        }
    }

    @Test
    public void testPropertyAliasExpressionResolves() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.setProperty("myConfiguredTopicProperty", "topic-from-exchange-property");

            String resolved = CpiKafkaPlusProducer.resolveTopic(
                    exchange, "${property.myConfiguredTopicProperty}");
            Assert.assertEquals("topic-from-exchange-property", resolved);
        }
    }

    @Test
    public void testExchangePropertyExpressionResolves() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.setProperty("myConfiguredTopicProperty", "topic-from-exchange-property");

            String resolved = CpiKafkaPlusProducer.resolveTopic(
                    exchange, "${exchangeProperty.myConfiguredTopicProperty}");
            Assert.assertEquals("topic-from-exchange-property", resolved);
        }
    }

    @Test
    public void testHeaderExpressionResolves() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setHeader("myConfiguredTopicHeader", "topic-from-header");

            String resolved = CpiKafkaPlusProducer.resolveTopic(
                    exchange, "${header.myConfiguredTopicHeader}");
            Assert.assertEquals("topic-from-header", resolved);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnresolvedExpressionFailsFast() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            Exchange exchange = new DefaultExchange(ctx);
            CpiKafkaPlusProducer.resolveTopic(exchange, "${property.topic}");
        }
    }
}
