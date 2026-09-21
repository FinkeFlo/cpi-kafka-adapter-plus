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
import org.apache.kafka.common.KafkaException;
import org.junit.Assert;
import org.junit.Test;

public class ClassSpaceFaultsTest {

    private static final String WIRING_MSG =
            "org.apache.kafka.common.compress.Lz4Compression$Builder not found by com.finkeflo.cpi.kafka [1501] "
            + "— the bundle wiring for com.finkeflo.cpi.kafka is no longer valid";

    @Test
    public void wiringTextIsConclusiveOnItsOwn() {
        Throwable wrapped = new RuntimeException("send failed", new KafkaException("x",
                new NoClassDefFoundError(WIRING_MSG)));
        Assert.assertTrue(ClassSpaceFaults.isWiringInvalid(wrapped));
        Assert.assertTrue(ClassSpaceFaults.isOnStaleClassSpace(wrapped, ClassSpaceFaultsTest.class));
        Assert.assertFalse(ClassSpaceFaults.isWiringInvalid(new RuntimeException(WIRING_MSG)));
    }

    @Test
    public void faultSignatureAloneIsNotConclusiveOutsideOsgi() {
        Throwable snappyLike = new KafkaException("x",
                new NoClassDefFoundError("Could not initialize class org.xerial.snappy.Snappy"));
        Assert.assertTrue(ClassSpaceFaults.hasFaultSignature(snappyLike));
        // Outside OSGi the loader can never be stale, so the second signature must not fire.
        Assert.assertFalse(ClassSpaceFaults.isOnStaleClassSpace(snappyLike, ClassSpaceFaultsTest.class));
        Assert.assertFalse(ClassSpaceFaults.isOnStaleClassSpace(new KafkaException("timeout"), ClassSpaceFaultsTest.class));
    }

    /** Producer counterpart of the consumer's honest stop: the send side must name the remedy too. */
    @Test
    public void producerTranslatesClassSpaceFaultIntoActionableFailure() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
            endpoint.setTopic("orders");
            Throwable[] toThrow = new Throwable[1];
            CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint) {
                @Override
                void doProcess(Exchange exchange) throws Exception {
                    if (toThrow[0] instanceof Exception) {
                        throw (Exception) toThrow[0];
                    }
                    throw (Error) toThrow[0];
                }
            };
            Exchange exchange = new DefaultExchange(ctx);

            toThrow[0] = new RuntimeException("Failed to send", new KafkaException("x", new NoClassDefFoundError(WIRING_MSG)));
            try {
                producer.process(exchange);
                Assert.fail("expected failure");
            } catch (IllegalStateException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("Redeploy the integration flow"));
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("topic='orders'"));
                Assert.assertSame(toThrow[0], e.getCause());
            }

            // Raw Error from the send path (not caught by the catch (Exception) blocks) gets the same treatment.
            toThrow[0] = new NoClassDefFoundError(WIRING_MSG);
            try {
                producer.process(exchange);
                Assert.fail("expected failure");
            } catch (IllegalStateException e) {
                Assert.assertSame(toThrow[0], e.getCause());
            }

            // Anything else passes through untouched.
            toThrow[0] = new KafkaException("timeout");
            try {
                producer.process(exchange);
                Assert.fail("expected failure");
            } catch (KafkaException e) {
                Assert.assertSame(toThrow[0], e);
            }
        }
    }
}
