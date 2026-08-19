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

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the opt-in behaviour of the Message Processing Log error attachment.
 *
 * <p>The default matters more than it looks. An attachment is the only part of the failure report
 * that consumes tenant storage, and it is written per failure — a record the receiver cannot deliver
 * is retried on every attempt, so a default of {@code true} would turn one stuck message into a
 * steady write against a tenant-wide storage budget shared with every other integration flow. The
 * information itself is not behind the flag: the error code, topic, producer path and retryable flag
 * reach the monitor either way, and the full cause chain always reaches the tenant trace file.
 */
public class MplErrorAttachmentOptionTest {

    private CpiKafkaPlusEndpoint endpoint(String uriSuffix) throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
            ctx.start();
            return (CpiKafkaPlusEndpoint) ctx.getEndpoint("cpi-kafka-plus:some-topic"
                    + "?bootstrapServers=localhost%3A9999&securityProtocol=PLAINTEXT" + uriSuffix);
        }
    }

    @Test
    public void attachmentIsOffUnlessTheChannelAsksForIt() throws Exception {
        Assert.assertFalse("The MPL error attachment must be opt-in: it costs tenant storage on "
                        + "every failure, including every redelivery of one stuck message.",
                endpoint("").isWriteMplErrorAttachment());
    }

    @Test
    public void attachmentIsEnabledFromTheChannelConfiguration() throws Exception {
        Assert.assertTrue("writeMplErrorAttachment=true must reach the endpoint, otherwise the "
                        + "channel offers a switch that does nothing.",
                endpoint("&writeMplErrorAttachment=true").isWriteMplErrorAttachment());
    }

    @Test
    public void attachmentCanBeSwitchedOffExplicitly() throws Exception {
        Assert.assertFalse(endpoint("&writeMplErrorAttachment=false").isWriteMplErrorAttachment());
    }
}
