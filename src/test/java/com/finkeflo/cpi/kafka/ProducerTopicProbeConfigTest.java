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

import java.security.SecureRandom;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLContext;

import org.apache.kafka.clients.admin.Admin;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * The topic probe builds its own AdminClient from the producer config. On a channel with a CPI
 * keystore alias that client must be constructible, otherwise every probe is INCONCLUSIVE and the
 * missing-topic / auth / TLS fail-fast never fires (#180).
 */
public class ProducerTopicProbeConfigTest {

    @After
    public void resetResolver() {
        CredentialHelper.setSslContextResolver(null);
    }

    @Test
    public void probeAdminClientCanBeCreatedOnAnMtlsChannel() throws Exception {
        final AtomicReference<String> resolvedAlias = new AtomicReference<String>();
        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, null, new SecureRandom());
        CredentialHelper.setSslContextResolver(new CredentialHelper.SslContextResolver() {
            public SSLContext resolveSslContext(String alias) {
                resolvedAlias.set(alias);
                return sslContext;
            }
        });

        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setBootstrapServers("localhost:9093");
        endpoint.setSecurityProtocol("SSL");
        endpoint.setSslKeystoreAlias("tenant-kafka");
        endpoint.setTopic("orders");

        Properties probeProps = new CpiKafkaPlusProducer(endpoint).buildTopicCheckProperties();

        // Construction configures the SSL engine factory but opens no connection.
        try (Admin admin = Admin.create(probeProps)) {
            Assert.assertNotNull(admin);
        }
        Assert.assertEquals("the probe must use the channel's keystore alias",
                "tenant-kafka", resolvedAlias.get());
    }
}
