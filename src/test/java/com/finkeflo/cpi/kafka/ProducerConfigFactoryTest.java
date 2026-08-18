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

import java.util.Properties;

import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link ProducerConfigFactory} — verifies client.id stability (c3).
 * 
 * Note: e6 transaction.two.phase.commit.enable is set in CpiKafkaPlusProducer.sendTransactionalBatch,
 * not here. See ProducerBatchHelperTest for transactional tests.
 */
public class ProducerConfigFactoryTest {

    private CamelContext camelContext;

    @Before
    public void setUp() throws Exception {
        camelContext = new DefaultCamelContext();
        camelContext.start();
    }

    @After
    public void tearDown() throws Exception {
        if (camelContext != null) {
            camelContext.stop();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // c3: Stable client.id
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    public void clientIdIsStableAcrossRebuilds() throws Exception {
        // Set a stable adapter instance ID (as CPI does at deployment time)
        camelContext.getGlobalOptions().put("adapterInstanceID", "unit-test-adapter-123");

        CpiKafkaPlusEndpoint endpoint = createEndpoint();
        Properties props1 = ProducerConfigFactory.buildProducerProperties(endpoint);
        Properties props2 = ProducerConfigFactory.buildProducerProperties(endpoint);

        String clientId1 = (String) props1.get(ProducerConfig.CLIENT_ID_CONFIG);
        String clientId2 = (String) props2.get(ProducerConfig.CLIENT_ID_CONFIG);

        Assert.assertNotNull("client.id should be set when adapterInstanceID is present", clientId1);
        Assert.assertEquals("client.id must be stable across rebuilds", clientId1, clientId2);
        Assert.assertTrue("client.id should contain the adapter instance ID",
                clientId1.contains("unit-test-adapter-123"));
    }

    @Test
    public void clientIdOmittedWhenAdapterInstanceIdMissing() throws Exception {
        // No adapterInstanceID set
        CpiKafkaPlusEndpoint endpoint = createEndpoint();
        Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);

        String clientId = (String) props.get(ProducerConfig.CLIENT_ID_CONFIG);
        Assert.assertNull("client.id should not be set when adapterInstanceID is missing", clientId);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // e6: transaction.two.phase.commit.enable
    // ─────────────────────────────────────────────────────────────────────────────

    /**
     * Pins transaction.two.phase.commit.enable to false for every producer, independently of the
     * retired transactionV2Enabled option.
     *
     * <p>transaction.two.phase.commit.enable controls KIP-939 (client-side Two-Phase Commit), not
     * KIP-890 (Transaction Protocol V2, which the client derives from the broker's finalized
     * transaction.version feature). 2PC requires broker support plus a TWO_PHASE_COMMIT ACL and
     * makes transactions non-expiring, so the adapter must never request it.
     */
    @Test
    public void transactionTwoPhasePinnedFalseInFactory() throws Exception {
        for (boolean legacyValue : new boolean[]{true, false}) {
            CpiKafkaPlusEndpoint endpoint = createEndpoint();
            endpoint.setTransactionV2Enabled(legacyValue);
            Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);

            Assert.assertEquals(
                    "two-phase commit must be pinned to false regardless of the retired option ("
                            + legacyValue + ")",
                    false,
                    props.get("transaction.two.phase.commit.enable"));
        }
    }

    private CpiKafkaPlusEndpoint createEndpoint() throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(camelContext);
        endpoint.setBootstrapServers("broker-a:9092");
        endpoint.setTopic("test-topic");
        endpoint.setSecurityProtocol("PLAINTEXT");
        return endpoint;
    }
}
