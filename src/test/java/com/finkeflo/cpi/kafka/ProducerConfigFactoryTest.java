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
 * Tests for {@link ProducerConfigFactory} — verifies client.id stability (c3)
 * and transaction.two.phase.commit.enable semantics (e6).
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
    // e6: transaction.two.phase.commit.enable semantics
    // ─────────────────────────────────────────────────────────────────────────────

    /**
     * Verifies that when transactionV2Enabled=true (the default), the config is NOT set,
     * allowing Kafka's default (false) to apply. This preserves backward-compatible
     * single-phase commit semantics.
     */
    @Test
    public void transactionV2EnabledTrueDoesNotSetConfig() throws Exception {
        CpiKafkaPlusEndpoint endpoint = createEndpoint();
        endpoint.setTransactionV2Enabled(true);

        Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);

        Assert.assertNull("When transactionV2Enabled=true, transaction.two.phase.commit.enable "
                + "should NOT be set (let Kafka default apply)",
                props.get("transaction.two.phase.commit.enable"));
    }

    /**
     * Verifies that when transactionV2Enabled=false, the config is explicitly set to false,
     * making the opt-out visible in broker logs and surviving any future default change.
     */
    @Test
    public void transactionV2EnabledFalseSetsConfigExplicitly() throws Exception {
        CpiKafkaPlusEndpoint endpoint = createEndpoint();
        endpoint.setTransactionV2Enabled(false);

        Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);

        Assert.assertEquals("When transactionV2Enabled=false, config should be explicitly set",
                "false", props.get("transaction.two.phase.commit.enable"));
    }

    /**
     * Documents that the OLD buggy behavior would have set the config to true when
     * transactionV2Enabled=true, potentially requesting 2PC from the broker.
     * This test exists to document the bug that was fixed.
     */
    @Test
    public void documentsPreviousBuggyBehavior() throws Exception {
        // The old code was:
        //   props.put("transaction.two.phase.commit.enable", endpoint.isTransactionV2Enabled())
        // Which would translate true -> "true" and request 2PC.
        //
        // The new code is:
        //   if (!endpoint.isTransactionV2Enabled()) { props.put(..., "false"); }
        // Which leaves the config unset when true, allowing Kafka's default (false) to apply.
        //
        // This test verifies the fix by checking that:
        // 1. transactionV2Enabled=true does NOT result in the config being "true"
        // 2. transactionV2Enabled=false results in the config being "false"

        CpiKafkaPlusEndpoint endpointTrue = createEndpoint();
        endpointTrue.setTransactionV2Enabled(true);
        Properties propsTrue = ProducerConfigFactory.buildProducerProperties(endpointTrue);

        CpiKafkaPlusEndpoint endpointFalse = createEndpoint();
        endpointFalse.setTransactionV2Enabled(false);
        Properties propsFalse = ProducerConfigFactory.buildProducerProperties(endpointFalse);

        // OLD BUGGY: propsTrue would have "true" -> would request 2PC
        // NEW FIXED: propsTrue has null -> uses Kafka default (false) -> no 2PC
        Assert.assertNotEquals("Must NOT set config to 'true' (would request 2PC)",
                "true", propsTrue.get("transaction.two.phase.commit.enable"));

        // Both old and new: propsFalse should have "false"
        Assert.assertEquals("false", propsFalse.get("transaction.two.phase.commit.enable"));
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
