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

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * Fail-fast configuration validation tests for transactional batching (ADR 0001).
 * These are pure unit tests (no Kafka broker required) — {@code doStart()} must reject
 * invalid configuration before any Kafka resources are created.
 */
public class ProducerTransactionalConfigTest {

    private CpiKafkaPlusProducer createProducer(Map<String, String> extraParams) throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
            ctx.start();

            Map<String, String> params = new HashMap<>();
            params.put("bootstrapServers", "localhost:9999");
            params.put("securityProtocol", "PLAINTEXT");
            params.put("producerBatchMode", "JSON_ARRAY");
            params.put("enableTransactions", "true");
            params.put("transactionalIdPrefix", "test-txn");
            // overrides (e.g. blank prefix, bad slot count, disabled idempotence) win over the defaults
            params.putAll(extraParams);

            StringBuilder uri = new StringBuilder("cpi-kafka-plus:test-topic?");
            boolean first = true;
            for (Map.Entry<String, String> entry : params.entrySet()) {
                if (!first) {
                    uri.append("&");
                }
                uri.append(entry.getKey()).append("=").append(entry.getValue());
                first = false;
            }

            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri.toString());
            return new CpiKafkaPlusProducer(endpoint);
        }
    }

    @Test
    public void testMissingTransactionalIdPrefixIsRejected() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("transactionalIdPrefix", "");
        CpiKafkaPlusProducer producer = createProducer(params);
        try {
            producer.doStart();
            Assert.fail("Expected IllegalArgumentException for missing transactionalIdPrefix");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("transactionalIdPrefix"));
        }
    }

    @Test
    public void testZeroMaxConcurrentTransactionsIsRejected() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("maxConcurrentTransactions", "0");
        CpiKafkaPlusProducer producer = createProducer(params);
        try {
            producer.doStart();
            Assert.fail("Expected IllegalArgumentException for maxConcurrentTransactions=0");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Message should mention maxConcurrentTransactions: " + e.getMessage(),
                    e.getMessage().contains("maxConcurrentTransactions"));
        }
    }

    @Test
    public void testNegativeMaxConcurrentTransactionsIsRejected() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("maxConcurrentTransactions", "-1");
        CpiKafkaPlusProducer producer = createProducer(params);
        try {
            producer.doStart();
            Assert.fail("Expected IllegalArgumentException for negative maxConcurrentTransactions");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("maxConcurrentTransactions"));
        }
    }

    @Test
    public void testDisabledIdempotenceIsRejected() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("enableIdempotence", "false");
        CpiKafkaPlusProducer producer = createProducer(params);
        try {
            producer.doStart();
            Assert.fail("Expected IllegalArgumentException for enableIdempotence=false with enableTransactions=true");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Message should mention idempotence: " + e.getMessage(),
                    e.getMessage().toLowerCase().contains("idempotence"));
        }
    }

    @Test
    public void testValidConfigurationStartsSuccessfully() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("maxConcurrentTransactions", "3");
        CpiKafkaPlusProducer producer = createProducer(params);
        try {
            producer.doStart();
        } finally {
            producer.doStop();
        }
    }

    @Test
    public void testComputeTopicHashIsDeterministic() {
        String hash1 = CpiKafkaPlusProducer.computeTopicHash("REGION_sample_app_STOCK_AMOUNT");
        String hash2 = CpiKafkaPlusProducer.computeTopicHash("REGION_sample_app_STOCK_AMOUNT");
        Assert.assertEquals("Hash must be deterministic across calls", hash1, hash2);
        Assert.assertEquals("Hash must be exactly 8 hex characters", 8, hash1.length());
        Assert.assertTrue("Hash must contain only hex characters", hash1.matches("[0-9a-f]+"));
    }

    @Test
    public void testComputeTopicHashDiffersForDifferentTopics() {
        String hashStock   = CpiKafkaPlusProducer.computeTopicHash("REGION_sample_app_STOCK_AMOUNT");
        String hashDemand  = CpiKafkaPlusProducer.computeTopicHash("REGION_sample_app_DEMAND_DETAIL_ERP_PRD");
        String hashSales   = CpiKafkaPlusProducer.computeTopicHash("REGION_sample_app_SALES_ORDER");
        String hashSku     = CpiKafkaPlusProducer.computeTopicHash("REGION_sample_app_SKU");
        String hashOrder   = CpiKafkaPlusProducer.computeTopicHash("REGION_sample_app_ORDER_PLAN_OUTB");

        java.util.Set<String> hashes = new java.util.HashSet<>(
                java.util.Arrays.asList(hashStock, hashDemand, hashSales, hashSku, hashOrder));
        Assert.assertEquals(
                "All 5 follow-the-sun topics must produce distinct hashes to prevent producer fencing",
                5, hashes.size());
    }

    @Test
    public void testTooLongTransactionalIdPrefixIsRejected() throws Exception {
        // A prefix long enough that prefix + hash + memberSuffix + slotId > 249 chars
        String longPrefix = new String(new char[240]).replace('\0', 'x');
        Map<String, String> params = new HashMap<>();
        params.put("transactionalIdPrefix", longPrefix);
        CpiKafkaPlusProducer producer = createProducer(params);
        try {
            producer.doStart();
            Assert.fail("Expected IllegalArgumentException for transactionalIdPrefix that produces an id > 249 chars");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Message should mention length: " + e.getMessage(),
                    e.getMessage().contains("249"));
        }
    }

    @Test
    public void testTransactionV2DisabledAppliedToRegularProducer() throws Exception {
        // transactionV2Enabled=false pins TRANSACTION_TWO_PHASE_COMMIT_ENABLE to false for ALL
        // producers (not only transactional ones), so two-phase commit (KIP-939) can never be
        // negotiated. Note: this does NOT disable Transaction Protocol V2 (KIP-890), which the
        // client derives from the broker's finalized "transaction.version" feature.
        try (org.apache.camel.impl.DefaultCamelContext ctx = new org.apache.camel.impl.DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
            ctx.start();

            String uri = "cpi-kafka-plus:some-topic?bootstrapServers=localhost%3A9999&securityProtocol=PLAINTEXT&transactionV2Enabled=false";
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
            Assert.assertFalse("transactionV2Enabled should be false", endpoint.isTransactionV2Enabled());

            java.util.Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);
            Assert.assertEquals(
                    "transaction.two.phase.commit.enable must be false for regular producer when transactionV2Enabled=false",
                    false,
                    props.get(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG));
        }
    }

    @Test
    public void testTransactionV2EnabledByDefaultForRegularProducer() throws Exception {
        // Default (transactionV2Enabled=true): the adapter must not touch the config at all, so the
        // Kafka client default (false) applies. It must never be set to true — that would request
        // two-phase commit, which needs broker support and a TWO_PHASE_COMMIT ACL.
        try (org.apache.camel.impl.DefaultCamelContext ctx = new org.apache.camel.impl.DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
            ctx.start();

            String uri = "cpi-kafka-plus:some-topic?bootstrapServers=localhost%3A9999&securityProtocol=PLAINTEXT";
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
            Assert.assertTrue("transactionV2Enabled should default to true", endpoint.isTransactionV2Enabled());

            java.util.Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);
            Assert.assertNull(
                    "transaction.two.phase.commit.enable must not be set when transactionV2Enabled=true",
                    props.get(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG));
        }
    }

    @Test
    public void testTwoPhaseCommitNeverEnabledForTransactionalProducer() throws Exception {
        // Regression guard for 1.2.4: the transactional path must never write
        // transaction.two.phase.commit.enable=true, regardless of transactionV2Enabled.
        try (org.apache.camel.impl.DefaultCamelContext ctx = new org.apache.camel.impl.DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
            ctx.start();

            String uri = "cpi-kafka-plus:some-topic?bootstrapServers=localhost%3A9999&securityProtocol=PLAINTEXT"
                    + "&enableTransactions=true&transactionalIdPrefix=cpi-test";
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);

            java.util.Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);
            Assert.assertNotEquals(
                    "two-phase commit must never be requested by the adapter",
                    true,
                    props.get(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG));
        }
    }
}
