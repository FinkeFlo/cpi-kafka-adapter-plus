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
}
