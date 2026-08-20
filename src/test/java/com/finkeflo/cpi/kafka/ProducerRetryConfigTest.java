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
 * Fail-fast validation of the producer outer-retry configuration.
 *
 * <p>A retry configuration whose very first attempt already consumes the whole budget is a promise
 * to operations that the adapter cannot keep, and it would only be discovered during the outage it
 * was configured for. Hence a start-up failure rather than a warning.
 */
public class ProducerRetryConfigTest {

    private CpiKafkaPlusProducer createProducer(Map<String, String> extraParams) throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
            ctx.start();

            Map<String, String> params = new HashMap<>();
            params.put("bootstrapServers", "localhost:9999");
            params.put("securityProtocol", "PLAINTEXT");
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

    private String startAndExpectRejection(Map<String, String> params) throws Exception {
        CpiKafkaPlusProducer producer = createProducer(params);
        try {
            producer.doStart();
            Assert.fail("Expected IllegalArgumentException for " + params);
            return null;
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        } finally {
            try {
                producer.doStop();
            } catch (Exception ignored) {
                // start failed, so stop may too — not what is under test
            }
        }
    }

    @Test
    public void maxAttemptsOutsideTheAllowedRangeIsRejected() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("producerRetryMaxAttempts", "6");
        String message = startAndExpectRejection(params);
        Assert.assertTrue(message, message.contains("producerRetryMaxAttempts"));
        Assert.assertTrue("the message must name the configured value: " + message,
                message.contains("6"));

        params.put("producerRetryMaxAttempts", "0");
        Assert.assertTrue(startAndExpectRejection(params).contains("producerRetryMaxAttempts"));
    }

    @Test
    public void delayOutsideTheAllowedRangeIsRejected() throws Exception {
        Map<String, String> params = new HashMap<>();
        // Zero was the tempting default: three immediate retries finish in under a second and hit
        // the same node that just went away, after the client already backed off exponentially.
        params.put("producerRetryDelaySeconds", "0");
        String message = startAndExpectRejection(params);
        Assert.assertTrue(message, message.contains("producerRetryDelaySeconds"));

        params.put("producerRetryDelaySeconds", "31");
        Assert.assertTrue(startAndExpectRejection(params).contains("producerRetryDelaySeconds"));
    }

    @Test
    public void budgetOutsideTheAllowedRangeIsRejected() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("producerRetryTotalBudgetSeconds", "301");
        Assert.assertTrue(startAndExpectRejection(params).contains("producerRetryTotalBudgetSeconds"));
    }

    @Test
    public void defaultDeliveryTimeoutIsRejectedOnceRetryIsSwitchedOn() throws Exception {
        // 120 s delivery timeout costs roughly 4 x 120 s per transactional attempt, so the retry
        // could never reach a second attempt inside a 30 s budget.
        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        params.put("enableTransactions", "true");
        params.put("transactionalIdPrefix", "test-txn");
        params.put("producerRetryMaxAttempts", "2");
        String message = startAndExpectRejection(params);
        Assert.assertTrue("the delivery timeout must be flagged: " + message,
                message.contains("deliveryTimeoutSeconds"));
    }

    @Test
    public void deliveryTimeoutAtOrAboveTheTransactionTimeoutIsRejected() throws Exception {
        // transaction.timeout.ms defaults to 60 s (kafka-clients 4.3.1, ProducerConfig.java:532-534)
        // and the adapter never overrides it, so the broker would discard the transaction while the
        // client is still waiting for acknowledgements.
        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        params.put("enableTransactions", "true");
        params.put("transactionalIdPrefix", "test-txn");
        params.put("producerRetryMaxAttempts", "2");
        params.put("deliveryTimeoutSeconds", "60");
        params.put("producerRetryTotalBudgetSeconds", "300");
        String message = startAndExpectRejection(params);
        Assert.assertTrue(message, message.contains("transaction.timeout.ms"));
    }

    @Test
    public void theRecommendedConfigurationStarts() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        params.put("enableTransactions", "true");
        params.put("transactionalIdPrefix", "test-txn");
        params.put("producerRetryMaxAttempts", "2");
        params.put("producerRetryDelaySeconds", "2");
        params.put("deliveryTimeoutSeconds", "2");

        CpiKafkaPlusProducer producer = createProducer(params);
        producer.doStart();   // worst case ~28 s, inside the 30 s default budget
        producer.doStop();
    }

    @Test
    public void theDefaultConfigurationStartsUnchanged() throws Exception {
        // The feature is off by default, so none of the checks above may affect an existing iFlow.
        CpiKafkaPlusProducer producer = createProducer(new HashMap<>());
        producer.doStart();
        producer.doStop();
    }
}
