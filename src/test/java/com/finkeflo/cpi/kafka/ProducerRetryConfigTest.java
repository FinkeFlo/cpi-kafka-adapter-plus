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
        params.put("producerRetryTotalBudgetSeconds", "901");
        Assert.assertTrue(startAndExpectRejection(params).contains("producerRetryTotalBudgetSeconds"));
    }

    @Test
    public void defaultDeliveryTimeoutIsRejectedOnlyForExceedingTheBudget() throws Exception {
        // Until 1.3.2 the shipped delivery timeout of 120 s was rejected outright on a
        // transactional channel with the retry on, because transaction.timeout.ms was left at its
        // 60 s client default. The adapter now derives that option from the delivery timeout, so
        // the only remaining question is whether the worst case fits the configured budget — and
        // the message must say so, rather than pointing at a broker limit that no longer applies.
        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        params.put("enableTransactions", "true");
        params.put("transactionalIdPrefix", "test-txn");
        params.put("producerRetryMaxAttempts", "2");
        String message = startAndExpectRejection(params);
        Assert.assertTrue("the budget must be flagged: " + message,
                message.contains("producerRetryTotalBudgetSeconds"));
        Assert.assertFalse("the 60 s transaction timeout rule is gone: " + message,
                message.contains("transaction.timeout.ms"));
    }

    @Test
    public void theShippedDeliveryTimeoutStartsWithABudgetThatFitsIt() throws Exception {
        // The configuration a customer hits first: everything at its default except the retry.
        // Two attempts at 120 s cost 432 s in the worst case, which was not even expressible
        // before 1.3.2 because the budget was capped at 300 s.
        Map<String, String> params = new HashMap<>();
        params.put("producerBatchMode", "JSON_ARRAY");
        params.put("enableTransactions", "true");
        params.put("transactionalIdPrefix", "test-txn");
        params.put("producerRetryMaxAttempts", "2");
        params.put("producerRetryTotalBudgetSeconds", "450");

        CpiKafkaPlusProducer producer = createProducer(params);
        producer.doStart();
        producer.doStop();
    }

    @Test
    public void aDeliveryTimeoutBeyondTheBrokerTransactionCapIsRejected() throws Exception {
        // A broker rejects transaction.timeout.ms above transaction.max.timeout.ms (15 minutes by
        // default) at initTransactions(), which would surface as a failed send rather than a named
        // parameter. The retry is deliberately off here: the bound belongs to transactions.
        Map<String, String> params = new HashMap<>();
        params.put("enableTransactions", "true");
        params.put("transactionalIdPrefix", "test-txn");
        params.put("deliveryTimeoutSeconds", "871");
        String message = startAndExpectRejection(params);
        Assert.assertTrue(message, message.contains("deliveryTimeoutSeconds"));
        Assert.assertTrue("the broker limit must be named: " + message,
                message.contains("transaction.max.timeout.ms"));
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
