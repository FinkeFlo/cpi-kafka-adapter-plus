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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for SASL security with a real Kafka broker.
 */
public class SecurityIT {

    private static DefaultCamelContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        KafkaTestInfrastructure.requireDockerAvailable();
        KafkaTestInfrastructure.startKafkaWithSasl();
        installValidCredentialResolver();

        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
    }

    @After
    public void restoreValidCredentials() {
        installValidCredentialResolver();
    }

    private static void installValidCredentialResolver() {
        final CredentialHelper.UserCredentials testCred =
                new CredentialHelper.UserCredentials(KafkaTestInfrastructure.SASL_USERNAME, KafkaTestInfrastructure.SASL_PASSWORD);
        CredentialHelper.setCredentialResolver(new CredentialHelper.CredentialResolver() {
            public CredentialHelper.UserCredentials resolveUserCredential(String alias) {
                return KafkaTestInfrastructure.SASL_CREDENTIAL_ALIAS.equals(alias) ? testCred : null;
            }
        });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (ctx != null) {
            ctx.stop();
        }
        CredentialHelper.setCredentialResolver(null);
    }

    @Test
    public void testProducerAuthenticatesWithSaslPlainCredentials() throws Exception {
        String topic = "it-sasl-positive-" + System.nanoTime();
        KafkaTestInfrastructure.createSaslTopic(topic, 1);

        CpiKafkaPlusProducer producer = createSaslProducer(topic);
        try {
            producer.doStart();

            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody("sasl-authenticated-message");
            producer.process(exchange);
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllSaslMessages(topic, 1, 10000);
        Assert.assertEquals("Should receive the SASL-produced record", 1, records.size());
        Assert.assertEquals("sasl-authenticated-message", records.get(0).value());
    }

    @Test
    public void testWrongPasswordFailsAuthentication() throws Exception {
        String topic = "it-sasl-wrong-password-" + System.nanoTime();
        KafkaTestInfrastructure.createSaslTopic(topic, 1);

        final CredentialHelper.UserCredentials wrongCred =
                new CredentialHelper.UserCredentials(KafkaTestInfrastructure.SASL_USERNAME, "wrong-password");
        CredentialHelper.setCredentialResolver(new CredentialHelper.CredentialResolver() {
            public CredentialHelper.UserCredentials resolveUserCredential(String alias) {
                return KafkaTestInfrastructure.SASL_CREDENTIAL_ALIAS.equals(alias) ? wrongCred : null;
            }
        });

        CpiKafkaPlusProducer producer = createBoundedSaslProducer(topic);
        try {
            Throwable failure = sendExpectingFailure(producer, "wrong-password-message");
            Assert.assertTrue("Expected SASL authentication failure but got: " + describeCauseChain(failure),
                    hasCauseNamed(failure, "SaslAuthenticationException"));
        } finally {
            producer.doStop();
            installValidCredentialResolver();
        }
    }

    @Test
    public void testMissingCredentialsFailFast() throws Exception {
        String topic = "it-sasl-missing-credentials-" + System.nanoTime();
        KafkaTestInfrastructure.createSaslTopic(topic, 1);

        CredentialHelper.setCredentialResolver(new CredentialHelper.CredentialResolver() {
            public CredentialHelper.UserCredentials resolveUserCredential(String alias) {
                return null;
            }
        });

        CpiKafkaPlusProducer producer = createBoundedSaslProducer(topic);
        try {
            Throwable failure = sendExpectingFailure(producer, "missing-credentials-message");
            Assert.assertTrue("Expected clear SASL/credential failure but got: " + describeCauseChain(failure),
                    hasAuthOrCredentialFailure(failure));
        } finally {
            producer.doStop();
            installValidCredentialResolver();
        }
    }

    private CpiKafkaPlusProducer createSaslProducer(String topic) throws Exception {
        String uri = KafkaTestInfrastructure.buildSaslEndpointUri(
                topic, "unused-sasl-group", new HashMap<String, String>());
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
        return new CpiKafkaPlusProducer(endpoint);
    }

    private CpiKafkaPlusProducer createBoundedSaslProducer(String topic) throws Exception {
        HashMap<String, String> params = new HashMap<String, String>();
        params.put("deliveryTimeoutSeconds", "5");
        String uri = KafkaTestInfrastructure.buildSaslEndpointUri(
                topic, "unused-sasl-group", params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
        return new CpiKafkaPlusProducer(endpoint);
    }

    private Throwable sendExpectingFailure(final CpiKafkaPlusProducer producer, final String body)
            throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Void> future = executor.submit(new Callable<Void>() {
            public Void call() throws Exception {
                producer.doStart();
                Exchange exchange = new DefaultExchange(ctx);
                exchange.getIn().setBody(body);
                producer.process(exchange);
                return null;
            }
        });
        try {
            future.get(20000, TimeUnit.MILLISECONDS);
            Assert.fail("Expected SASL send to fail, but it succeeded");
            return null;
        } catch (TimeoutException e) {
            producer.doStop();
            future.cancel(true);
            Assert.fail("SASL failure did not complete within the bounded wait");
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        } finally {
            executor.shutdownNow();
        }
    }

    private boolean hasCauseNamed(Throwable failure, String simpleClassName) {
        Throwable current = failure;
        while (current != null) {
            if (simpleClassName.equals(current.getClass().getSimpleName())) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean hasAuthOrCredentialFailure(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            String className = current.getClass().getSimpleName().toLowerCase();
            String message = current.getMessage() == null ? "" : current.getMessage().toLowerCase();
            if (className.contains("sasl")
                    || className.contains("authentication")
                    || message.contains("sasl")
                    || message.contains("authentication")
                    || message.contains("credential")
                    || message.contains("jaas")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private String describeCauseChain(Throwable failure) {
        StringBuilder sb = new StringBuilder();
        Throwable current = failure;
        while (current != null) {
            if (sb.length() > 0) {
                sb.append(" -> ");
            }
            sb.append(current.getClass().getName()).append(": ").append(current.getMessage());
            current = current.getCause();
        }
        return sb.toString();
    }
}
