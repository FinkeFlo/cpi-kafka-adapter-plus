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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A SASL credential with a line break (#245). Kafka's JAAS parser ends a quoted value at a line
 * break and quotes the rest of the password as a key in its error message, which reached the trace
 * ({@code producer.init.failed}) and the MPL. Every client that logs in with SASL, the receiver's,
 * the sender's and the DLQ producer's, now refuses such a credential before Kafka sees it, with a
 * message that names the alias; no part of the password may appear in a message or a log line.
 */
public class SaslCredentialRejectionTest {

    /** The two parts of the password, around its line break. */
    private static final String HEAD = "QuuxHeadPart";
    private static final String TAIL = "QuuxTailPart";

    private DefaultCamelContext ctx;

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
        CredentialHelper.setCredentialResolver(alias -> "kafka-user".equals(alias)
                ? new CredentialHelper.UserCredentials("alice", HEAD + "\n" + TAIL)
                : new CredentialHelper.UserCredentials("alice", "secret"));
    }

    @After
    public void tearDown() throws Exception {
        CredentialHelper.setCredentialResolver(null);
        ctx.close();
    }

    @Test
    public void theReceiverFailsTheMessageNamingTheAlias() throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint("credentialAlias=kafka-user");
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        // Normally assigned in doStart(), which would also start the background topic check.
        setField(CpiKafkaPlusProducer.class, producer, "tracingHelper", new AdapterTracingHelper(endpoint));
        Exchange exchange = new DefaultExchange(ctx);
        exchange.getIn().setBody("{\"id\": 1}");
        AtomicReference<Exception> failure = new AtomicReference<>();

        String logged = ownRecords(capture(() -> {
            try {
                producer.doProcess(exchange);
            } catch (Exception e) {
                failure.set(e);
            }
        }));

        // The message of the exchange, which is what the MPL shows.
        Assert.assertNotNull("expected the send to fail", failure.get());
        String message = chain(failure.get());
        Assert.assertTrue(message, message.contains("The password stored under 'Credential Alias' "
                + "(credentialAlias) 'kafka-user' contains a line break or another control character"));
        assertNoPartOfThePassword(message);
        Assert.assertTrue(logged, logged.contains("producer.init.failed"));
        Assert.assertTrue(logged, logged.contains("(credentialAlias) ''kafka-user''"));
        assertNoPartOfThePassword(logged);
    }

    @Test
    public void theSenderFailsItsConnectNamingTheAlias() throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint("groupId=g&credentialAlias=kafka-user");
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, exchange -> { });
        setField(CpiKafkaPlusConsumer.class, consumer, "tracingHelper", new AdapterTracingHelper(endpoint));

        String logged = ownRecords(capture(() -> Assert.assertEquals(0, consumer.poll())));

        Assert.assertTrue(logged, logged.contains("consumer.init.failed"));
        Assert.assertTrue(logged, logged.contains("The password stored under ''Credential Alias'' "
                + "(credentialAlias) ''kafka-user'' contains a line break or another control character"));
        assertNoPartOfThePassword(logged);
    }

    @Test
    public void theDlqProducerFailsNamingItsOwnAlias() throws Exception {
        // The channel's own credential is fine; the one of the DLQ has the line break.
        CpiKafkaPlusEndpoint endpoint = endpoint("groupId=g&credentialAlias=other-user"
                + "&dlqEnabled=true&dlqTopic=orders-dlq&dlqCredentialAlias=kafka-user");
        AtomicReference<RuntimeException> failure = new AtomicReference<>();

        String logged = ownRecords(capture(() -> {
            try {
                new DlqProducerHelper(endpoint).close();
            } catch (RuntimeException e) {
                failure.set(e);
            }
        }));

        Assert.assertNotNull("expected the DLQ producer to fail", failure.get());
        String message = chain(failure.get());
        Assert.assertTrue(message, message.contains("The password stored under 'DLQ SASL Credential Alias' "
                + "(dlqCredentialAlias) 'kafka-user' contains a line break or another control character"));
        assertNoPartOfThePassword(message);
        Assert.assertTrue(logged, logged.contains("dlq.producer.init.failed"));
        assertNoPartOfThePassword(logged);
    }

    /** A SASL_SSL endpoint: the TLS listener probe only runs for plaintext protocols. */
    private CpiKafkaPlusEndpoint endpoint(String parameters) throws Exception {
        return (CpiKafkaPlusEndpoint) ctx.getEndpoint("cpi-kafka-plus:orders?bootstrapServers=localhost:9999"
                + "&securityProtocol=SASL_SSL&saslMechanism=PLAIN&" + parameters);
    }

    private static void assertNoPartOfThePassword(String text) {
        for (String part : new String[] {"Quux", "HeadPart", "TailPart"}) {
            Assert.assertFalse("'" + part + "' found in:\n" + text, text.contains(part));
        }
    }

    /** The messages of the whole cause chain. */
    private static String chain(Throwable t) {
        StringBuilder out = new StringBuilder();
        for (Throwable cause = t; cause != null; cause = cause.getCause() == cause ? null : cause.getCause()) {
            out.append(cause).append('\n');
        }
        return out.toString();
    }

    private static void setField(Class<?> type, Object target, String name, Object value) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Runs {@code action} and returns what was logged meanwhile; slf4j-simple writes to System.err. */
    private static String capture(ThrowingRunnable action) throws Exception {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, "UTF-8"));
        try {
            action.run();
        } finally {
            System.setErr(original);
        }
        return new String(captured.toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * The records of this thread, with the stack trace lines that follow them: clients left behind
     * by other tests keep logging on their own threads.
     */
    private static String ownRecords(String logged) {
        String own = "[" + Thread.currentThread().getName() + "] ";
        StringBuilder out = new StringBuilder();
        boolean inOwnRecord = false;
        for (String line : logged.split("\\R")) {
            if (line.startsWith("[")) {
                inOwnRecord = line.startsWith(own);
            }
            if (inOwnRecord) {
                out.append(line).append('\n');
            }
        }
        return out.toString();
    }
}
