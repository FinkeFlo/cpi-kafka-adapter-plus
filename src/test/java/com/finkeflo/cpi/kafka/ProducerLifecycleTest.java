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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

/**
 * Lifecycle of the shared producer and its serialization helpers across rebuilds (#169): an Avro
 * channel must keep producing Avro, and a reconnect racing a re-initialization must not leave the
 * producer marked ready without a client.
 */
public class ProducerLifecycleTest {

    private static final String SCHEMA_RESPONSE = "{\"subject\":\"orders-value\",\"version\":1,\"id\":7,"
            + "\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Order\\\","
            + "\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"string\\\"}]}\"}";

    private MockWebServer schemaRegistry;
    private DefaultCamelContext ctx;

    @Before
    public void setUp() throws Exception {
        TlsListenerProbe.setProbeRunnerForTests(address -> TlsListenerProbe.Verdict.INCONCLUSIVE);
        CredentialHelper.setCredentialResolver(null);
        schemaRegistry = new MockWebServer();
        schemaRegistry.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                return new MockResponse()
                        .setResponseCode(200)
                        .setHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                        .setBody(SCHEMA_RESPONSE);
            }
        });
        schemaRegistry.start();
        ctx = new DefaultCamelContext();
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        TlsListenerProbe.clearCacheForTests();
        CredentialHelper.setCredentialResolver(null);
        ctx.close();
        schemaRegistry.shutdown();
    }

    @Test
    public void avroStaysOnAfterProducerRebuild() throws Exception {
        CpiKafkaPlusProducer producer = newProducer(avroEndpoint("NONE", false));
        try {
            invoke(producer, "ensureInitialized");

            // Credential rotation is one of the FATAL_PRODUCER_UNUSABLE errors that rebuild the
            // shared producer.
            producer.handleSendFailure(new AuthenticationException("SASL secret rotated"),
                    "producer.single.send", new LinkedHashMap<String, String>());
            invoke(producer, "ensureInitialized");

            byte[] value = (byte[]) invoke(producer, "serializeValue",
                    new Class<?>[] { String.class, Message.class }, "orders", message("{\"id\":\"42\"}"));

            Assert.assertEquals("expected the Confluent magic byte, i.e. an Avro record, not raw JSON",
                    0x00, value[0]);
        } finally {
            invoke(producer, "doStop");
        }
    }

    @Test
    public void transactionalBatchFailsWhenItsHelpersCannotBeCreated() throws Exception {
        CpiKafkaPlusEndpoint endpoint = avroEndpoint("JSON_ARRAY", true);
        endpoint.setSchemaRegistryCredentialAlias("sr-alias");
        CredentialHelper.setCredentialResolver(alias -> {
            throw new IllegalStateException("Secure Store unavailable");
        });
        CpiKafkaPlusProducer producer = newProducer(endpoint);
        setField(producer, "txnSlotSemaphore", new Semaphore(1, true));
        setField(producer, "txnSlotInUse", new boolean[1]);
        setField(producer, "topicHash", "0000abcd");
        setField(producer, "resolvedMemberSuffix", "test");
        verifiedTopics(producer).add("orders");
        AtomicInteger transactionalProducers = new AtomicInteger();
        producer.txnProducerFactory = props -> {
            transactionalProducers.incrementAndGet();
            return new AcknowledgingProducer();
        };

        Exchange exchange = new DefaultExchange(ctx);
        exchange.getIn().setBody("[{\"key\": \"k1\", \"value\": {\"id\": \"1\"}}]");

        try {
            producer.doProcess(exchange);
            Assert.fail("expected the exchange to fail while the Avro serializer is missing");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Secure Store unavailable"));
        }
        Assert.assertEquals("no transaction may be opened, so nothing can be committed as raw JSON",
                0, transactionalProducers.get());
    }

    @Test
    public void singleSendRefusesRawBodyWhenAvroHelperIsMissing() throws Exception {
        CpiKafkaPlusProducer producer = newProducer(avroEndpoint("NONE", false));

        try {
            invoke(producer, "serializeValue",
                    new Class<?>[] { String.class, Message.class }, "orders", message("{\"id\":\"42\"}"));
            Assert.fail("expected a configured but missing Avro serializer to fail the send");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    @Test
    public void batchSendRefusesRawBodyWhenAvroHelperIsMissing() throws Exception {
        CpiKafkaPlusProducer producer = newProducer(avroEndpoint("JSON_ARRAY", false));

        try {
            invoke(producer, "buildBatchValueSerializer");
            Assert.fail("expected a configured but missing Avro serializer to fail the batch");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    @Test
    public void reconnectRacingReinitializationKeepsTheNewProducer() throws Exception {
        CpiKafkaPlusEndpoint endpoint = plainEndpoint();
        CpiKafkaPlusProducer producer = newProducer(endpoint);
        CountDownLatch closeEntered = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        setField(producer, "kafkaProducer", new SlowClosingProducer(closeEntered, releaseClose));
        setField(producer, "helpersInitialized", true);
        setField(producer, "initialized", true);

        AtomicReference<Throwable> reconnectFailure = new AtomicReference<>();
        Thread reconnecting = new Thread(() -> {
            try {
                invoke(producer, "triggerReconnect");
            } catch (Throwable t) {
                reconnectFailure.set(t);
            }
        }, "reconnect");
        reconnecting.start();
        try {
            Assert.assertTrue("precondition: the reconnect is closing the old producer",
                    closeEntered.await(5, TimeUnit.SECONDS));

            // A sender thread re-initializes while the old producer is still closing.
            invoke(producer, "ensureInitialized");
        } finally {
            releaseClose.countDown();
            reconnecting.join(10_000);
        }

        Assert.assertNull(reconnectFailure.get());
        Assert.assertTrue("precondition: the sender thread completed the re-initialization",
                (Boolean) getField(producer, "initialized"));
        Assert.assertNotNull("a ready producer must have a client, or every exchange fails until "
                + "the iFlow is restarted", getField(producer, "kafkaProducer"));
        invoke(producer, "doStop");
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private CpiKafkaPlusEndpoint plainEndpoint() {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(ctx);
        endpoint.setBootstrapServers("localhost:9092");
        endpoint.setSecurityProtocol("PLAINTEXT");
        endpoint.setTopic("orders");
        return endpoint;
    }

    private CpiKafkaPlusEndpoint avroEndpoint(String batchMode, boolean transactional) {
        CpiKafkaPlusEndpoint endpoint = plainEndpoint();
        endpoint.setProducerBatchMode(batchMode);
        endpoint.setEnableTransactions(transactional);
        endpoint.setSchemaRegistryEnabled(true);
        endpoint.setSchemaRegistryUrl(schemaRegistry.url("/").toString());
        endpoint.setAvroValueSerialization(true);
        endpoint.setSubjectNameStrategy("TopicNameStrategy");
        return endpoint;
    }

    private static CpiKafkaPlusProducer newProducer(CpiKafkaPlusEndpoint endpoint) throws Exception {
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        // Normally assigned in doStart(); injected so the lifecycle can run without a broker.
        setField(producer, "tracingHelper", new AdapterTracingHelper(endpoint));
        return producer;
    }

    private Message message(String body) {
        Exchange exchange = new DefaultExchange(ctx);
        exchange.getIn().setBody(body);
        return exchange.getIn();
    }

    @SuppressWarnings("unchecked")
    private static Set<String> verifiedTopics(CpiKafkaPlusProducer producer) throws Exception {
        return (Set<String>) getField(producer, "verifiedTopics");
    }

    private static Object invoke(Object target, String name) throws Exception {
        return invoke(target, name, new Class<?>[0]);
    }

    private static Object invoke(Object target, String name, Class<?>[] types, Object... args)
            throws Exception {
        Method m = target.getClass().getDeclaredMethod(name, types);
        m.setAccessible(true);
        try {
            return m.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw (Error) cause;
        }
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Properties stubProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-lifecycle-test");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        return props;
    }

    /** Acknowledges every send immediately; transaction calls are no-ops. */
    private static final class AcknowledgingProducer extends KafkaProducer<byte[], byte[]> {
        AcknowledgingProducer() {
            super(stubProps(), new ByteArraySerializer(), new ByteArraySerializer());
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            return CompletableFuture.completedFuture(new RecordMetadata(
                    new TopicPartition(record.topic(), 0), 0, 0, System.currentTimeMillis(), 0, 0));
        }

        @Override
        public void initTransactions() {
        }

        @Override
        public void beginTransaction() {
        }

        @Override
        public void commitTransaction() {
        }

        @Override
        public void close(Duration timeout) {
            super.close(Duration.ZERO);
        }
    }

    /** Holds close() open until released, like a producer draining in-flight requests for 5 s. */
    private static final class SlowClosingProducer extends KafkaProducer<byte[], byte[]> {
        private final CountDownLatch closeEntered;
        private final CountDownLatch releaseClose;

        SlowClosingProducer(CountDownLatch closeEntered, CountDownLatch releaseClose) {
            super(stubProps(), new ByteArraySerializer(), new ByteArraySerializer());
            this.closeEntered = closeEntered;
            this.releaseClose = releaseClose;
        }

        @Override
        public void close(Duration timeout) {
            closeEntered.countDown();
            try {
                releaseClose.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            super.close(Duration.ZERO);
        }
    }
}
