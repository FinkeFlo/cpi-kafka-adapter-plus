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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The receiver validates single messages against the JSON Schema; a batch is sent without
 * validation (#263). With a batch send mode the schema is therefore not compiled, so a schema that
 * would not compile cannot stop the batches, and nothing is logged per message: the start says once
 * that the setting has no effect.
 */
public class ProducerJsonSchemaBatchTest {

    /** Compiles, but its {@code $ref} cannot be loaded: nothing listens on port 1. */
    private static final String UNREACHABLE_REF = "{\"$ref\": \"http://localhost:1/order.schema.json\"}";
    /** Valid JSON that does not compile: the pattern is not a regular expression. */
    private static final String UNCOMPILABLE = "{\"type\": \"object\", \"properties\": "
            + "{\"id\": {\"type\": \"string\", \"pattern\": \"[unclosed\"}}}";
    private static final String BATCH = "[{\"key\": \"k1\", \"value\": {\"id\": \"1\"}}, "
            + "{\"key\": \"k2\", \"value\": {\"id\": \"2\"}}]";

    private DefaultCamelContext ctx;

    @Before
    public void setUp() throws Exception {
        TlsListenerProbe.setProbeRunnerForTests(address -> TlsListenerProbe.Verdict.INCONCLUSIVE);
        ctx = new DefaultCamelContext();
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        TlsListenerProbe.clearCacheForTests();
        ctx.close();
    }

    @Test
    public void aBatchIsSentWhateverTheSchema() throws Exception {
        for (String schema : new String[] {UNREACHABLE_REF, UNCOMPILABLE}) {
            CpiKafkaPlusProducer producer = newProducer(batchEndpoint(schema));
            AtomicInteger sent = new AtomicInteger();
            try {
                String logged = ownLines(capture(() -> {
                    // Creates the shared producer and the helpers, as the first message would.
                    invoke(producer, "ensureInitialized");
                    Assert.assertTrue(schema, (Boolean) getField(producer, "initialized"));
                    Assert.assertNull(schema, getField(producer, "jsonSchemaValidator"));

                    // There is no broker: from here on every send is acknowledged at once.
                    ((KafkaProducer<?, ?>) getField(producer, "kafkaProducer")).close(Duration.ZERO);
                    setField(producer, "kafkaProducer", new AcknowledgingProducer(sent));
                    verifiedTopics(producer).add("orders");
                    sendTwoBatches(producer);
                }));

                Assert.assertEquals(schema, 4, sent.get());
                assertNothingPerMessage(logged);
            } finally {
                invoke(producer, "doStop");
            }
        }
    }

    @Test
    public void aTransactionalBatchIsSentWhateverTheSchema() throws Exception {
        for (String schema : new String[] {UNREACHABLE_REF, UNCOMPILABLE}) {
            CpiKafkaPlusEndpoint endpoint = batchEndpoint(schema);
            endpoint.setEnableTransactions(true);
            endpoint.setTransactionalIdPrefix("orders-tx");
            CpiKafkaPlusProducer producer = newProducer(endpoint);
            producer.txnPool = new TransactionalProducerPool(endpoint::getTransactionalIdPrefix,
                    "0000abcd", "test", 1);
            verifiedTopics(producer).add("orders");
            AtomicInteger sent = new AtomicInteger();
            producer.txnProducerFactory = props -> new AcknowledgingProducer(sent);
            try {
                String logged = ownLines(capture(() -> sendTwoBatches(producer)));

                Assert.assertEquals(schema, 4, sent.get());
                Assert.assertNull(schema, getField(producer, "jsonSchemaValidator"));
                assertNothingPerMessage(logged);
            } finally {
                invoke(producer, "doStop");
            }
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private void sendTwoBatches(CpiKafkaPlusProducer producer) throws Exception {
        for (int i = 0; i < 2; i++) {
            Exchange exchange = new DefaultExchange(ctx);
            exchange.getIn().setBody(BATCH);
            producer.doProcess(exchange);
            Assert.assertEquals("OK", exchange.getIn().getHeader("CpiKafkaPlusStatus"));
            Assert.assertEquals(2, exchange.getIn().getHeader("CpiKafkaPlusRecordCount"));
        }
    }

    private static void assertNothingPerMessage(String logged) {
        Assert.assertFalse(logged, logged.contains("JSON Schema"));
        Assert.assertFalse(logged, logged.contains("producer.init.failed"));
    }

    private CpiKafkaPlusEndpoint batchEndpoint(String schema) {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(ctx);
        endpoint.setBootstrapServers("localhost:9092");
        endpoint.setSecurityProtocol("PLAINTEXT");
        endpoint.setTopic("orders");
        endpoint.setProducerBatchMode("JSON_ARRAY");
        endpoint.setJsonSchemaValidation(true);
        endpoint.setJsonSchema(schema);
        return endpoint;
    }

    private static CpiKafkaPlusProducer newProducer(CpiKafkaPlusEndpoint endpoint) throws Exception {
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        // Normally assigned in doStart(), which would also start the background topic check.
        setField(producer, "tracingHelper", new AdapterTracingHelper(endpoint));
        return producer;
    }

    @SuppressWarnings("unchecked")
    private static Set<String> verifiedTopics(CpiKafkaPlusProducer producer) throws Exception {
        return (Set<String>) getField(producer.topicChecker, "verifiedTopics");
    }

    private static Object invoke(Object target, String name) throws Exception {
        Method m = target.getClass().getDeclaredMethod(name);
        m.setAccessible(true);
        try {
            return m.invoke(target);
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

    /** The lines of this thread: the Kafka clients log from their own threads. */
    private static String ownLines(String logged) {
        String own = "[" + Thread.currentThread().getName() + "] ";
        StringBuilder out = new StringBuilder();
        for (String line : logged.split("\\R")) {
            if (line.startsWith(own)) {
                out.append(line).append('\n');
            }
        }
        return out.toString();
    }

    private static Properties stubProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-json-schema-batch-test");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        return props;
    }

    /** Acknowledges every send at once and counts it; transaction calls are no-ops. */
    private static final class AcknowledgingProducer extends KafkaProducer<byte[], byte[]> {
        private final AtomicInteger sent;

        AcknowledgingProducer(AtomicInteger sent) {
            super(stubProps(), new ByteArraySerializer(), new ByteArraySerializer());
            this.sent = sent;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            int offset = sent.getAndIncrement();
            return CompletableFuture.completedFuture(new RecordMetadata(
                    new TopicPartition(record.topic(), 0), offset, 0, System.currentTimeMillis(), 0, 0));
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return Collections.singletonList(new PartitionInfo(topic, 0, null, new Node[0], new Node[0]));
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
}
