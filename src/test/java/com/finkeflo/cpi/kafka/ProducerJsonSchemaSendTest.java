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

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.finkeflo.cpi.kafka.TopicMetadataChecker.TopicCheckResult;

/**
 * A single-message receiver with JSON Schema validation sends the body it validated (#246). A body
 * that is a stream and not cached can be read only once; validating it as a String and reading it
 * again for the send used to send it empty, with status OK.
 */
public class ProducerJsonSchemaSendTest {

    private static final String SCHEMA = "{\"type\":\"object\",\"required\":[\"id\"]}";

    private DefaultCamelContext ctx;
    private CpiKafkaPlusProducer producer;
    private RecordingClient client;

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.start();
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(ctx);
        endpoint.setBootstrapServers("localhost:9092");
        endpoint.setTopic("orders");
        endpoint.setProducerBatchMode("NONE");
        endpoint.setJsonSchemaValidation(true);
        endpoint.setJsonSchema(SCHEMA);

        producer = new CpiKafkaPlusProducer(endpoint);
        // Normally assigned in doStart() and on the first send; injected so no broker is needed.
        setField(producer, "tracingHelper", new AdapterTracingHelper(endpoint));
        setField(producer, "jsonSchemaValidator", new JsonSchemaValidator(SCHEMA));
        client = new RecordingClient();
        setField(producer, "kafkaProducer", client);
        producer.topicChecker.topicProbe = topic -> TopicCheckResult.EXISTS;
    }

    @After
    public void tearDown() throws Exception {
        client.close(Duration.ZERO);
        ctx.close();
    }

    @Test
    public void aStreamBodyIsValidatedAndSentInFull() throws Exception {
        String json = "{\"id\":\"42\",\"name\":\"Müller\"}";
        Exchange exchange = new DefaultExchange(ctx);
        exchange.getIn().setBody(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

        producer.doProcess(exchange);

        Assert.assertEquals("OK", exchange.getIn().getHeader("CpiKafkaPlusStatus"));
        Assert.assertEquals(1, client.records().size());
        Assert.assertArrayEquals(json.getBytes(StandardCharsets.UTF_8), client.records().get(0).value());
    }

    @Test
    public void anInvalidStreamBodyIsRejectedAndNotSent() throws Exception {
        Exchange exchange = new DefaultExchange(ctx);
        exchange.getIn().setBody(new ByteArrayInputStream("{\"name\":\"x\"}".getBytes(StandardCharsets.UTF_8)));

        try {
            producer.doProcess(exchange);
            Assert.fail("expected the body without the required id to be rejected");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("Outbound "));
        }
        Assert.assertEquals(0, client.records().size());
    }

    @Test
    public void theBodyIsValidatedInTheExchangeCharset() throws Exception {
        // The text the schema sees is the one Camel's String conversion gave before the body was
        // read only once: decoded as UTF-8, the ü of this ISO-8859-1 body would fail the enum.
        setField(producer, "jsonSchemaValidator", new JsonSchemaValidator(
                "{\"type\":\"object\",\"properties\":{\"name\":{\"enum\":[\"Müller\"]}}}"));
        byte[] latin1 = "{\"id\":\"42\",\"name\":\"Müller\"}".getBytes(StandardCharsets.ISO_8859_1);
        Exchange exchange = new DefaultExchange(ctx);
        exchange.setProperty(Exchange.CHARSET_NAME, "ISO-8859-1");
        exchange.getIn().setBody(latin1);

        producer.doProcess(exchange);

        Assert.assertEquals(1, client.records().size());
        Assert.assertArrayEquals(latin1, client.records().get(0).value());
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    /** A client that never connects: records every send and acknowledges it. */
    private static final class RecordingClient extends KafkaProducer<byte[], byte[]> {
        private final List<ProducerRecord<byte[], byte[]>> records = new CopyOnWriteArrayList<>();

        RecordingClient() {
            super(stubProps(), new ByteArraySerializer(), new ByteArraySerializer());
        }

        List<ProducerRecord<byte[], byte[]>> records() {
            return records;
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return Collections.emptyList();
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            records.add(record);
            return CompletableFuture.completedFuture(
                    new RecordMetadata(new TopicPartition(record.topic(), 0), 0L, 0, 0L, 0, 0));
        }

        @Override
        public void close(Duration timeout) {
            super.close(Duration.ZERO);
        }

        private static Properties stubProps() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-json-schema-send-test");
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
            return props;
        }
    }
}
