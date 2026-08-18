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
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Assert;
import org.junit.Test;

public class CpiKafkaPlusProducerSinglePathRetryTest {

    @Test
    public void singlePathRetriesMonitorFaultAndSucceeds() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx);
            Exchange exchange = new DefaultExchange(ctx);
            Message in = exchange.getIn();
            in.setBody("payload");

            RecordMetadata metadata = new RecordMetadata(
                    new TopicPartition("orders", 2), 77L, 0, 1234L, 0, 0);
            try (ScriptedKafkaProducer kafkaProducer = new ScriptedKafkaProducer(
                    1, CompletableFuture.completedFuture(metadata))) {
                setField(producer, "kafkaProducer", kafkaProducer);

                invokeProcessSingle(producer, exchange, in, "orders");

                Assert.assertEquals("single path must retry the sync send call on monitor fault",
                        2, kafkaProducer.sendCalls());
                Assert.assertEquals("OK", in.getHeader("CpiKafkaPlusStatus"));
                Assert.assertEquals("orders", in.getHeader("CpiKafkaPlusTopic"));
                Assert.assertEquals(2, in.getHeader("CpiKafkaPlusPartition"));
                Assert.assertEquals(77L, in.getHeader("CpiKafkaPlusOffset"));
            }
        }
    }

    @Test
    public void singlePathFailsCleanlyWhenRetryBudgetIsExhausted() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusProducer producer = newProducer(ctx);
            // Keep this failure-focused test deterministic: no producer rebuild side effects.
            setField(producer, "lastRebuildAttemptMs", System.currentTimeMillis());

            Exchange exchange = new DefaultExchange(ctx);
            Message in = exchange.getIn();
            in.setBody("payload");

            try (ScriptedKafkaProducer kafkaProducer = new ScriptedKafkaProducer(
                    Integer.MAX_VALUE, null)) {
                setField(producer, "kafkaProducer", kafkaProducer);

                try {
                    invokeProcessSingle(producer, exchange, in, "orders");
                    Assert.fail("expected send failure after exhausting monitor-fault retries");
                } catch (RuntimeException expected) {
                    Assert.assertTrue("single path must stop after per-record retry budget",
                            kafkaProducer.sendCalls() == MonitorFaultRetry.MAX_RETRIES_PER_RECORD + 1);
                    Assert.assertTrue("failure must keep monitor fault signature",
                            KafkaErrorHelper.isMetadataMonitorFault(expected));
                }

                Assert.assertNull(in.getHeader("CpiKafkaPlusStatus"));
            }
        }
    }

    private static CpiKafkaPlusProducer newProducer(DefaultCamelContext ctx) throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(ctx);
        endpoint.setBootstrapServers("localhost:9092");
        endpoint.setTopic("orders");
        endpoint.setProducerBatchMode("NONE");

        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        setField(producer, "tracingHelper", new AdapterTracingHelper(endpoint));
        return producer;
    }

    private static void invokeProcessSingle(CpiKafkaPlusProducer producer, Exchange exchange,
                                            Message in, String topic) throws Exception {
        Method m = CpiKafkaPlusProducer.class.getDeclaredMethod(
                "processSingle", Exchange.class, Message.class, String.class);
        m.setAccessible(true);
        try {
            m.invoke(producer, exchange, in, topic);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static RuntimeException monitorFault() {
        IllegalMonitorStateException imse = new IllegalMonitorStateException("current thread is not owner");
        imse.setStackTrace(new StackTraceElement[] {
            new StackTraceElement("org.apache.kafka.common.utils.SystemTime", "waitObject", "SystemTime.java", 62),
            new StackTraceElement("org.apache.kafka.clients.producer.internals.ProducerMetadata",
                    "awaitUpdate", "ProducerMetadata.java", 119),
            new StackTraceElement("org.apache.kafka.clients.producer.KafkaProducer",
                    "waitOnMetadata", "KafkaProducer.java", 1120)
        });
        return new RuntimeException("Batch send failed at record index 0: " + imse.getMessage(), imse);
    }

    private static final class ScriptedKafkaProducer extends KafkaProducer<byte[], byte[]> {
        private final int faultsBeforeSuccess;
        private final Future<RecordMetadata> successFuture;
        private final AtomicInteger sendCalls = new AtomicInteger();

        ScriptedKafkaProducer(int faultsBeforeSuccess, Future<RecordMetadata> successFuture) {
            super(testProducerProps(), new ByteArraySerializer(), new ByteArraySerializer());
            this.faultsBeforeSuccess = faultsBeforeSuccess;
            this.successFuture = successFuture;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            int call = sendCalls.getAndIncrement();
            if (call < faultsBeforeSuccess) {
                throw monitorFault();
            }
            if (successFuture == null) {
                throw monitorFault();
            }
            return successFuture;
        }

        int sendCalls() {
            return sendCalls.get();
        }
    }

    private static Properties testProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "single-path-retry-test");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        return props;
    }
}
