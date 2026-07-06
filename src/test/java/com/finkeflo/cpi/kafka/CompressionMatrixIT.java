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
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for adapter producer compression codecs with a real Kafka broker.
 */
public class CompressionMatrixIT {

    private static DefaultCamelContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        KafkaTestInfrastructure.requireDockerAvailable();
        KafkaTestInfrastructure.startKafka();

        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (ctx != null) {
            ctx.stop();
        }
    }

    @Test
    public void testCompressionNoneRoundTrip() throws Exception {
        assertCompressionRoundTrip("none");
    }

    @Test
    public void testCompressionGzipRoundTrip() throws Exception {
        assertCompressionRoundTrip("gzip");
    }

    @Test
    public void testCompressionLz4RoundTrip() throws Exception {
        assertCompressionRoundTrip("lz4");
    }

    @Test
    public void testCompressionZstdRoundTrip() throws Exception {
        assertCompressionRoundTrip("zstd");
    }

    private void assertCompressionRoundTrip(String compressionType) throws Exception {
        String topic = "it-compression-" + compressionType + "-" + System.nanoTime();
        KafkaTestInfrastructure.createTopic(topic, 1);

        String[] payloads = new String[] {
                payloadFor(compressionType, 0),
                payloadFor(compressionType, 1),
                payloadFor(compressionType, 2)
        };

        Map<String, String> params = new HashMap<String, String>();
        params.put("compressionType", compressionType);

        CpiKafkaPlusProducer producer = createProducer(topic, params);
        try {
            producer.doStart();
            for (int i = 0; i < payloads.length; i++) {
                Exchange exchange = new DefaultExchange(ctx);
                exchange.getIn().setBody(payloads[i]);
                exchange.getIn().setHeader("kafka.KEY", compressionType + "-key-" + i);
                producer.process(exchange);
            }
        } finally {
            producer.doStop();
        }

        List<ConsumerRecord<String, String>> records =
                KafkaTestInfrastructure.consumeAllMessages(topic, payloads.length, 10000);
        Assert.assertEquals("Should receive every record for compressionType=" + compressionType,
                payloads.length, records.size());
        for (int i = 0; i < payloads.length; i++) {
            Assert.assertEquals("Payload must round-trip unchanged for compressionType="
                    + compressionType + ", record=" + i, payloads[i], records.get(i).value());
        }
    }

    private String payloadFor(String compressionType, int index) {
        StringBuilder sb = new StringBuilder();
        sb.append("compression=").append(compressionType)
                .append(";index=").append(index)
                .append(";payload=");
        for (int i = 0; i < 256; i++) {
            sb.append("SAP-CPI-KAFKA-ADAPTER-PLUS-");
            sb.append(compressionType);
            sb.append('-');
            sb.append(index);
            sb.append(';');
        }
        return sb.toString();
    }

    private CpiKafkaPlusProducer createProducer(String topic, Map<String, String> params) throws Exception {
        String uri = KafkaTestInfrastructure.buildEndpointUri(topic, "unused-group", params);
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri);
        return new CpiKafkaPlusProducer(endpoint);
    }
}
