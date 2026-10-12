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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Which exchange headers the receiver writes to a record, and how (#195).
 */
public class ProducerRecordHeadersTest {

    private DefaultCamelContext ctx;

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        ctx.close();
    }

    @Test
    public void theDefaultSendsTheFlowsHeadersButNoCredentialsAndNoInternalHeaders() {
        Exchange exchange = exchangeWithHeaders();

        ProducerRecord<byte[], byte[]> record = recordFor("*", exchange);

        Assert.assertEquals("[orderId, SAP_Sender]", names(record).toString());
        Assert.assertEquals("42", text(record, "orderId"));
    }

    @Test
    public void aCredentialHeaderNamedExactlyIsSent() {
        Exchange exchange = exchangeWithHeaders();

        ProducerRecord<byte[], byte[]> record = recordFor("*|authorization", exchange);

        Assert.assertEquals("[Authorization, orderId, SAP_Sender]", names(record).toString());
        Assert.assertEquals("Bearer eyJhbGciOiJIUzI1NiJ9", text(record, "Authorization"));
    }

    @Test
    public void anEmptyListSendsNoHeader() {
        Assert.assertTrue(names(recordFor("", exchangeWithHeaders())).isEmpty());
    }

    @Test
    public void aByteArrayValueIsSentAsItIs() {
        Exchange exchange = new DefaultExchange(ctx);
        byte[] digest = {0x00, 0x7F, (byte) 0x80, (byte) 0xFF};
        exchange.getIn().setHeader("payloadDigest", digest);

        ProducerRecord<byte[], byte[]> record = recordFor("*", exchange);

        Assert.assertArrayEquals(digest, record.headers().lastHeader("payloadDigest").value());
    }

    private Exchange exchangeWithHeaders() {
        Exchange exchange = new DefaultExchange(ctx);
        exchange.getIn().setHeader("Authorization", "Bearer eyJhbGciOiJIUzI1NiJ9");
        exchange.getIn().setHeader("Cookie", "JSESSIONID=4711");
        exchange.getIn().setHeader("CamelHttpUri", "https://erp.example.com/orders");
        exchange.getIn().setHeader("kafka.KEY", "order-42");
        exchange.getIn().setHeader("CpiKafkaPlusTopic", "orders");
        exchange.getIn().setHeader("SAP_Sender", "ERP");
        exchange.getIn().setHeader("orderId", 42);
        return exchange;
    }

    private ProducerRecord<byte[], byte[]> recordFor(String allowedHeaders, Exchange exchange) {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(ctx);
        endpoint.setBootstrapServers("localhost:9092");
        endpoint.setTopic("orders");
        endpoint.setAllowedHeaders(allowedHeaders);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("orders", new byte[0]);
        new CpiKafkaPlusProducer(endpoint).addRecordHeaders(record, exchange.getIn());
        return record;
    }

    private static List<String> names(ProducerRecord<byte[], byte[]> record) {
        List<String> names = new ArrayList<>();
        for (Header header : record.headers()) {
            names.add(header.key());
        }
        names.sort(String.CASE_INSENSITIVE_ORDER);
        return names;
    }

    private static String text(ProducerRecord<byte[], byte[]> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
