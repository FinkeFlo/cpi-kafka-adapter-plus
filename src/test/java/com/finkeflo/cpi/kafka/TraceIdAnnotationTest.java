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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Every message the sender delivers carries the trace IDs of its records into the MPL (#168).
 */
public class TraceIdAnnotationTest {

    private static final String A = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String B = "0af7651916cd43dd8448eb211c80319c";

    private final List<DefaultCamelContext> contexts = new ArrayList<>();
    private final List<Exchange> delivered = new ArrayList<>();
    private final List<List<String>> annotated = new ArrayList<>();

    @After
    public void tearDown() throws Exception {
        for (DefaultCamelContext ctx : contexts) {
            ctx.close();
        }
    }

    @Test
    public void aSingleRecordMessageCarriesItsTraceId() throws Exception {
        ConsumerRecords<byte[], byte[]> records = records(record(0, A), record(1, null));

        processor(endpoint("")).processSingleRecords(null, records, false, new PollProgress(records));

        Assert.assertEquals(2, delivered.size());
        Assert.assertEquals(Collections.singletonList(Collections.singletonList(A)), annotated);
    }

    @Test
    public void aBatchMessageCarriesTheDistinctTraceIdsOfItsRecords() throws Exception {
        ConsumerRecords<byte[], byte[]> records = records(record(0, A), record(1, B), record(2, A));

        processor(endpoint("&batchMode=true&batchSize=10"))
                .processBatchRecords(null, records, false, new PollProgress(records));

        Assert.assertEquals(1, delivered.size());
        Assert.assertEquals(Collections.singletonList(Arrays.asList(A, B)), annotated);
    }

    @Test
    public void aRecordOfAFailedBatchCarriesItsOwnTraceId() throws Exception {
        List<ConsumerRecord<byte[], byte[]>> batch = Arrays.asList(record(0, A), record(1, B));
        ConsumerRecords<byte[], byte[]> records = records(batch.toArray(new ConsumerRecord[0]));

        processor(endpoint("&batchMode=true&batchSize=10"))
                .processRecordsIndividually(null, batch, false, new PollProgress(records));

        Assert.assertEquals(2, delivered.size());
        Assert.assertEquals(Boolean.TRUE, delivered.get(0).getIn().getHeader("CpiKafkaPlusFallbackMode"));
        Assert.assertEquals(Arrays.asList(Collections.singletonList(A), Collections.singletonList(B)), annotated);
    }

    private CpiKafkaPlusEndpoint endpoint(String options) throws Exception {
        DefaultCamelContext ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
        contexts.add(ctx);
        return (CpiKafkaPlusEndpoint) ctx.getEndpoint("cpi-kafka-plus:orders?bootstrapServers=localhost:9092"
                + "&groupId=g&commitStrategy=BATCH_COMPLETE" + options);
    }

    private static ConsumerRecord<byte[], byte[]> record(long offset, String traceId) {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("orders", 0, offset, null,
                "{}".getBytes(StandardCharsets.UTF_8));
        if (traceId != null) {
            record.headers().add("traceparent",
                    ("00-" + traceId + "-00f067aa0ba902b7-01").getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    @SuppressWarnings("unchecked")
    private static ConsumerRecords<byte[], byte[]> records(ConsumerRecord<byte[], byte[]>... records) {
        return new ConsumerRecords<>(Collections.singletonMap(new TopicPartition("orders", 0),
                Arrays.asList(records)));
    }

    private RecordProcessor processor(CpiKafkaPlusEndpoint endpoint) {
        AdapterTracingHelper tracing = new AdapterTracingHelper(endpoint) {
            @Override
            public void annotateTraceIds(Exchange exchange, Collection<String> traceIds) {
                if (!traceIds.isEmpty()) {
                    annotated.add(new ArrayList<>(traceIds));
                }
            }
        };
        return new RecordProcessor(endpoint, tracing, null, null, null,
                new RecordProcessor.ConsumerCallback() {
                    @Override
                    public void processExchange(Exchange exchange) {
                        delivered.add(exchange);
                    }

                    @Override
                    public void handleException(String message, Exchange exchange, Exception e) {
                    }

                    @Override
                    public Exchange createExchange() {
                        return endpoint.createExchange();
                    }
                }, new OffsetCommitTracker());
    }
}
