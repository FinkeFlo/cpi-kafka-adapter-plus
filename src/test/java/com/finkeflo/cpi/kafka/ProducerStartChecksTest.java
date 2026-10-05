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

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Deployment-time checks of the receiver: reject values that can never work (#183), and only where the
 * field is in effect.
 */
public class ProducerStartChecksTest {

    private DefaultCamelContext ctx;

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        ctx.close();
    }

    @Test
    public void emptyBootstrapServersAreRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setBootstrapServers(" ");
        assertRejected(e, "bootstrapServers");
    }

    @Test
    public void malformedJsonSchemaIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setJsonSchemaValidation(true);
        e.setJsonSchema("{\"type\": \"object\",");
        assertRejected(e, "JSON Schema");
    }

    @Test
    public void malformedJsonSchemaIsIgnoredWhileValidationIsOff() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setJsonSchemaValidation(false);
        e.setJsonSchema("{\"type\": \"object\",");
        assertStarts(e);
    }

    @Test
    public void unknownProducerBatchModeIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("CSV");
        assertRejected(e, "producerBatchMode");
    }

    @Test
    public void producerBatchModeIsCaseInsensitiveAsAtRuntime() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setProducerBatchMode("json_array");
        assertStarts(e);
    }

    @Test
    public void deliveryTimeoutBelowOneSecondIsRejected() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setDeliveryTimeoutSeconds(0);
        assertRejected(e, "deliveryTimeoutSeconds");
    }

    @Test
    public void unsupportedSubjectNameStrategyIsRejectedForAvroSerialization() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setSchemaRegistryEnabled(true);
        e.setSchemaRegistryUrl("http://localhost:8081");
        e.setAvroValueSerialization(true);
        e.setSubjectNameStrategy("RecordNameStrategy");
        assertRejected(e, "subjectNameStrategy");
    }

    @Test
    public void subjectNameStrategyIsIgnoredWithoutAvroSerialization() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint();
        e.setSchemaRegistryEnabled(false);
        e.setSubjectNameStrategy("RecordNameStrategy");
        assertStarts(e);
    }

    @Test
    public void receiverWithoutGroupIdStarts() throws Exception {
        // groupId is a sender field; the endpoint() URI has none on purpose.
        assertStarts(endpoint());
    }

    private CpiKafkaPlusEndpoint endpoint() throws Exception {
        return (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:orders?bootstrapServers=localhost:9999&securityProtocol=PLAINTEXT");
    }

    private static void assertRejected(CpiKafkaPlusEndpoint endpoint, String field) throws Exception {
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        try {
            producer.doStart();
            Assert.fail("expected the deployment to be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage(), expected.getMessage().contains(field));
        } finally {
            try {
                producer.doStop();
            } catch (Exception ignored) {
                // start failed, so stop may too — not what is under test
            }
        }
    }

    private static void assertStarts(CpiKafkaPlusEndpoint endpoint) throws Exception {
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        try {
            producer.doStart();
        } finally {
            producer.doStop();
        }
    }
}
