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
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Integration tests for {@link AvroDeserializerHelper} with a real Schema Registry.
 */
public class AvroDeserializerHelperIT {

    private static final String USER_SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"com.test\","
            + "\"fields\":["
            + "{\"name\":\"name\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"int\"},"
            + "{\"name\":\"email\",\"type\":\"string\"}"
            + "]}";

    private static final String NESTED_SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.test\","
            + "\"fields\":["
            + "{\"name\":\"orderId\",\"type\":\"string\"},"
            + "{\"name\":\"customer\",\"type\":" + USER_SCHEMA_JSON + "}"
            + "]}";

    private static AvroDeserializerHelper jsonHelper;
    private static AvroDeserializerHelper xmlHelper;
    private static KafkaAvroSerializer valueSerializer;
    private static Schema userSchema;
    private static Schema orderSchema;

    @BeforeClass
    public static void setUp() throws Exception {
        KafkaTestInfrastructure.requireDockerAvailable();
        KafkaTestInfrastructure.startKafkaWithSchemaRegistry();

        String schemaRegistryUrl = KafkaTestInfrastructure.getSchemaRegistryUrl();

        // Set up serializers
        Map<String, Object> serConfig = new HashMap<>();
        serConfig.put("schema.registry.url", schemaRegistryUrl);
        serConfig.put("auto.register.schemas", true);

        valueSerializer = new KafkaAvroSerializer();
        valueSerializer.configure(serConfig, false);

        // Create helpers via real endpoint
        DefaultCamelContext ctx = new DefaultCamelContext();
        CpiKafkaPlusComponent component = new CpiKafkaPlusComponent();
        ctx.addComponent("cpi-kafka-plus", component);
        ctx.start();

        CpiKafkaPlusEndpoint jsonEndpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:avro-test?bootstrapServers=" + KafkaTestInfrastructure.getBootstrapServers()
                + "&groupId=avro-test-group&securityProtocol=PLAINTEXT"
                + "&schemaRegistryEnabled=true&schemaRegistryUrl=" + schemaRegistryUrl
                + "&avroOutputFormat=JSON");
        jsonHelper = new AvroDeserializerHelper(jsonEndpoint);

        CpiKafkaPlusEndpoint xmlEndpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:avro-test-xml?bootstrapServers=" + KafkaTestInfrastructure.getBootstrapServers()
                + "&groupId=avro-test-group&securityProtocol=PLAINTEXT"
                + "&schemaRegistryEnabled=true&schemaRegistryUrl=" + schemaRegistryUrl
                + "&avroOutputFormat=XML");
        xmlHelper = new AvroDeserializerHelper(xmlEndpoint);

        userSchema = new Schema.Parser().parse(USER_SCHEMA_JSON);
        orderSchema = new Schema.Parser().parse(NESTED_SCHEMA_JSON);
    }

    @AfterClass
    public static void tearDown() {
        if (jsonHelper != null) {
            jsonHelper.close();
        }
        if (xmlHelper != null) {
            xmlHelper.close();
        }
        if (valueSerializer != null) {
            valueSerializer.close();
        }
    }

    @Test
    public void testDeserializeToJson() {
        GenericRecord user = new GenericData.Record(userSchema);
        user.put("name", "Alice");
        user.put("age", 30);
        user.put("email", "alice@example.com");

        byte[] avroBytes = valueSerializer.serialize("avro-json-test", user);
        String result = jsonHelper.deserialize("avro-json-test", avroBytes);

        Assert.assertNotNull(result);
        Assert.assertTrue("Should contain name field", result.contains("\"name\":\"Alice\""));
        Assert.assertTrue("Should contain age field", result.contains("\"age\":30"));
        Assert.assertTrue("Should contain email field", result.contains("\"email\":\"alice@example.com\""));
    }

    @Test
    public void testDeserializeToXml() {
        GenericRecord user = new GenericData.Record(userSchema);
        user.put("name", "Bob");
        user.put("age", 25);
        user.put("email", "bob@example.com");

        byte[] avroBytes = valueSerializer.serialize("avro-xml-test", user);
        String result = xmlHelper.deserialize("avro-xml-test", avroBytes);

        Assert.assertNotNull(result);
        Assert.assertTrue("Should contain record element", result.contains("<record>"));
        Assert.assertTrue("Should contain name", result.contains("<name>Bob</name>"));
        Assert.assertTrue("Should contain age", result.contains("<age>25</age>"));
        Assert.assertTrue("Should contain email", result.contains("<email>bob@example.com</email>"));
    }

    @Test
    public void testDeserializeNullData() {
        String result = jsonHelper.deserialize("any-topic", null);
        Assert.assertNull(result);
    }

    @Test
    public void testDeserializeEmptyData() {
        String result = jsonHelper.deserialize("any-topic", new byte[0]);
        Assert.assertNull(result);
    }

    @Test
    public void testDeserializeNestedRecord() {
        GenericRecord customer = new GenericData.Record(userSchema);
        customer.put("name", "Charlie");
        customer.put("age", 40);
        customer.put("email", "charlie@example.com");

        GenericRecord order = new GenericData.Record(orderSchema);
        order.put("orderId", "ORD-001");
        order.put("customer", customer);

        byte[] avroBytes = valueSerializer.serialize("avro-nested-test", order);
        String jsonResult = jsonHelper.deserialize("avro-nested-test", avroBytes);

        Assert.assertNotNull(jsonResult);
        Assert.assertTrue("Should contain orderId", jsonResult.contains("\"orderId\":\"ORD-001\""));
        Assert.assertTrue("Should contain nested customer name", jsonResult.contains("Charlie"));

        byte[] avroBytesXml = valueSerializer.serialize("avro-nested-xml-test", order);
        String xmlResult = xmlHelper.deserialize("avro-nested-xml-test", avroBytesXml);

        Assert.assertNotNull(xmlResult);
        Assert.assertTrue("Should contain orderId element", xmlResult.contains("<orderId>ORD-001</orderId>"));
        Assert.assertTrue("Should contain nested record", xmlResult.contains("<name>Charlie</name>"));
    }

}
