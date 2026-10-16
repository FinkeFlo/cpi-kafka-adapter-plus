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
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.Collections;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.InputSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

/**
 * Avro output as XML stays well-formed when a field holds a character that XML 1.0 does not allow
 * (#284): such a field carries its UTF-8 bytes in Base64, marked {@code format="base64"}.
 */
public class AvroXmlOutputTest {

    private static final int SCHEMA_ID = 42;
    private static final Schema SCHEMA = SchemaBuilder.record("Item").fields()
            .requiredString("id")
            .requiredString("note")
            .endRecord();

    private MockWebServer server;

    @Before
    public void setUp() throws Exception {
        CredentialHelper.setCredentialResolver(null);
        server = new MockWebServer();
        server.start();
        server.enqueue(new MockResponse().setResponseCode(200)
                .setHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                .setBody(new ObjectMapper().writeValueAsString(
                        Collections.singletonMap("schema", SCHEMA.toString()))));
    }

    @After
    public void tearDown() throws Exception {
        CredentialHelper.setCredentialResolver(null);
        server.shutdown();
    }

    @Test
    public void aFieldWithACharacterXmlDoesNotAllowIsWrittenAsBase64() throws Exception {
        String xml = deserializeAsXml("a", "x\u0001y");

        Assert.assertEquals("<record><id>a</id><note format=\"base64\">eAF5</note></record>", xml);
        DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
    }

    @Test
    public void otherFieldsAreEscapedAsBefore() throws Exception {
        Assert.assertEquals("<record><id>a&amp;b</id><note>tab\there</note></record>",
                deserializeAsXml("a&b", "tab\there"));
    }

    private String deserializeAsXml(String id, String note) throws Exception {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("id", id);
        record.put("note", note);
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        wire.write(0); // magic byte
        wire.write(ByteBuffer.allocate(4).putInt(SCHEMA_ID).array());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(wire, null);
        new GenericDatumWriter<GenericRecord>(SCHEMA).write(record, encoder);
        encoder.flush();

        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setSchemaRegistryUrl(server.url("/").toString());
        endpoint.setAvroOutputFormat("XML");
        try (AvroDeserializerHelper helper = new AvroDeserializerHelper(endpoint)) {
            return helper.deserialize("orders", wire.toByteArray());
        }
    }
}
