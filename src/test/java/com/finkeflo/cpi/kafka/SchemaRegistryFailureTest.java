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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

/**
 * A record whose schema the Schema Registry cannot serve for a reason outside the record is told
 * apart from a record that is not Avro or names a schema the registry does not know (#188): only the
 * first kind counts as transient, so the sender blocks the partition instead of dead-lettering it.
 */
public class SchemaRegistryFailureTest {

    private static final int SCHEMA_ID = 7;
    private static final InetAddress LOOPBACK = loopback();

    private MockWebServer server;
    private ServerSocket plainHttpServer;

    @Before
    public void setUp() throws Exception {
        CredentialHelper.setCredentialResolver(null);
        server = new MockWebServer();
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        CredentialHelper.setCredentialResolver(null);
        server.shutdown();
        if (plainHttpServer != null) {
            plainHttpServer.close();
        }
    }

    @Test
    public void aRegistryAnswering503IsTransient() {
        server.enqueue(registryError(503, 50301, "Service unavailable"));

        RuntimeException failure = deserializeFails(server.url("/").toString());

        Assert.assertEquals(503, registryFailure(failure).getHttpStatus());
        Assert.assertTrue(KafkaErrorHelper.isTransientForRedelivery(failure));
    }

    @Test
    public void aRejectedLoginIsTransient() {
        server.enqueue(registryError(401, 40101, "Unauthorized"));

        RuntimeException failure = deserializeFails(server.url("/").toString());

        Assert.assertEquals(401, registryFailure(failure).getHttpStatus());
        Assert.assertTrue(KafkaErrorHelper.isTransientForRedelivery(failure));
    }

    @Test
    public void aSchemaIdTheRegistryDoesNotKnowIsNotTransient() {
        server.enqueue(registryError(404, 40403, "Schema not found"));

        RuntimeException failure = deserializeFails(server.url("/").toString());

        Assert.assertEquals(404, registryFailure(failure).getHttpStatus());
        Assert.assertFalse(KafkaErrorHelper.isTransientForRedelivery(failure));
    }

    @Test
    public void aRegistryThatRefusesTheConnectionIsTransient() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0, 1, LOOPBACK)) {
            closedPort = socket.getLocalPort();
        }

        RuntimeException failure = deserializeFails("http://127.0.0.1:" + closedPort);

        SchemaRegistryHttpClient.SchemaRegistryException registryFailure = registryFailure(failure);
        Assert.assertEquals(SchemaRegistryHttpClient.SchemaRegistryException.NO_RESPONSE,
                registryFailure.getHttpStatus());
        Assert.assertTrue(failure.getMessage(), failure.getMessage().startsWith(
                "Schema Registry GET /schemas/ids/" + SCHEMA_ID + " failed: java.net.ConnectException"));
        Assert.assertTrue(KafkaErrorHelper.isTransientForRedelivery(failure));
    }

    @Test
    public void aFailedTlsHandshakeIsTransient() throws Exception {
        // The SSLException is an IOException but no java.net type: only the registry client's own
        // exception says that the registry, not the record, failed.
        plainHttpServer = plainHttpServer();

        RuntimeException failure = deserializeFails("https://127.0.0.1:" + plainHttpServer.getLocalPort());

        SchemaRegistryHttpClient.SchemaRegistryException registryFailure = registryFailure(failure);
        Assert.assertEquals(SchemaRegistryHttpClient.SchemaRegistryException.NO_RESPONSE,
                registryFailure.getHttpStatus());
        Assert.assertTrue(String.valueOf(registryFailure.getCause()),
                registryFailure.getCause() instanceof javax.net.ssl.SSLException);
        Assert.assertTrue(KafkaErrorHelper.isTransientForRedelivery(failure));
    }

    @Test
    public void aTruncatedAvroPayloadIsNotTransient() {
        // Reading the record fails with an EOFException, an I/O failure that is the record's own.
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                .setBody("{\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Order\\\","
                        + "\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"string\\\"}]}\"}"));
        // A string of 10 bytes, of which only 2 arrived.
        byte[] truncated = record(new byte[] {20, 'a', 'b'});

        RuntimeException failure = deserializeFails(server.url("/").toString(), truncated);

        Assert.assertTrue(String.valueOf(CauseChain.root(failure)), CauseChain.root(failure) instanceof java.io.EOFException);
        Assert.assertFalse(KafkaErrorHelper.isTransientForRedelivery(failure));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static RuntimeException deserializeFails(String registryUrl) {
        return deserializeFails(registryUrl, record(new byte[] {2, 'a'}));
    }

    private static RuntimeException deserializeFails(String registryUrl, byte[] value) {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setSchemaRegistryUrl(registryUrl);
        try (AvroDeserializerHelper helper = new AvroDeserializerHelper(endpoint)) {
            helper.deserialize("orders", value);
        } catch (RuntimeException expected) {
            return expected;
        }
        throw new AssertionError("expected the record to fail");
    }

    private static SchemaRegistryHttpClient.SchemaRegistryException registryFailure(RuntimeException failure) {
        Assert.assertTrue(String.valueOf(failure),
                failure instanceof SchemaRegistryHttpClient.SchemaRegistryException);
        return (SchemaRegistryHttpClient.SchemaRegistryException) failure;
    }

    /** {@code avro} in the Confluent wire format, with schema ID {@link #SCHEMA_ID}. */
    private static byte[] record(byte[] avro) {
        return ByteBuffer.allocate(5 + avro.length)
                .put(AvroDeserializerHelper.MAGIC_BYTE)
                .putInt(SCHEMA_ID)
                .put(avro)
                .array();
    }

    private static MockResponse registryError(int httpStatus, int errorCode, String message) {
        return new MockResponse()
                .setResponseCode(httpStatus)
                .setHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                .setBody("{\"error_code\":" + errorCode + ",\"message\":\"" + message + "\"}");
    }

    private static InetAddress loopback() {
        try {
            return InetAddress.getByName("127.0.0.1");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Answers every connection in plain HTTP, so a TLS handshake with it fails. */
    private static ServerSocket plainHttpServer() throws IOException {
        ServerSocket socket = new ServerSocket(0, 50, LOOPBACK);
        Thread acceptor = new Thread(() -> {
            while (!socket.isClosed()) {
                try (Socket connection = socket.accept()) {
                    connection.setSoTimeout(5_000);
                    InputStream in = connection.getInputStream();
                    in.read(new byte[16 * 1024]); // the TLS ClientHello
                    OutputStream out = connection.getOutputStream();
                    out.write("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n"
                            .getBytes(StandardCharsets.US_ASCII));
                    out.flush();
                    // Closing with the reply unread could reset the connection before the client
                    // reads it, so wait until the client gives up.
                    while (in.read() != -1) {
                        // discard
                    }
                } catch (IOException e) {
                    // closed by the test, or the client reset the connection
                }
            }
        }, "plain-http-server");
        acceptor.setDaemon(true);
        acceptor.start();
        return socket;
    }
}
