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
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

/**
 * Unit tests for Schema Registry HTTP interactions exercised through {@link AvroSerializerHelper}.
 */
public class AvroSerializerHelperTest {

    private MockWebServer server;

    @Before
    public void setUp() throws Exception {
        CredentialHelper.setCredentialResolver(null);
        server = new MockWebServer();
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        CredentialHelper.setCredentialResolver(null);
        if (server != null) {
            server.shutdown();
        }
    }

    @Test
    public void testSerializeSchemaNotFoundSurfacesClearMessage() throws Exception {
        server.enqueue(schemaRegistryError(404, 40401, "Subject not found"));

        try (AvroSerializerHelper helper = new AvroSerializerHelper(createEndpoint(false, null))) {
            try {
                helper.serialize("orders", "{\"id\":\"123\"}");
                Assert.fail("Expected schema lookup failure");
            } catch (RuntimeException e) {
                Assert.assertTrue("Expected clear missing-subject message but got: " + describeCauseChain(e),
                        e.getMessage().contains("Schema subject 'orders-value' not found in Schema Registry"));
                Assert.assertTrue("Expected RestClientException in cause chain but got: " + describeCauseChain(e),
                        hasCauseNamed(e, "RestClientException"));
            }
        }

        RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
        Assert.assertNotNull("Expected Schema Registry lookup request", request);
        Assert.assertEquals("GET", request.getMethod());
        Assert.assertTrue("Unexpected request path: " + request.getPath(),
                request.getPath().startsWith("/subjects/orders-value/versions/latest"));
    }

    @Test
    public void testSerializeUnauthorizedSendsBasicAuthHeader() throws Exception {
        CredentialHelper.setCredentialResolver(new CredentialHelper.CredentialResolver() {
            public CredentialHelper.UserCredentials resolveUserCredential(String alias) {
                if ("sr-alias".equals(alias)) {
                    return new CredentialHelper.UserCredentials("alice", "secret");
                }
                return null;
            }
        });
        server.enqueue(schemaRegistryError(401, 40101, "Unauthorized"));

        try (AvroSerializerHelper helper = new AvroSerializerHelper(createEndpoint(false, "sr-alias"))) {
            try {
                helper.serialize("payments", "{\"id\":\"42\"}");
                Assert.fail("Expected unauthorized schema lookup failure");
            } catch (RuntimeException e) {
                Assert.assertTrue("Expected auth failure details but got: " + describeCauseChain(e),
                        causeChainContains(e, "401") || causeChainContains(e, "unauthorized"));
            }
        }

        RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
        Assert.assertNotNull("Expected authenticated Schema Registry request", request);
        Assert.assertEquals("Basic " + Base64.getEncoder().encodeToString("alice:secret".getBytes(StandardCharsets.UTF_8)),
                request.getHeader("Authorization"));
        Assert.assertTrue("Unexpected request path: " + request.getPath(),
                request.getPath().startsWith("/subjects/payments-value/versions/latest"));
    }

    @Test
    public void testSerializeMalformedSchemaRegistryResponseSurfacesFailure() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                .setBody("{\"subject\":\"broken-value\",\"version\":1,\"id\":7,\"schema\":\"{not-valid-schema\"}"));

        try (AvroSerializerHelper helper = new AvroSerializerHelper(createEndpoint(false, null))) {
            try {
                helper.serialize("broken", "{\"id\":\"7\"}");
                Assert.fail("Expected malformed Schema Registry response to fail");
            } catch (RuntimeException e) {
                Assert.assertTrue("Expected wrapped serialization failure but got: " + describeCauseChain(e),
                        e.getMessage().startsWith("Avro serialization failed:"));
                Assert.assertTrue("Expected schema parsing details but got: " + describeCauseChain(e),
                        causeChainContains(e, "schema")
                                || causeChainContains(e, "json")
                                || causeChainContains(e, "parse")
                                || causeChainContains(e, "unexpected"));
            }
        }

        RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
        Assert.assertNotNull("Expected Schema Registry lookup request", request);
        Assert.assertTrue("Unexpected request path: " + request.getPath(),
                request.getPath().startsWith("/subjects/broken-value/versions/latest"));
    }

    private CpiKafkaPlusEndpoint createEndpoint(boolean autoRegisterSchemas, String credentialAlias) {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setSchemaRegistryUrl(server.url("/").toString());
        endpoint.setAutoRegisterSchemas(autoRegisterSchemas);
        endpoint.setSubjectNameStrategy("TopicNameStrategy");
        endpoint.setSchemaRegistryCredentialAlias(credentialAlias);
        return endpoint;
    }

    private MockResponse schemaRegistryError(int httpStatus, int errorCode, String message) {
        return new MockResponse()
                .setResponseCode(httpStatus)
                .setHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
                .setBody("{\"error_code\":" + errorCode + ",\"message\":\"" + message + "\"}");
    }

    private boolean hasCauseNamed(Throwable failure, String simpleClassName) {
        Throwable current = failure;
        while (current != null) {
            if (simpleClassName.equals(current.getClass().getSimpleName())) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean causeChainContains(Throwable failure, String needle) {
        String lowerNeedle = needle.toLowerCase();
        Throwable current = failure;
        while (current != null) {
            String className = current.getClass().getName().toLowerCase();
            String message = current.getMessage() == null ? "" : current.getMessage().toLowerCase();
            if (className.contains(lowerNeedle) || message.contains(lowerNeedle)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private String describeCauseChain(Throwable failure) {
        StringBuilder sb = new StringBuilder();
        Throwable current = failure;
        while (current != null) {
            if (sb.length() > 0) {
                sb.append(" -> " );
            }
            sb.append(current.getClass().getSimpleName());
            if (current.getMessage() != null) {
                sb.append("[").append(current.getMessage()).append("]");
            }
            current = current.getCause();
        }
        return sb.toString();
    }
}
