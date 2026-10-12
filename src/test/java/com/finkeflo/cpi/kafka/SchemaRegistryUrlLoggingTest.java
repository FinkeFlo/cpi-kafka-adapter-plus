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
import java.io.PrintStream;

import org.junit.Assert;
import org.junit.Test;

/**
 * A Schema Registry URL may carry {@code user:password@}. It must never reach a log line (#177).
 */
public class SchemaRegistryUrlLoggingTest {

    private static final String URL_WITH_CREDENTIALS = "https://alice:s3cr3t-pw@sr.example.com:8081/registry";

    @Test
    public void serializerLogsTheRegistryWithoutCredentials() throws Exception {
        String logged = captureStdErr(() -> new AvroSerializerHelper(endpoint()).close());

        Assert.assertTrue("precondition: the registry is logged\n" + logged, logged.contains("sr.example.com:8081"));
        Assert.assertFalse(logged, logged.contains("s3cr3t-pw"));
        Assert.assertFalse(logged, logged.contains("alice"));
    }

    @Test
    public void deserializerLogsTheRegistryWithoutCredentials() throws Exception {
        String logged = captureStdErr(() -> new AvroDeserializerHelper(endpoint()).close());

        Assert.assertTrue("precondition: the registry is logged\n" + logged, logged.contains("sr.example.com:8081"));
        Assert.assertFalse(logged, logged.contains("s3cr3t-pw"));
        Assert.assertFalse(logged, logged.contains("alice"));
    }

    @Test
    public void urlsWithoutCredentialsAreLoggedUnchanged() {
        Assert.assertEquals("https://sr.example.com:8081",
                SchemaRegistryHttpClient.withoutUserInfo("https://sr.example.com:8081"));
        Assert.assertEquals("https://***@sr.example.com:8081/registry",
                SchemaRegistryHttpClient.withoutUserInfo(URL_WITH_CREDENTIALS));
        Assert.assertNull(SchemaRegistryHttpClient.withoutUserInfo(null));
    }

    @Test
    public void credentialsInAUrlWithoutSchemeAreMaskedToo() throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint();
        endpoint.setSchemaRegistryUrl("alice:s3cr3t-pw@sr.example.com:8081");

        String logged = captureStdErr(() -> new AvroSerializerHelper(endpoint).close());

        Assert.assertTrue("precondition: the registry is logged\n" + logged, logged.contains("sr.example.com:8081"));
        Assert.assertFalse(logged, logged.contains("s3cr3t-pw"));
        Assert.assertFalse(logged, logged.contains("alice"));
        Assert.assertEquals("***@sr.example.com:8081/registry",
                SchemaRegistryHttpClient.withoutUserInfo("alice:s3cr3t-pw@sr.example.com:8081/registry"));
        Assert.assertEquals("an @ in the path is not user info", "sr.example.com:8081/a@b",
                SchemaRegistryHttpClient.withoutUserInfo("sr.example.com:8081/a@b"));
    }

    /**
     * The start check refuses a URL that does not parse, so the mask in its message cannot rely on
     * the URL's structure: a password with {@code /} or {@code @} and a key in the query or fragment
     * stay hidden (#261).
     */
    @Test
    public void aRefusedUrlShowsNoPartOfItsSecrets() {
        String[][] cases = {
                {"sr.example.com:8081", "sr.example.com:8081"},
                {"ftp://sr.example.com", "ftp://sr.example.com"},
                {"user:s3cr3t@sr.example.com:8081", "***@sr.example.com:8081"},
                {"htps://alice:s3/cr3t@sr:8081", "htps://***@sr:8081"},
                {"http://alice:s3/cr3t@sr:8081", "http://***@sr:8081"},
                {"htps://alice@s3cr3t:pw@sr:8081", "htps://***@sr:8081"},
                {"htp://sr:8081?apikey=s3cr3t", "htp://sr:8081?***"},
                {"htp://sr:8081/registry#s3cr3t", "htp://sr:8081/registry#***"},
                {"htps://alice:pw@sr:8081/x?key=s3@cr3t", "htps://***"},
                {"htps://alice:s3?cr3t@sr:8081", "htps://***"},
        };
        for (String[] c : cases) {
            Assert.assertEquals(c[0], c[1], SchemaRegistryHttpClient.maskedForRejection(c[0]));
            try {
                SchemaRegistryHttpClient.requireUsableUrl(c[0]);
                Assert.fail("expected " + c[0] + " to be refused");
            } catch (IllegalArgumentException expected) {
                String message = expected.getMessage();
                Assert.assertTrue(message, message.contains("got: '" + c[1] + "'"));
                Assert.assertFalse(message, message.contains("alice"));
                Assert.assertFalse(message, message.contains("cr3t"));
            }
        }
    }

    @Test
    public void anHttpUrlThatDoesNotParseIsNotToldToStartWithHttp() {
        try {
            SchemaRegistryHttpClient.requireUsableUrl("https://sr.example.com:80a81");
            Assert.fail("expected the URL to be refused");
        } catch (IllegalArgumentException expected) {
            Assert.assertEquals("'Schema Registry URL' (schemaRegistryUrl) must be an http or https URL, got: "
                    + "'https://sr.example.com:80a81'. Its host or port cannot be read.", expected.getMessage());
        }
    }

    private static CpiKafkaPlusEndpoint endpoint() {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setSchemaRegistryUrl(URL_WITH_CREDENTIALS);
        endpoint.setSubjectNameStrategy("TopicNameStrategy");
        return endpoint;
    }

    private interface Action {
        void run() throws Exception;
    }

    private static String captureStdErr(Action action) throws Exception {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try {
            System.setErr(new PrintStream(captured, true, "UTF-8"));
            action.run();
        } finally {
            System.setErr(original);
        }
        return captured.toString("UTF-8");
    }
}
