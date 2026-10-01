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
