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
 * A Schema Registry credential alias that is set must resolve (#183): running without it turned every
 * registry call into a 401 that looked like a bad record.
 */
public class SchemaRegistryCredentialTest {

    private DefaultCamelContext ctx;

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        CredentialHelper.setCredentialResolver(null);
        ctx.close();
    }

    @Test
    public void anUnresolvableAliasFailsTheHelperInsteadOfRunningWithoutAuth() throws Exception {
        CredentialHelper.setCredentialResolver(alias -> null);
        CpiKafkaPlusEndpoint endpoint = endpoint("sr-cred");
        try {
            new AvroDeserializerHelper(endpoint);
            Assert.fail("expected the missing credential to fail the helper");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("sr-cred"));
        }
        try {
            new AvroSerializerHelper(endpoint);
            Assert.fail("expected the missing credential to fail the helper");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("sr-cred"));
        }
    }

    @Test
    public void noAliasMeansNoAuthentication() throws Exception {
        CredentialHelper.setCredentialResolver(alias -> {
            throw new AssertionError("must not be called without an alias");
        });
        new AvroDeserializerHelper(endpoint(""));
        new AvroSerializerHelper(endpoint(null));
    }

    @Test
    public void aResolvableAliasIsUsed() throws Exception {
        CredentialHelper.setCredentialResolver(alias -> new CredentialHelper.UserCredentials("u", "p"));
        new AvroDeserializerHelper(endpoint("sr-cred"));
    }

    /** A distinct URI per alias, so Camel's endpoint cache does not hand back a shared instance. */
    private CpiKafkaPlusEndpoint endpoint(String alias) throws Exception {
        CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:orders-" + (alias == null ? "none" : alias.isEmpty() ? "empty" : alias)
                + "?bootstrapServers=localhost:9092&securityProtocol=PLAINTEXT");
        endpoint.setSchemaRegistryEnabled(true);
        endpoint.setSchemaRegistryUrl("http://localhost:8081");
        endpoint.setSchemaRegistryCredentialAlias(alias);
        return endpoint;
    }
}
