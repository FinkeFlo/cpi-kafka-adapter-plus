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

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class SecurityConfigHelperTest {

    private static final String SOURCE = "'Credential Alias' (credentialAlias) 'kafka-user'";

    @After
    public void resetResolvers() {
        CredentialHelper.setCredentialResolver(null);
        CredentialHelper.setSslContextResolver(null);
    }

    /**
     * A line break ends a quoted JAAS value, and Kafka quotes the rest of the password in its error
     * (#245). CR and LF are refused, in the user name as in the password, and the message names where
     * the credential comes from but no part of it.
     */
    @Test
    public void lineBreaksInACredentialAreRejectedWithoutTheValue() {
        for (String lineBreak : new String[] {"\n", "\r", "\r\n"}) {
            String value = "QuuxHeadPart" + lineBreak + "QuuxTailPart";
            assertUnusable("password", () -> SecurityConfigHelper.buildJaasConfig("PLAIN", "user", value, SOURCE));
            assertUnusable("user name", () -> SecurityConfigHelper.buildJaasConfig("SCRAM-SHA-512", value, "pass",
                    SOURCE));
        }
    }

    /**
     * Kafka's JAAS parser keeps every other character of a quoted value, so a credential with a tab or
     * another control character logged in up to 1.4.1 and must still reach Kafka unchanged.
     */
    @Test
    public void otherControlCharactersReachKafkaUnchanged() {
        for (String control : new String[] {"\t", "\u0000", "\u0001", "\u000b", "\f", "\u001b", "\u007f",
                "\u0085", "\u2028"}) {
            String user = "us" + control + "er";
            String password = "pa" + control + "ss \\ \"quoted\"";
            String jaas = SecurityConfigHelper.buildJaasConfig("SCRAM-SHA-256", user, password, SOURCE);

            Map<String, Object> configs = new HashMap<>();
            configs.put(SaslConfigs.SASL_JAAS_CONFIG, new Password(jaas));
            Map<String, ?> options = JaasContext.loadClientContext(configs).configurationEntries().get(0)
                    .getOptions();
            Assert.assertEquals(user, options.get("username"));
            Assert.assertEquals(password, options.get("password"));
        }
    }

    @Test
    public void printableCharactersAreStillAccepted() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user", "päss w€rd ~!", SOURCE);
        Assert.assertTrue(result, result.endsWith("password=\"päss w€rd ~!\";"));
    }

    @Test
    public void anUnusableCredentialFailsTheClientBuildInsteadOfBeingLogged() {
        CredentialHelper.setCredentialResolver(alias ->
                new CredentialHelper.UserCredentials("alice", "QuuxHeadPart\nQuuxTailPart"));
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setSecurityProtocol("SASL_SSL");
        endpoint.setCredentialAlias("kafka-user");
        Properties props = new Properties();

        assertUnusable("password", () -> SecurityConfigHelper.configureSecurityProperties(props, endpoint));
        Assert.assertNull(props.getProperty("sasl.jaas.config"));
    }

    @Test
    public void anUnusableDlqCredentialNamesTheDlqAlias() {
        CredentialHelper.setCredentialResolver(alias ->
                new CredentialHelper.UserCredentials("alice", "QuuxHeadPart\nQuuxTailPart"));
        Properties props = new Properties();

        try {
            SecurityConfigHelper.overrideSaslCredentials(props, "PLAIN", "dlq-user");
            Assert.fail("expected the credential to be rejected");
        } catch (SecurityConfigHelper.UnusableCredentialException expected) {
            Assert.assertTrue(expected.getMessage(), expected.getMessage().startsWith("The password stored under "
                    + "'DLQ SASL Credential Alias' (dlqCredentialAlias) 'dlq-user' contains a line break"));
            Assert.assertFalse(expected.getMessage(), expected.getMessage().contains("Quux"));
        }
        Assert.assertNull(props.getProperty("sasl.jaas.config"));
    }

    private static void assertUnusable(String what, Runnable build) {
        try {
            build.run();
            Assert.fail("expected the " + what + " to be rejected");
        } catch (SecurityConfigHelper.UnusableCredentialException expected) {
            Assert.assertEquals("The " + what + " stored under " + SOURCE + " contains a line break, which the "
                    + "Kafka client's JAAS configuration cannot take. Please store the credential without it.",
                    expected.getMessage());
        }
    }

    @Test
    public void testBuildJaasConfigPlain() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user", "pass", SOURCE);
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";",
                result);
    }

    @Test
    public void testBuildJaasConfigScramSha256() {
        String result = SecurityConfigHelper.buildJaasConfig("SCRAM-SHA-256", "user", "pass", SOURCE);
        Assert.assertTrue(result.startsWith("org.apache.kafka.common.security.scram.ScramLoginModule"));
    }

    @Test
    public void testBuildJaasConfigScramSha512() {
        String result = SecurityConfigHelper.buildJaasConfig("SCRAM-SHA-512", "user", "pass", SOURCE);
        Assert.assertTrue(result.startsWith("org.apache.kafka.common.security.scram.ScramLoginModule"));
    }

    @Test
    public void testBuildJaasConfigEscapesDoubleQuotes() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user", "pass\"word", SOURCE);
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\\\"word\";",
                result);
    }

    @Test
    public void testBuildJaasConfigEscapesBackslashes() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user", "pass\\word", SOURCE);
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\\\\word\";",
                result);
    }

    @Test
    public void testBuildJaasConfigEscapesBackslashBeforeQuote() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user", "pass\\\"end", SOURCE);
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\\\\\\\"end\";",
                result);
    }

    @Test
    public void testBuildJaasConfigEscapesUsernameSpecialChars() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user\"name", "pass", SOURCE);
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\\\"name\" password=\"pass\";",
                result);
    }

    @Test
    public void lowercaseSaslSslStillConfiguresSaslAndSsl() {
        CredentialHelper.setCredentialResolver(alias -> new CredentialHelper.UserCredentials("alice", "secret"));
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setSecurityProtocol(" sasl_ssl ");   // as an externalized parameter may deliver it
        endpoint.setSaslMechanism("scram-sha-512");
        endpoint.setCredentialAlias("kafka-user");
        endpoint.setSslKeystoreAlias("tenant-kafka");
        Properties props = new Properties();

        SecurityConfigHelper.configureSecurityProperties(props, endpoint);

        Assert.assertEquals("SASL_SSL", props.getProperty("security.protocol"));
        Assert.assertEquals("SCRAM-SHA-512", props.getProperty("sasl.mechanism"));
        Assert.assertNotNull("JAAS config is required to authenticate", props.getProperty("sasl.jaas.config"));
        Assert.assertEquals("tenant-kafka",
                props.getProperty(CpiKafkaPlusSslEngineFactory.SSL_KEYSTORE_ALIAS_CONFIG));
    }

    @Test
    public void lowercaseSslStillUsesTheKeystoreAlias() {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setSecurityProtocol("ssl");
        endpoint.setSslKeystoreAlias("tenant-kafka");
        Properties props = new Properties();

        SecurityConfigHelper.configureSecurityProperties(props, endpoint);

        Assert.assertEquals("SSL", props.getProperty("security.protocol"));
        Assert.assertEquals(CpiKafkaPlusSslEngineFactory.class.getName(),
                props.getProperty(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG));
    }

    @Test
    public void testConfigureSslWithoutKeystoreAliasUsesDefaultTruststoreBehavior() {
        Properties props = new Properties();
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setSecurityProtocol("SSL");
        endpoint.setSslKeystoreAlias("   ");

        SecurityConfigHelper.configureSecurityProperties(props, endpoint);

        Assert.assertNull("ssl.protocol should not be set explicitly; Kafka 4.x defaults to TLSv1.3",
                props.getProperty(SslConfigs.SSL_PROTOCOL_CONFIG));
        Assert.assertNull(props.getProperty(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG));
        Assert.assertNull(props.getProperty(CpiKafkaPlusSslEngineFactory.SSL_KEYSTORE_ALIAS_CONFIG));
    }

    @Test
    public void testConfigureSslWithKeystoreAliasRegistersCustomSslEngineFactory() {
        Properties props = new Properties();
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setSecurityProtocol("SSL");
        endpoint.setSslKeystoreAlias("tenant-kafka");

        SecurityConfigHelper.configureSecurityProperties(props, endpoint);

        Assert.assertNull("ssl.protocol should not be set explicitly; Kafka 4.x defaults to TLSv1.3",
                props.getProperty(SslConfigs.SSL_PROTOCOL_CONFIG));
        Assert.assertEquals(CpiKafkaPlusSslEngineFactory.class.getName(),
                props.getProperty(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG));
        Assert.assertEquals("tenant-kafka",
                props.getProperty(CpiKafkaPlusSslEngineFactory.SSL_KEYSTORE_ALIAS_CONFIG));
    }

    @Test
    public void testCustomSslEngineFactoryBuildsClientEngineFromInjectedSslContext() throws Exception {
        final AtomicReference<String> resolvedAlias = new AtomicReference<String>();
        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, null, new SecureRandom());

        CredentialHelper.setSslContextResolver(new CredentialHelper.SslContextResolver() {
            public SSLContext resolveSslContext(String alias) {
                resolvedAlias.set(alias);
                return sslContext;
            }
        });

        CpiKafkaPlusSslEngineFactory factory = new CpiKafkaPlusSslEngineFactory();
        Map<String, Object> configs = new HashMap<String, Object>();
        configs.put(CpiKafkaPlusSslEngineFactory.SSL_KEYSTORE_ALIAS_CONFIG, "tenant-kafka");
        configs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        configs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.3");

        factory.configure(configs);
        SSLEngine engine = factory.createClientSslEngine("broker.example.com", 9093, null);

        Assert.assertEquals("tenant-kafka", resolvedAlias.get());
        Assert.assertTrue(engine.getUseClientMode());
        Assert.assertArrayEquals(new String[] { "TLSv1.3" }, engine.getEnabledProtocols());
        Assert.assertEquals("HTTPS", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
        Assert.assertFalse(factory.shouldBeRebuilt(configs));

        Map<String, Object> changedConfigs = new HashMap<String, Object>(configs);
        changedConfigs.put(CpiKafkaPlusSslEngineFactory.SSL_KEYSTORE_ALIAS_CONFIG, "other-alias");
        Assert.assertTrue(factory.shouldBeRebuilt(changedConfigs));
    }
}
