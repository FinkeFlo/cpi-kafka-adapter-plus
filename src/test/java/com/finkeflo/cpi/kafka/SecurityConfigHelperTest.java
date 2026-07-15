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

import org.apache.kafka.common.config.SslConfigs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class SecurityConfigHelperTest {

    @After
    public void resetResolvers() {
        CredentialHelper.setCredentialResolver(null);
        CredentialHelper.setSslContextResolver(null);
    }

    @Test
    public void testBuildJaasConfigPlain() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user", "pass");
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";",
                result);
    }

    @Test
    public void testBuildJaasConfigScramSha256() {
        String result = SecurityConfigHelper.buildJaasConfig("SCRAM-SHA-256", "user", "pass");
        Assert.assertTrue(result.startsWith("org.apache.kafka.common.security.scram.ScramLoginModule"));
    }

    @Test
    public void testBuildJaasConfigScramSha512() {
        String result = SecurityConfigHelper.buildJaasConfig("SCRAM-SHA-512", "user", "pass");
        Assert.assertTrue(result.startsWith("org.apache.kafka.common.security.scram.ScramLoginModule"));
    }

    @Test
    public void testBuildJaasConfigEscapesDoubleQuotes() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user", "pass\"word");
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\\\"word\";",
                result);
    }

    @Test
    public void testBuildJaasConfigEscapesBackslashes() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user", "pass\\word");
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\\\\word\";",
                result);
    }

    @Test
    public void testBuildJaasConfigEscapesBackslashBeforeQuote() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user", "pass\\\"end");
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\\\\\\\"end\";",
                result);
    }

    @Test
    public void testBuildJaasConfigEscapesUsernameSpecialChars() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user\"name", "pass");
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\\\"name\" password=\"pass\";",
                result);
    }

    @Test
    public void testConfigureSslWithoutKeystoreAliasUsesDefaultTruststoreBehavior() {
        Properties props = new Properties();
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setSecurityProtocol("SSL");
        endpoint.setSslKeystoreAlias("   ");

        SecurityConfigHelper.configureSecurityProperties(props, endpoint);

        Assert.assertEquals("TLSv1.3", props.getProperty(SslConfigs.SSL_PROTOCOL_CONFIG));
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

        Assert.assertEquals("TLSv1.3", props.getProperty(SslConfigs.SSL_PROTOCOL_CONFIG));
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
