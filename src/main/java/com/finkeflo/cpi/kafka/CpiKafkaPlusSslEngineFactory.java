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

import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLContext;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka SSL engine factory backed by SAP CPI's KeystoreService.
 * <p>
 * When configured, Kafka asks this factory to create client SSL engines. The
 * factory resolves an {@link SSLContext} from {@link CredentialHelper}, which
 * in turn reads the tenant keystore/truststore via CPI APIs instead of
 * requiring filesystem-based JKS/PKCS12 material.
 */
public class CpiKafkaPlusSslEngineFactory implements SslEngineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusSslEngineFactory.class);

    /**
     * Custom Kafka client property containing the CPI keystore alias to use for
     * optional client-key material. TrustManagers are sourced from the CPI
     * tenant keystore as a whole by {@link CredentialHelper#getSSLContext(String)}.
     */
    public static final String SSL_KEYSTORE_ALIAS_CONFIG = "cpi.kafka.ssl.keystore.alias";

    private static final Set<String> RECONFIGURABLE_CONFIGS;
    static {
        LinkedHashSet<String> configs = new LinkedHashSet<String>();
        configs.add(SSL_KEYSTORE_ALIAS_CONFIG);
        configs.add(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        configs.add(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        configs.add(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        RECONFIGURABLE_CONFIGS = Collections.unmodifiableSet(configs);
    }

    private SSLContext sslContext;
    private String keystoreAlias;
    private String configuredEndpointIdentificationAlgorithm;
    private String[] enabledProtocols;
    private String[] cipherSuites;

    @Override
    public void configure(Map<String, ?> configs) {
        keystoreAlias = trimToNull(asString(configs.get(SSL_KEYSTORE_ALIAS_CONFIG)));
        if (keystoreAlias == null) {
            throw new IllegalArgumentException(
                    "Kafka SSL engine factory requires config '" + SSL_KEYSTORE_ALIAS_CONFIG + "'");
        }

        sslContext = CredentialHelper.getSSLContext(keystoreAlias);
        if (sslContext == null) {
            throw new IllegalStateException(
                    "Could not resolve SSLContext from CPI Keystore alias '" + keystoreAlias + "'");
        }

        configuredEndpointIdentificationAlgorithm =
                trimToNull(asString(configs.get(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)));
        enabledProtocols = parseConfiguredArray(configs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG));
        cipherSuites = parseConfiguredArray(configs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));

        LOG.info("Configured CPI-backed Kafka SSL engine factory for keystore alias '{}'", keystoreAlias);
    }

    @Override
    public SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification) {
        SSLEngine engine = requireSslContext().createSSLEngine(peerHost, peerPort);
        engine.setUseClientMode(true);
        configureEngine(engine, endpointIdentification);
        return engine;
    }

    @Override
    public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
        SSLEngine engine = requireSslContext().createSSLEngine(peerHost, peerPort);
        engine.setUseClientMode(false);
        configureEngine(engine, null);
        return engine;
    }

    @Override
    public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
        return !Objects.equals(keystoreAlias,
                        trimToNull(asString(nextConfigs.get(SSL_KEYSTORE_ALIAS_CONFIG))))
                || !Arrays.equals(enabledProtocols,
                        parseConfiguredArray(nextConfigs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG)))
                || !Arrays.equals(cipherSuites,
                        parseConfiguredArray(nextConfigs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG)))
                || !Objects.equals(configuredEndpointIdentificationAlgorithm,
                        trimToNull(asString(
                                nextConfigs.get(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG))));
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    @Override
    public KeyStore keystore() {
        return null;
    }

    @Override
    public KeyStore truststore() {
        return null;
    }

    @Override
    public void close() {
        // no-op
    }

    private SSLContext requireSslContext() {
        if (sslContext == null) {
            throw new IllegalStateException("SSL engine factory not configured yet");
        }
        return sslContext;
    }

    private void configureEngine(SSLEngine engine, String endpointIdentification) {
        if (enabledProtocols != null && enabledProtocols.length > 0) {
            engine.setEnabledProtocols(enabledProtocols.clone());
        }
        if (cipherSuites != null && cipherSuites.length > 0) {
            engine.setEnabledCipherSuites(cipherSuites.clone());
        }

        String effectiveEndpointIdentification =
                endpointIdentification != null ? endpointIdentification : configuredEndpointIdentificationAlgorithm;
        if (effectiveEndpointIdentification != null) {
            SSLParameters sslParameters = engine.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm(effectiveEndpointIdentification);
            engine.setSSLParameters(sslParameters);
        }
    }

    private static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String[] parseConfiguredArray(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String[]) {
            String[] values = (String[]) value;
            return values.length == 0 ? null : values.clone();
        }

        java.util.ArrayList<String> items = new java.util.ArrayList<String>();
        if (value instanceof Iterable<?>) {
            for (Object item : (Iterable<?>) value) {
                String str = trimToNull(asString(item));
                if (str != null) {
                    items.add(str);
                }
            }
        } else {
            String raw = asString(value);
            if (raw != null) {
                for (String part : raw.split(",")) {
                    String str = trimToNull(part);
                    if (str != null) {
                        items.add(str);
                    }
                }
            }
        }
        return items.isEmpty() ? null : items.toArray(new String[0]);
    }
}
