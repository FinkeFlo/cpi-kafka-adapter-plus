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

import java.util.Properties;

import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared security configuration for Kafka Consumer and Producer.
 * Configures SASL authentication and SSL/TLS from CPI Secure Store and Keystore.
 */
public final class SecurityConfigHelper {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityConfigHelper.class);
    private static final String DEFAULT_SSL_PROTOCOL = "TLSv1.3";

    private SecurityConfigHelper() {}

    /**
     * Configure SASL and SSL properties on the given Kafka Properties based on endpoint settings.
     */
    public static void configureSecurityProperties(Properties props, CpiKafkaPlusEndpoint endpoint) {
        String securityProtocol = endpoint.getSecurityProtocol();
        if (securityProtocol == null || securityProtocol.isEmpty()) {
            return;
        }
        props.put("security.protocol", securityProtocol);

        if (securityProtocol.contains("SASL")) {
            configureSasl(props, endpoint);
        }

        if (securityProtocol.contains("SSL")) {
            configureSsl(props, endpoint);
        }
    }

    private static void configureSasl(Properties props, CpiKafkaPlusEndpoint endpoint) {
        props.put("sasl.mechanism", endpoint.getSaslMechanism());

        String alias = endpoint.getCredentialAlias();
        if (alias == null || alias.isEmpty()) {
            LOG.error("SASL configured but no credential alias specified. Kafka will fail to connect.");
            return;
        }

        try {
            CredentialHelper.UserCredentials credentials = CredentialHelper.getUserCredential(alias);
            if (credentials != null) {
                String jaasConfig = buildJaasConfig(
                        endpoint.getSaslMechanism(), credentials.username(), credentials.password());
                props.put("sasl.jaas.config", jaasConfig);
                LOG.info("SASL credentials resolved for alias '{}'", alias);
            } else {
                LOG.error("SASL configured but no credentials resolved for alias '{}'. " +
                        "Kafka will fail to connect.", alias);
            }
        } catch (Exception e) {
            LOG.error("Could not resolve SASL credentials from Secure Store alias '{}': {}. " +
                    "Kafka client will likely fail to connect.", alias, e.getMessage());
        }
    }

    private static void configureSsl(Properties props, CpiKafkaPlusEndpoint endpoint) {
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, DEFAULT_SSL_PROTOCOL);

        String sslKeystoreAlias = trimToNull(endpoint.getSslKeystoreAlias());
        if (sslKeystoreAlias == null) {
            return;
        }

        props.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, CpiKafkaPlusSslEngineFactory.class.getName());
        props.put(CpiKafkaPlusSslEngineFactory.SSL_KEYSTORE_ALIAS_CONFIG, sslKeystoreAlias);
        LOG.info("Kafka SSL will use CPI keystore alias '{}' for trust/key material", sslKeystoreAlias);
    }

    static String buildJaasConfig(String mechanism, String username, String password) {
        String loginModule;
        String mech = mechanism.toUpperCase();
        if ("SCRAM-SHA-256".equals(mech) || "SCRAM-SHA-512".equals(mech)) {
            loginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
        } else {
            loginModule = "org.apache.kafka.common.security.plain.PlainLoginModule";
        }
        String safeUser = username.replace("\\", "\\\\").replace("\"", "\\\"");
        String safePass = password.replace("\\", "\\\\").replace("\"", "\\\"");
        return String.format("%s required username=\"%s\" password=\"%s\";", loginModule, safeUser, safePass);
    }

    /**
     * Override SASL credentials with a different credential alias.
     * Used by DLQ producer when the DLQ cluster requires separate credentials.
     */
    public static void overrideSaslCredentials(Properties props, String mechanism, String alias) {
        try {
            CredentialHelper.UserCredentials credentials = CredentialHelper.getUserCredential(alias);
            if (credentials != null) {
                String jaasConfig = buildJaasConfig(mechanism, credentials.username(), credentials.password());
                props.put("sasl.jaas.config", jaasConfig);
                LOG.info("DLQ SASL credentials resolved for alias '{}'", alias);
            } else {
                LOG.error("DLQ credential alias '{}' configured but no credentials resolved. " +
                        "DLQ producer will fail to connect.", alias);
            }
        } catch (Exception e) {
            LOG.warn("Could not resolve DLQ credentials from Secure Store alias '{}': {}",
                    alias, e.getMessage());
        }
    }

    /**
     * Returns a string representation of Kafka properties with sensitive values redacted.
     * Use this instead of props.toString() to prevent credential leaks in logs.
     */
    public static String safePropertiesToString(Properties props) {
        Properties safe = new Properties();
        safe.putAll(props);
        if (safe.containsKey("sasl.jaas.config")) {
            safe.put("sasl.jaas.config", "***REDACTED***");
        }
        if (safe.containsKey("ssl.context")) {
            safe.put("ssl.context", "***SSLContext***");
        }
        return safe.toString();
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
