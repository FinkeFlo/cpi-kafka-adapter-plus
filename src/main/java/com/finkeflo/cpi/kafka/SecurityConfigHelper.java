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
            LOG.error("[CPI-KAFKA-PLUS-DIAG] SASL configured but no credential alias specified. Kafka will fail to connect.");
            return;
        }

        try {
            CredentialHelper.UserCredentials credentials = CredentialHelper.getUserCredential(alias);
            if (credentials != null) {
                String jaasConfig = buildJaasConfig(endpoint.getSaslMechanism(), credentials.username(),
                        credentials.password(), "'Credential Alias' (credentialAlias) '" + alias + "'");
                props.put("sasl.jaas.config", jaasConfig);
                LOG.info("SASL credentials resolved for alias '{}'", alias);
            } else {
                LOG.error("[CPI-KAFKA-PLUS-DIAG] SASL configured but no credentials resolved for alias '{}'. " +
                        "Kafka will fail to connect.", alias);
            }
        } catch (UnusableCredentialException e) {
            // Fails the client build with a message that names the alias. Logged and dropped here,
            // the client would fail anyway, on the missing JAAS configuration instead.
            throw e;
        } catch (Exception e) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] Could not resolve SASL credentials from Secure Store alias '{}': {}. " +
                    "Kafka client will likely fail to connect.", alias, e.getMessage());
        }
    }

    private static void configureSsl(Properties props, CpiKafkaPlusEndpoint endpoint) {
        String sslKeystoreAlias = trimToNull(endpoint.getSslKeystoreAlias());
        if (sslKeystoreAlias == null) {
            return;
        }

        props.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, CpiKafkaPlusSslEngineFactory.class.getName());
        props.put(CpiKafkaPlusSslEngineFactory.SSL_KEYSTORE_ALIAS_CONFIG, sslKeystoreAlias);
        LOG.info("Kafka SSL will use CPI keystore alias '{}' for trust/key material", sslKeystoreAlias);
    }

    /**
     * The {@code sasl.jaas.config} of a SASL login with these credentials.
     *
     * @param source where the credentials come from, named in the message of a rejection, e.g.
     *        {@code 'Credential Alias' (credentialAlias) 'kafka-credentials'}
     * @throws UnusableCredentialException if the user name or the password contains a control
     *         character, CR and LF included
     */
    static String buildJaasConfig(String mechanism, String username, String password, String source) {
        // Kafka reads the configuration with a StreamTokenizer: a quoted value ends at a line break,
        // and the rest of the password becomes a key that Kafka's error message then quotes. So the
        // message names where the credential comes from, never the value or a part of it.
        rejectControlCharacters(username, "user name", source);
        rejectControlCharacters(password, "password", source);
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

    private static void rejectControlCharacters(String value, String what, String source) {
        if (value == null) {
            return;
        }
        for (int i = 0; i < value.length(); i++) {
            if (Character.isISOControl(value.charAt(i))) {
                throw new UnusableCredentialException("The " + what + " stored under " + source
                        + " contains a line break or another control character, which the Kafka client's "
                        + "JAAS configuration cannot take. Please store the credential without it.");
            }
        }
    }

    /**
     * Override SASL credentials with a different credential alias.
     * Used by DLQ producer when the DLQ cluster requires separate credentials.
     */
    public static void overrideSaslCredentials(Properties props, String mechanism, String alias) {
        try {
            CredentialHelper.UserCredentials credentials = CredentialHelper.getUserCredential(alias);
            if (credentials != null) {
                String jaasConfig = buildJaasConfig(mechanism, credentials.username(), credentials.password(),
                        "'DLQ SASL Credential Alias' (dlqCredentialAlias) '" + alias + "'");
                props.put("sasl.jaas.config", jaasConfig);
                LOG.info("DLQ SASL credentials resolved for alias '{}'", alias);
            } else {
                LOG.error("[CPI-KAFKA-PLUS-DIAG] DLQ credential alias '{}' configured but no credentials resolved. " +
                        "DLQ producer will fail to connect.", alias);
            }
        } catch (UnusableCredentialException e) {
            throw e;   // as in configureSasl
        } catch (Exception e) {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] Could not resolve DLQ credentials from Secure Store alias '{}': {}",
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

    /**
     * A credential that cannot be written into a JAAS configuration. Its message names the alias,
     * never the value.
     */
    static final class UnusableCredentialException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        UnusableCredentialException(String message) {
            super(message);
        }
    }
}
