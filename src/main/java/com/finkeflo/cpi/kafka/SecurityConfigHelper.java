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

    /**
     * What an empty security protocol means: nothing is configured, so Kafka's default applies. Both
     * validators log it at start with this reason (#261).
     */
    static final String EMPTY_PROTOCOL = "empty; the Kafka client connects with PLAINTEXT, without TLS "
            + "and without authentication";

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
                        credentials.password(), UiLabels.field("credentialAlias") + " '" + alias + "'");
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
     * @throws UnusableCredentialException if the user name or the password contains a line break
     */
    static String buildJaasConfig(String mechanism, String username, String password, String source) {
        // Kafka reads the configuration with a StreamTokenizer: a quoted value ends at a line break,
        // and the rest of the password becomes a key that Kafka's error message then quotes. So the
        // message names where the credential comes from, never the value or a part of it.
        rejectLineBreaks(username, "user name", source);
        rejectLineBreaks(password, "password", source);
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
     * Only CR and LF end a quoted value. The tokenizer keeps every other character, a tab or another
     * control character included, so such a credential logs in as before and is not refused.
     */
    private static void rejectLineBreaks(String value, String what, String source) {
        if (value != null && (value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0)) {
            throw new UnusableCredentialException("The " + what + " stored under " + source
                    + " contains a line break, which the Kafka client's JAAS configuration cannot take. "
                    + "Please store the credential without it.");
        }
    }

    /**
     * Resolves every alias the client will read, so that one which is not deployed fails the start
     * instead of the first connect or send (#307). It needs no network. An alias fails the start
     * only if the secure store or the keystore answers that it does not exist; a store that cannot
     * be asked, as outside CPI or during a restart, is logged and left to the client build as before.
     *
     * @param registryUsed whether a Schema Registry client is built: the registry is on and values
     *                     are (de)serialized through it
     */
    static void requireAliases(CpiKafkaPlusEndpoint endpoint, boolean registryUsed) {
        String protocol = endpoint.getSecurityProtocol() != null ? endpoint.getSecurityProtocol().toUpperCase() : "";
        if (protocol.contains("SASL")) {
            String alias = trimToNull(endpoint.getCredentialAlias());
            String parameter = UiLabels.field("credentialAlias");
            unlessTheStoreFails(parameter, alias, () -> {
                CredentialHelper.UserCredentials credentials = CredentialHelper.requireUserCredential(alias,
                        parameter, null);
                if (credentials != null) {
                    buildJaasConfig(endpoint.getSaslMechanism(), credentials.username(), credentials.password(),
                            parameter + " '" + alias + "'");
                }
            });
            String dlqAlias = trimToNull(endpoint.getDlqCredentialAlias());
            if (endpoint.isDlqEnabled() && dlqAlias != null) {
                unlessTheStoreFails(UiLabels.field("dlqCredentialAlias"), dlqAlias,
                        () -> overrideSaslCredentials(new Properties(), endpoint.getSaslMechanism(), dlqAlias));
            }
        }
        String keystoreAlias = trimToNull(endpoint.getSslKeystoreAlias());
        if (protocol.contains("SSL") && keystoreAlias != null) {
            String parameter = UiLabels.field("sslKeystoreAlias");
            unlessTheStoreFails(parameter, keystoreAlias, () -> {
                try {
                    CredentialHelper.getSSLContext(keystoreAlias);
                } catch (RuntimeException e) {
                    if (!isKeystoreAliasFailure(e)) {
                        throw e;
                    }
                    throw new IllegalStateException(parameter + " '" + keystoreAlias + "' could not be resolved "
                            + "from the CPI keystore: " + e.getMessage() + ". Please deploy a key pair with this "
                            + "alias, or clear the field to use TLS without a client certificate.", e);
                }
            });
        }
        String registryAlias = trimToNull(endpoint.getSchemaRegistryCredentialAlias());
        if (registryUsed && registryAlias != null) {
            String parameter = UiLabels.field("schemaRegistryCredentialAlias");
            unlessTheStoreFails(parameter, registryAlias,
                    () -> CredentialHelper.requireUserCredential(registryAlias, parameter));
        }
    }

    private static boolean isKeystoreAliasFailure(Throwable failure) {
        for (Throwable cause : CauseChain.of(failure)) {
            if (cause instanceof CredentialHelper.KeystoreAliasException) {
                return true;
            }
        }
        return false;
    }

    /**
     * Runs {@code check}, which throws an {@link IllegalStateException} for an alias the store does
     * not know and an {@link UnusableCredentialException} for a credential Kafka cannot take. Any other
     * failure means the store could not be asked: that says nothing about the alias.
     */
    private static void unlessTheStoreFails(String parameter, String alias, Runnable check) {
        try {
            check.run();
        } catch (IllegalStateException | UnusableCredentialException e) {
            throw e;
        } catch (RuntimeException e) {
            // ERROR, so that it reaches the tenant trace: a skipped check must not look like a passed one.
            AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("alias.check.skipped")
                    .with("parameter", parameter)
                    .with("alias", alias)
                    .with("reason", e.getMessage())
                    .with("consequence", "the client reports the alias when it is built"), null);
        }
    }

    /**
     * Override SASL credentials with a different credential alias.
     * Used by DLQ producer when the DLQ cluster requires separate credentials.
     */
    public static void overrideSaslCredentials(Properties props, String mechanism, String alias) {
        // An alias that cannot be resolved fails the DLQ producer. Falling back to the main
        // credentials wrote the DLQ with the consumer's principal (#306).
        String parameter = UiLabels.field("dlqCredentialAlias");
        CredentialHelper.UserCredentials credentials = CredentialHelper.requireUserCredential(alias, parameter,
                "use the SASL credentials of the Connection tab");
        if (credentials != null) {
            props.put("sasl.jaas.config", buildJaasConfig(mechanism, credentials.username(),
                    credentials.password(), parameter + " '" + alias + "'"));
            LOG.info("DLQ SASL credentials resolved for alias '{}'", alias);
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
