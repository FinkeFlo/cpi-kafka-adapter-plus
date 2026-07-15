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

import java.util.Arrays;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sap.it.api.ITApiFactory;
import com.sap.it.api.keystore.KeystoreService;
import com.sap.it.api.securestore.SecureStoreService;
import com.sap.it.api.securestore.UserCredential;

/**
 * Helper to access CPI Secure Store and Keystore services.
 * Uses the official SAP CPI API (ITApiFactory / SecureStoreService / KeystoreService).
 *
 * @see <a href="https://help.sap.com/docs/cloud-integration/sap-cloud-integration/accessing-user-credentials">SAP Help: Accessing User Credentials</a>
 */
public final class CredentialHelper {

    private static final Logger LOG = LoggerFactory.getLogger(CredentialHelper.class);

    public static final class UserCredentials {
        private final String username;
        private final String password;
        public UserCredentials(String username, String password) {
            this.username = username;
            this.password = password;
        }
        public String username() { return username; }
        public String password() { return password; }
    }

    /**
     * Strategy for resolving username/password credentials by alias.
     * The default implementation reads from the CPI Secure Store via ITApiFactory;
     * alternative implementations (e.g. tests, or non-CPI runtimes) can be injected
     * via {@link #setCredentialResolver(CredentialResolver)}.
     */
    public interface CredentialResolver {
        UserCredentials resolveUserCredential(String alias);
    }

    private static final CredentialResolver DEFAULT_RESOLVER = new CredentialResolver() {
        public UserCredentials resolveUserCredential(String alias) {
            return resolveFromSecureStore(alias);
        }
    };
    private static volatile CredentialResolver credentialResolver = DEFAULT_RESOLVER;

    /**
     * Strategy for resolving SSL contexts from the CPI Keystore.
     * The default implementation reads from the CPI KeystoreService via ITApiFactory;
     * tests can inject an alternative resolver via {@link #setSslContextResolver(SslContextResolver)}.
     */
    public interface SslContextResolver {
        SSLContext resolveSslContext(String alias);
    }

    private static final SslContextResolver DEFAULT_SSL_CONTEXT_RESOLVER = new SslContextResolver() {
        public SSLContext resolveSslContext(String alias) {
            return resolveSslContextFromKeystore(alias);
        }
    };
    private static volatile SslContextResolver sslContextResolver = DEFAULT_SSL_CONTEXT_RESOLVER;

    private CredentialHelper() {}

    /**
     * Inject the credential resolver. Passing null restores the default CPI Secure Store resolver.
     */
    public static void setCredentialResolver(CredentialResolver resolver) {
        credentialResolver = (resolver != null) ? resolver : DEFAULT_RESOLVER;
    }

    /**
     * Inject the SSL context resolver. Passing null restores the default CPI Keystore resolver.
     */
    public static void setSslContextResolver(SslContextResolver resolver) {
        sslContextResolver = (resolver != null) ? resolver : DEFAULT_SSL_CONTEXT_RESOLVER;
    }

    /**
     * Configure Schema Registry basic-auth credentials on the given config map.
     * If the alias is null/empty or credential resolution fails, the config is left unchanged.
     */
    public static void configureSchemaRegistryAuth(Map<String, Object> config, String credentialAlias) {
        if (credentialAlias == null || credentialAlias.isEmpty()) {
            return;
        }
        try {
            UserCredentials cred = getUserCredential(credentialAlias);
            if (cred != null) {
                config.put("basic.auth.credentials.source", "USER_INFO");
                config.put("basic.auth.user.info", cred.username() + ":" + cred.password());
            }
        } catch (Exception e) {
            LOG.warn("Could not resolve Schema Registry credentials from alias '{}': {}",
                    credentialAlias, e.getMessage());
        }
    }

    /**
     * Retrieve username/password from CPI Secure Store by alias.
     */
    public static UserCredentials getUserCredential(String alias) {
        return credentialResolver.resolveUserCredential(alias);
    }

    private static UserCredentials resolveFromSecureStore(String alias) {
        try {
            SecureStoreService secureStoreService = ITApiFactory.getService(SecureStoreService.class, null);

            if (secureStoreService == null) {
                LOG.warn("SecureStoreService not available - not running on CPI?");
                return null;
            }

            UserCredential userCredential = secureStoreService.getUserCredential(alias);

            if (userCredential == null) {
                LOG.warn("No credential found for alias '{}'", alias);
                return null;
            }

            String username = userCredential.getUsername();
            String password = toStringAndZero(userCredential.getPassword());

            LOG.debug("Successfully resolved credentials for alias '{}'", alias);
            return new UserCredentials(username, password);
        } catch (Exception e) {
            LOG.error("Failed to retrieve credentials for alias '{}': {}", alias, e.getMessage(), e);
            throw new RuntimeException("Failed to retrieve credentials: " + e.getMessage(), e);
        }
    }

    /**
     * Copies the given password characters into a String and immediately zeroes out the
     * original array. Reduces the time window during which the plaintext password sits in
     * JVM heap memory as a mutable {@code char[]} that could otherwise be overwritten with
     * dummy data (unlike a {@code String}, which is immutable and stays put until GC).
     *
     * <p>Package-private (not private) so it can be unit-tested directly without needing to
     * mock the SAP CPI {@code SecureStoreService}.</p>
     *
     * @param passwordChars the password characters as returned by {@code UserCredential.getPassword()};
     *                      may be {@code null}. The array is mutated (zeroed) as a side effect.
     * @return a new String containing the password, or {@code null} if {@code passwordChars} is {@code null}
     */
    static String toStringAndZero(char[] passwordChars) {
        if (passwordChars == null) {
            return null;
        }
        try {
            return new String(passwordChars);
        } finally {
            Arrays.fill(passwordChars, ' ');
        }
    }

    /**
     * Get an SSLContext configured with KeyManager and TrustManager from CPI Keystore.
     */
    public static SSLContext getSSLContext(String keystoreAlias) {
        return sslContextResolver.resolveSslContext(keystoreAlias);
    }

    private static SSLContext resolveSslContextFromKeystore(String keystoreAlias) {
        try {
            KeystoreService keystoreService = ITApiFactory.getService(KeystoreService.class, null);

            if (keystoreService == null) {
                LOG.warn("KeystoreService not available - not running on CPI?");
                return null;
            }

            KeyManager[] keyManagers = keystoreService.getKeyManagers(keystoreAlias);
            TrustManager[] trustManagers = keystoreService.getTrustManagers();

            SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
            sslContext.init(keyManagers, trustManagers, null);

            LOG.debug("SSLContext created for keystore alias '{}'", keystoreAlias);
            return sslContext;
        } catch (Exception e) {
            LOG.error("Failed to get SSLContext for alias '{}': {}", keystoreAlias, e.getMessage(), e);
            throw new RuntimeException("Failed to get SSLContext: " + e.getMessage(), e);
        }
    }
}
