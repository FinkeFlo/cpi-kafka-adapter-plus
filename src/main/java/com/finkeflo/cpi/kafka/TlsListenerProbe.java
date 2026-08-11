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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.util.Locale;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Refuses to start a Kafka client that would talk plaintext to a TLS-only broker.
 *
 * <p>This mismatch cannot be left to the Kafka client, because it does not fail — it takes the
 * whole node down. A broker whose listener requires TLS answers a plaintext Kafka request with a
 * TLS alert record, which begins with the bytes {@code 15 03 03 00 02}. Kafka reads the first four
 * bytes of every response as the frame length, so it sees {@code 0x15030300} and allocates a buffer
 * of 352,321,280 bytes — and it allocates one per connection attempt, because the client keeps
 * reconnecting. On a CPI node with a ~2 GB heap this exhausts the heap within seconds and Cloud
 * Foundry's {@code jvmkill} agent kills the JVM, which surfaces in the MPL as {@code Node Crashed}.
 *
 * <p>No timeout or exception handler inside the adapter can prevent that: the process is killed
 * from the outside, and the allocation happens in Kafka's own network thread before any adapter
 * code runs. The size is not configurable either — the Kafka clients construct their {@code Selector}
 * with an unlimited receive size. The only reliable defence is to detect the TLS listener before a
 * Kafka client is created at all.
 *
 * <p>The probe therefore opens a plain TCP connection and offers a TLS handshake. It blocks only on
 * a definitive answer:
 * <ul>
 *   <li>the handshake completes, or fails because the certificate is not trusted — either way the
 *       peer sent a TLS server response, so the listener speaks TLS;</li>
 *   <li>anything else — a plaintext listener, a connection error, a timeout — is inconclusive and
 *       lets the connection proceed unchanged.</li>
 * </ul>
 *
 * <p>A false positive would require a TLS server answering on the configured broker port while the
 * configuration says plaintext, which is exactly the condition being detected.
 */
final class TlsListenerProbe {

    private static final Logger LOG = LoggerFactory.getLogger(TlsListenerProbe.class);

    static final int CONNECT_TIMEOUT_MS = 3_000;
    static final int HANDSHAKE_TIMEOUT_MS = 3_000;

    private TlsListenerProbe() {
    }

    /** What a single bootstrap server told us about its listener. */
    enum Verdict {
        /** The peer completed or attempted a TLS server handshake. */
        TLS,
        /** No evidence of TLS, or the peer could not be reached at all. */
        INCONCLUSIVE
    }

    /**
     * @throws IllegalStateException if a configured bootstrap server is proven to require TLS while
     *         {@code securityProtocol} sends unencrypted traffic
     */
    static void assertNoTlsListener(String bootstrapServers, String securityProtocol) {
        if (!isPlaintext(securityProtocol) || bootstrapServers == null) {
            return;
        }
        for (String server : bootstrapServers.split(",")) {
            String address = server.trim();
            if (address.isEmpty()) {
                continue;
            }
            if (probe(address) == Verdict.TLS) {
                throw new IllegalStateException(
                        "Security Protocol '" + securityProtocol + "' sends unencrypted traffic, but "
                        + "broker " + address + " requires TLS. Use SASL_SSL (or SSL) instead. "
                        + "Connecting anyway would make the Kafka client read the broker's TLS alert "
                        + "as a 352 MB message frame and exhaust the node's heap.");
            }
        }
    }

    /** Visible for testing. */
    static Verdict probe(String hostAndPort) {
        int lastColon = hostAndPort.lastIndexOf(':');
        if (lastColon <= 0 || lastColon == hostAndPort.length() - 1) {
            return Verdict.INCONCLUSIVE;
        }
        String host = hostAndPort.substring(0, lastColon).trim();
        int port;
        try {
            port = Integer.parseInt(hostAndPort.substring(lastColon + 1).trim());
        } catch (NumberFormatException e) {
            return Verdict.INCONCLUSIVE;
        }
        return probe(host, port);
    }

    /** Visible for testing. */
    static Verdict probe(String host, int port) {
        try (Socket plain = new Socket()) {
            plain.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            plain.setSoTimeout(HANDSHAKE_TIMEOUT_MS);

            // The socket is layered rather than created directly so that a broker which does not
            // speak TLS costs one TCP connection and nothing else.
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket tls = (SSLSocket) factory.createSocket(plain, host, port, false)) {
                tls.setUseClientMode(true);
                tls.setSoTimeout(HANDSHAKE_TIMEOUT_MS);
                tls.startHandshake();
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] {}:{} completed a TLS handshake — the listener requires TLS",
                        host, port);
                return Verdict.TLS;
            }
        } catch (SSLException e) {
            return classifyHandshakeFailure(host, port, e);
        } catch (IOException e) {
            // Unreachable, refused or timed out: says nothing about the protocol. Kafka's own
            // connection error is the better message here, so do not interfere.
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] TLS probe of {}:{} could not connect ({}) — inconclusive",
                    host, port, e.toString());
            return Verdict.INCONCLUSIVE;
        } catch (RuntimeException e) {
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] TLS probe of {}:{} failed unexpectedly ({}) — inconclusive",
                    host, port, e.toString());
            return Verdict.INCONCLUSIVE;
        }
    }

    private static Verdict classifyHandshakeFailure(String host, int port, SSLException e) {
        if (hasCertificateCause(e)) {
            // The peer sent a certificate we do not trust. Untrusted or not, only a TLS server
            // sends one, and that is the whole question here.
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] {}:{} presented a TLS certificate ({}) — the listener "
                    + "requires TLS", host, port, e.getMessage());
            return Verdict.TLS;
        }
        // "Unrecognized SSL message, plaintext connection?" and peers that simply drop a client
        // hello they cannot parse are what a plaintext listener looks like.
        LOG.debug("[CPI-KAFKA-PLUS-DIAG] TLS probe of {}:{} found no TLS server ({}) — inconclusive",
                host, port, e.toString());
        return Verdict.INCONCLUSIVE;
    }

    private static boolean hasCertificateCause(Throwable t) {
        for (Throwable current = t; current != null; current = current.getCause()) {
            if (current instanceof CertificateException) {
                return true;
            }
            if (current.getClass().getName().endsWith("ValidatorException")) {
                return true;
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return false;
    }

    static boolean isPlaintext(String securityProtocol) {
        return securityProtocol == null
                || !securityProtocol.toUpperCase(Locale.ROOT).contains("SSL");
    }
}
