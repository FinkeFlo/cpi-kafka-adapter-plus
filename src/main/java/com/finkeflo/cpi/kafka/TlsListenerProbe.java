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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;

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
 * <p>The probe therefore opens a plain TCP connection and offers a TLS client hello. It blocks only
 * on a definitive answer:
 * <ul>
 *   <li>the peer answers with a TLS handshake or alert record — either way the listener speaks
 *       TLS;</li>
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

    private static final int TLS_RECORD_HANDSHAKE = 0x16;
    private static final int TLS_RECORD_ALERT = 0x15;

    private static final ConcurrentMap<ProbeCacheKey, ProbeResult> CACHE =
            new ConcurrentHashMap<>();

    private static volatile ProbeRunner probeRunner = new ProbeRunner() {
        @Override
        public Verdict probe(String address) {
            return TlsListenerProbe.probe(address);
        }
    };

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
        ProbeResult result = CACHE.computeIfAbsent(new ProbeCacheKey(bootstrapServers, securityProtocol),
                new java.util.function.Function<ProbeCacheKey, ProbeResult>() {
                    @Override
                    public ProbeResult apply(ProbeCacheKey key) {
                        return probeBootstrapServers(key.bootstrapServers);
                    }
                });
        if (result.verdict == Verdict.TLS) {
            throw new IllegalStateException(
                    "Security Protocol '" + securityProtocol + "' sends unencrypted traffic, but "
                    + "broker " + result.address + " requires TLS. Use SASL_SSL (or SSL) instead.");
        }
    }

    private static ProbeResult probeBootstrapServers(String bootstrapServers) {
        for (String server : bootstrapServers.split(",")) {
            String address = server.trim();
            if (address.isEmpty()) {
                continue;
            }
            if (probeRunner.probe(address) == Verdict.TLS) {
                return new ProbeResult(Verdict.TLS, address);
            }
        }
        return new ProbeResult(Verdict.INCONCLUSIVE, null);
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

            sendClientHello(plain, host, port);
            int firstByte = plain.getInputStream().read();
            if (firstByte == TLS_RECORD_HANDSHAKE || firstByte == TLS_RECORD_ALERT) {
                LOG.warn("[CPI-KAFKA-PLUS-DIAG] {}:{} answered the TLS probe with record type 0x{} "
                        + "— the listener requires TLS",
                        host, port, Integer.toHexString(firstByte));
                return Verdict.TLS;
            }
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] TLS probe of {}:{} found no TLS server (first byte {}) "
                    + "— inconclusive", host, port, firstByte);
            return Verdict.INCONCLUSIVE;
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

    private static void sendClientHello(Socket plain, String host, int port) throws IOException {
        SSLEngine engine;
        try {
            engine = SSLContext.getDefault().createSSLEngine(host, port);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IOException("Default TLS context is unavailable", e);
        }
        engine.setUseClientMode(true);
        engine.beginHandshake();

        SSLSession session = engine.getSession();
        ByteBuffer empty = ByteBuffer.allocate(0);
        ByteBuffer outbound = ByteBuffer.allocate(session.getPacketBufferSize());
        javax.net.ssl.SSLEngineResult result = engine.wrap(empty, outbound);
        if (result.bytesProduced() <= 0) {
            throw new IOException("TLS engine did not produce a client hello (status "
                    + result.getHandshakeStatus() + ")");
        }
        outbound.flip();

        OutputStream out = plain.getOutputStream();
        byte[] bytes = new byte[outbound.remaining()];
        outbound.get(bytes);
        out.write(bytes);
        out.flush();
    }

    static boolean isPlaintext(String securityProtocol) {
        return securityProtocol == null
                || !securityProtocol.toUpperCase(Locale.ROOT).contains("SSL");
    }

    /** Visible for testing. */
    static void setProbeRunnerForTests(ProbeRunner runner) {
        probeRunner = runner;
        CACHE.clear();
    }

    /** Visible for testing. */
    static void clearCacheForTests() {
        CACHE.clear();
        probeRunner = new ProbeRunner() {
            @Override
            public Verdict probe(String address) {
                return TlsListenerProbe.probe(address);
            }
        };
    }

    interface ProbeRunner {
        Verdict probe(String address);
    }

    private static final class ProbeCacheKey {
        private final String bootstrapServers;
        private final String securityProtocol;

        ProbeCacheKey(String bootstrapServers, String securityProtocol) {
            this.bootstrapServers = bootstrapServers;
            this.securityProtocol = securityProtocol;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ProbeCacheKey)) {
                return false;
            }
            ProbeCacheKey that = (ProbeCacheKey) other;
            return this.bootstrapServers.equals(that.bootstrapServers)
                    && String.valueOf(this.securityProtocol).equals(String.valueOf(that.securityProtocol));
        }

        @Override
        public int hashCode() {
            int result = bootstrapServers.hashCode();
            result = 31 * result + String.valueOf(securityProtocol).hashCode();
            return result;
        }
    }

    private static final class ProbeResult {
        private final Verdict verdict;
        private final String address;

        ProbeResult(Verdict verdict, String address) {
            this.verdict = verdict;
            this.address = address;
        }
    }
}
