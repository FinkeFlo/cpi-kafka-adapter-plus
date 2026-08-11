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

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;

import org.junit.AfterClass;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Guards the only defence that works against the {@code Node Crashed} failure mode.
 *
 * <p>A plaintext Kafka client against a TLS-only broker reads the broker's TLS alert record as a
 * 352 MB frame length and allocates it per connection attempt until Cloud Foundry's {@code jvmkill}
 * agent kills the JVM. Nothing inside the adapter can catch that — the process is killed from the
 * outside — so the mismatch has to be detected before a Kafka client is created.
 *
 * <p>The tests run against real sockets, because the whole question is what an actual peer does
 * with an actual TLS client hello.
 */
public class TlsListenerProbeTest {

    private static final String STORE_PASSWORD = "changeit";

    private static Path keystoreDir;

    /** Generated with the JDK's own keytool so the test carries no key material in the repository. */
    @BeforeClass
    public static void createServerCertificate() throws Exception {
        Path parent = Paths.get("target", "test-work");
        Files.createDirectories(parent);
        keystoreDir = parent.resolve("tls-listener-probe-" + System.nanoTime());
        Files.createDirectory(keystoreDir);
        Process process = new ProcessBuilder(
                Paths.get(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair", "-alias", "probe", "-keyalg", "RSA", "-keysize", "2048",
                "-storetype", "PKCS12", "-keystore", "server.p12",
                "-storepass", STORE_PASSWORD, "-keypass", STORE_PASSWORD,
                "-dname", "CN=localhost", "-validity", "1")
                .directory(keystoreDir.toFile())
                .redirectErrorStream(true)
                .start();
        Assert.assertEquals("keytool failed", 0, process.waitFor());
    }

    @AfterClass
    public static void deleteServerCertificate() throws Exception {
        Files.deleteIfExists(keystoreDir.resolve("server.p12"));
        Files.deleteIfExists(keystoreDir);
    }

    @After
    public void clearProbeCache() {
        TlsListenerProbe.clearCacheForTests();
    }

    private static SSLContext serverContext() throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keystoreDir.resolve("server.p12"))) {
            keyStore.load(in, STORE_PASSWORD.toCharArray());
        }

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, STORE_PASSWORD.toCharArray());

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), null, new SecureRandom());
        return context;
    }

    @Test
    public void aTlsListenerIsDetectedEvenWhenItsCertificateIsNotTrusted() throws Exception {
        SSLServerSocket server = (SSLServerSocket) serverContext().getServerSocketFactory()
                .createServerSocket(0);
        Thread acceptor = acceptOnce(server, true);
        try {
            TlsListenerProbe.Verdict verdict =
                    TlsListenerProbe.probe("localhost", server.getLocalPort());

            Assert.assertEquals("A self-signed broker certificate still proves the listener is TLS",
                    TlsListenerProbe.Verdict.TLS, verdict);
        } finally {
            close(server, acceptor);
        }
    }

    @Test
    public void aPlaintextListenerIsLeftAlone() throws Exception {
        // Stands in for a plaintext Kafka broker: it reads the client hello as a Kafka request,
        // cannot make sense of it and drops the connection.
        ServerSocket server = new ServerSocket(0);
        Thread acceptor = acceptOnce(server, false);
        try {
            TlsListenerProbe.Verdict verdict =
                    TlsListenerProbe.probe("localhost", server.getLocalPort());

            Assert.assertEquals(TlsListenerProbe.Verdict.INCONCLUSIVE, verdict);
        } finally {
            close(server, acceptor);
        }
    }

    @Test
    public void fatalTlsAlertRecordProvesTheListenerIsTls() throws Exception {
        ServerSocket server = new ServerSocket(0);
        Thread acceptor = acceptAndReply(server, new byte[] {0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28});
        try {
            Assert.assertEquals(TlsListenerProbe.Verdict.TLS,
                    TlsListenerProbe.probe("localhost", server.getLocalPort()));
        } finally {
            close(server, acceptor);
        }
    }

    @Test
    public void serverHelloRecordProvesTheListenerIsTls() throws Exception {
        ServerSocket server = new ServerSocket(0);
        Thread acceptor = acceptAndReply(server, new byte[] {0x16, 0x03, 0x03, 0x00, 0x00});
        try {
            Assert.assertEquals(TlsListenerProbe.Verdict.TLS,
                    TlsListenerProbe.probe("localhost", server.getLocalPort()));
        } finally {
            close(server, acceptor);
        }
    }

    @Test
    public void peerThatClosesImmediatelyStaysInconclusive() throws Exception {
        ServerSocket server = new ServerSocket(0);
        Thread acceptor = acceptAndClose(server);
        try {
            Assert.assertEquals(TlsListenerProbe.Verdict.INCONCLUSIVE,
                    TlsListenerProbe.probe("localhost", server.getLocalPort()));
        } finally {
            close(server, acceptor);
        }
    }

    @Test
    public void peerThatStaysSilentStaysInconclusive() throws Exception {
        ServerSocket server = new ServerSocket(0);
        Thread acceptor = acceptAndStaySilent(server);
        try {
            Assert.assertEquals(TlsListenerProbe.Verdict.INCONCLUSIVE,
                    TlsListenerProbe.probe("localhost", server.getLocalPort()));
        } finally {
            close(server, acceptor);
        }
    }

    @Test
    public void plaintextKafkaLikePeerThatReadsAndDiscardsStaysInconclusive() throws Exception {
        ServerSocket server = new ServerSocket(0);
        Thread acceptor = acceptOnce(server, false);
        try {
            Assert.assertEquals(TlsListenerProbe.Verdict.INCONCLUSIVE,
                    TlsListenerProbe.probe("localhost", server.getLocalPort()));
        } finally {
            close(server, acceptor);
        }
    }

    @Test
    public void aPlaintextProtocolAgainstATlsBrokerIsRefusedBeforeAnyKafkaClientExists()
            throws Exception {
        SSLServerSocket server = (SSLServerSocket) serverContext().getServerSocketFactory()
                .createServerSocket(0);
        Thread acceptor = acceptOnce(server, true);
        try {
            String bootstrap = "localhost:" + server.getLocalPort();

            TlsListenerProbe.assertNoTlsListener(bootstrap, "SASL_PLAINTEXT");
            Assert.fail("Expected the TLS-only broker to be refused");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("SASL_PLAINTEXT"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("requires TLS"));
            Assert.assertTrue("The operator needs to be told what to change: " + e.getMessage(),
                    e.getMessage().contains("SASL_SSL"));
        } finally {
            close(server, acceptor);
        }
    }

    @Test
    public void aTlsProtocolSkipsTheProbeEntirely() throws Exception {
        // No server at all: reaching the network would fail the test by timing out on a closed
        // port, so this also asserts that the probe is not even attempted.
        long startMs = System.currentTimeMillis();

        TlsListenerProbe.assertNoTlsListener("broker.invalid:9093", "SASL_SSL");

        long elapsedMs = System.currentTimeMillis() - startMs;
        Assert.assertTrue("A TLS configuration must not pay for the probe, took " + elapsedMs + " ms",
                elapsedMs < TlsListenerProbe.CONNECT_TIMEOUT_MS);
    }

    @Test
    public void anUnreachableBrokerStaysInconclusiveSoKafkaReportsItInstead() {
        // Port 1 is reserved and closed: connection refused, which says nothing about TLS.
        Assert.assertEquals(TlsListenerProbe.Verdict.INCONCLUSIVE,
                TlsListenerProbe.probe("localhost", 1));

        TlsListenerProbe.assertNoTlsListener("localhost:1", "PLAINTEXT");
    }

    @Test
    public void malformedBootstrapEntriesAreNotTreatedAsAMismatch() {
        Assert.assertEquals(TlsListenerProbe.Verdict.INCONCLUSIVE,
                TlsListenerProbe.probe("no-port-here"));
        Assert.assertEquals(TlsListenerProbe.Verdict.INCONCLUSIVE,
                TlsListenerProbe.probe("host:not-a-number"));

        TlsListenerProbe.assertNoTlsListener(null, "PLAINTEXT");
    }

    @Test
    public void repeatedAssertionsUseTheCachedProbeVerdict() {
        final AtomicInteger calls = new AtomicInteger();
        TlsListenerProbe.setProbeRunnerForTests(new TlsListenerProbe.ProbeRunner() {
            @Override
            public TlsListenerProbe.Verdict probe(String address) {
                calls.incrementAndGet();
                return TlsListenerProbe.Verdict.INCONCLUSIVE;
            }
        });

        TlsListenerProbe.assertNoTlsListener("broker1:9092,broker2:9092", "PLAINTEXT");
        TlsListenerProbe.assertNoTlsListener("broker1:9092,broker2:9092", "PLAINTEXT");

        Assert.assertEquals("The bootstrap list should be probed once for the same config key",
                2, calls.get());
    }

    @Test
    public void cachedTlsVerdictStillThrowsOnEveryCall() {
        final AtomicInteger calls = new AtomicInteger();
        TlsListenerProbe.setProbeRunnerForTests(new TlsListenerProbe.ProbeRunner() {
            @Override
            public TlsListenerProbe.Verdict probe(String address) {
                calls.incrementAndGet();
                return TlsListenerProbe.Verdict.TLS;
            }
        });

        for (int i = 0; i < 2; i++) {
            try {
                TlsListenerProbe.assertNoTlsListener("broker1:9092", "SASL_PLAINTEXT");
                Assert.fail("Expected cached TLS mismatch to be raised");
            } catch (IllegalStateException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("requires TLS"));
            }
        }

        Assert.assertEquals(1, calls.get());
    }

    private static Thread acceptOnce(final ServerSocket server, final boolean speakTls) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try (Socket socket = server.accept()) {
                    if (speakTls) {
                        // Reading drives the handshake to completion.
                        socket.getInputStream().read();
                    } else {
                        // A plaintext Kafka broker rejects the client hello as an oversized
                        // request and closes the connection.
                        InputStream in = socket.getInputStream();
                        byte[] buffer = new byte[4];
                        in.read(buffer);
                        OutputStream out = socket.getOutputStream();
                        out.flush();
                    }
                } catch (Exception ignored) {
                    // The probe closing the socket mid-handshake is the expected outcome here.
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static Thread acceptAndReply(final ServerSocket server, final byte[] response) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try (Socket socket = server.accept()) {
                    socket.getInputStream().read(new byte[64]);
                    OutputStream out = socket.getOutputStream();
                    out.write(response);
                    out.flush();
                } catch (Exception ignored) {
                    // The probe closing the socket is expected.
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static Thread acceptAndClose(final ServerSocket server) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try (Socket ignored = server.accept()) {
                    // Close immediately.
                } catch (Exception ignored) {
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static Thread acceptAndStaySilent(final ServerSocket server) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try (Socket ignored = server.accept()) {
                    Thread.sleep(TlsListenerProbe.HANDSHAKE_TIMEOUT_MS + 500L);
                } catch (Exception ignored) {
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void close(ServerSocket server, Thread acceptor) throws Exception {
        server.close();
        acceptor.join(5_000);
    }
}
