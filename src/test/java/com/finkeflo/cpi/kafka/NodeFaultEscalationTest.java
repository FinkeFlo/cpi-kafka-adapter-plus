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

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.Test;

import com.finkeflo.cpi.kafka.KafkaErrorHelper.Classification;

/**
 * Unit tests for {@link NodeFaultEscalation} (e4b, c6, c6b): fault identity for causes that are not
 * an {@link Exception}, the classifications that are tracked, the window and the content and logger
 * category of the {@code producer.node.fault.escalation} diagnostic. No Camel context and no broker,
 * except for the one test that checks how {@link CpiKafkaPlusProducer} wires its endpoint in.
 */
public class NodeFaultEscalationTest {

    private static final String ESCALATION = "producer.node.fault.escalation";
    private static final long WINDOW_MS = 20 * 60 * 1000L;

    /**
     * Starts far from 0: the window start is initialised to 0, so the first fault only opens a
     * window when {@code now} is more than one window past it, as it always is with the wall clock.
     */
    private final AtomicLong clock = new AtomicLong(1_000_000_000_000L);

    private NodeFaultEscalation newEscalation(boolean diagnosticsFull) {
        return new NodeFaultEscalation(() -> "test-topic", () -> diagnosticsFull, () -> "client-1", clock::get);
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static String captureStderr(ThrowingRunnable action) throws Exception {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try {
            System.setErr(new PrintStream(captured, true, "UTF-8"));
            action.run();
        } finally {
            System.setErr(original);
        }
        return captured.toString("UTF-8");
    }

    private static long escalationLines(String logged) {
        return logged.lines().filter(l -> l.contains(ESCALATION)).count();
    }

    private static String escalationLine(String logged) {
        return logged.lines().filter(l -> l.contains(ESCALATION)).findFirst().orElse("");
    }

    private static void sameFault(NodeFaultEscalation escalation, int times) {
        for (int i = 0; i < times; i++) {
            escalation.checkNodeFaultEscalation(Classification.FATAL_PRODUCER_UNUSABLE,
                    CpiKafkaPlusErrorCode.KP_PROD_001, new RuntimeException("same"));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Fault identity for causes that are not an Exception
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    public void nullCauseHasNoIdentityCause() {
        assertNull(newEscalation(false).resolveNodeFaultIdentityCause(null));
    }

    @Test
    public void errorCauseResolvesToTheRootOfItsChain() {
        IOException root = new IOException("root");
        Error error = new Error("outer", new IllegalStateException("middle", root));
        assertSame(root, newEscalation(false).resolveNodeFaultIdentityCause(error));
    }

    @Test
    public void errorWithoutCauseResolvesToItself() {
        Error error = new OutOfMemoryError("no cause");
        assertSame(error, newEscalation(false).resolveNodeFaultIdentityCause(error));
    }

    @Test
    public void nullCauseEscalatesAsUnknownFaultClass() throws Exception {
        NodeFaultEscalation escalation = newEscalation(false);
        String logged = captureStderr(() -> {
            for (int i = 0; i < 5; i++) {
                escalation.checkNodeFaultEscalation(Classification.UNKNOWN_FATAL, CpiKafkaPlusErrorCode.KP_GEN_001, null);
            }
        });
        assertEquals(1, escalationLines(logged));
        assertTrue(logged, escalationLine(logged).contains("faultClass=unknown"));
    }

    @Test
    public void errorCauseEscalatesWithTheRootCauseClass() throws Exception {
        NodeFaultEscalation escalation = newEscalation(false);
        String logged = captureStderr(() -> {
            for (int i = 0; i < 5; i++) {
                escalation.checkNodeFaultEscalation(Classification.FATAL_PRODUCER_UNUSABLE,
                        CpiKafkaPlusErrorCode.KP_PROD_001,
                        new Error("wrapper " + i, new IllegalMonitorStateException("root")));
            }
        });
        assertEquals(1, escalationLines(logged));
        assertTrue(logged, escalationLine(logged).contains("faultClass=java.lang.IllegalMonitorStateException"));
    }

    @Test
    public void nullAndErrorCausesAreDifferentFaults() {
        NodeFaultEscalation escalation = newEscalation(false);
        for (int i = 0; i < 4; i++) {
            escalation.checkNodeFaultEscalation(Classification.UNKNOWN_FATAL, CpiKafkaPlusErrorCode.KP_GEN_001, null);
        }
        escalation.checkNodeFaultEscalation(Classification.UNKNOWN_FATAL, CpiKafkaPlusErrorCode.KP_GEN_001,
                new Error("other"));
        assertFalse(escalation.isEscalated());
        assertEquals(1, escalation.countInWindow());
        assertEquals("java.lang.Error:" + CpiKafkaPlusErrorCode.KP_GEN_001.code(), escalation.faultIdentity());
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Tracked classifications and window
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    public void retriableAndDataErrorsAreNotTracked() throws Exception {
        NodeFaultEscalation escalation = newEscalation(false);
        String logged = captureStderr(() -> {
            for (int i = 0; i < 10; i++) {
                escalation.checkNodeFaultEscalation(Classification.RETRIABLE, CpiKafkaPlusErrorCode.KP_PROD_001,
                        new RuntimeException("retriable"));
                escalation.checkNodeFaultEscalation(Classification.FATAL_DATA_ERROR, CpiKafkaPlusErrorCode.KP_PROD_001,
                        new RuntimeException("data"));
            }
        });
        assertEquals(0, escalationLines(logged));
        assertEquals(0, escalation.countInWindow());
        assertNull(escalation.faultIdentity());
    }

    @Test
    public void expiredWindowRestartsTheCount() {
        NodeFaultEscalation escalation = newEscalation(false);
        sameFault(escalation, 4);
        // Just over 20 minutes later.
        clock.addAndGet(WINDOW_MS + 1000L);

        sameFault(escalation, 1);
        assertEquals(1, escalation.countInWindow());
        assertFalse(escalation.isEscalated());
    }

    @Test
    public void windowIsStillOpenAtExactlyTwentyMinutes() {
        NodeFaultEscalation escalation = newEscalation(false);
        sameFault(escalation, 4);
        clock.addAndGet(WINDOW_MS);

        sameFault(escalation, 1);
        assertEquals(5, escalation.countInWindow());
        assertTrue(escalation.isEscalated());
    }

    @Test
    public void escalatesAgainInTheNextWindow() throws Exception {
        NodeFaultEscalation escalation = newEscalation(false);
        String logged = captureStderr(() -> {
            sameFault(escalation, 6);
            clock.addAndGet(WINDOW_MS + 1L);
            sameFault(escalation, 5);
        });
        assertEquals(2, escalationLines(logged));
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Escalation diagnostic: content, logger category, thread dump gating
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    public void escalationIsLoggedOnceUnderTheProducerCategoryWithJvmState() throws Exception {
        NodeFaultEscalation escalation = newEscalation(false);
        String logged = captureStderr(() -> {
            for (int i = 0; i < 8; i++) {
                escalation.checkNodeFaultEscalation(Classification.FATAL_PRODUCER_UNUSABLE,
                        CpiKafkaPlusErrorCode.KP_PROD_001,
                        new RuntimeException("wrapper", new IllegalStateException("root")));
            }
        });
        assertEquals("exactly one escalation per window, got:\n" + logged, 1, escalationLines(logged));
        String line = escalationLine(logged);
        assertTrue(line, line.contains("ERROR " + CpiKafkaPlusProducer.class.getName() + " - "));
        assertTrue(line, line.contains(AdapterDiagnostics.MARKER + " " + ESCALATION));
        assertTrue(line, line.contains("faultClass=java.lang.IllegalStateException"));
        assertTrue(line, line.contains("errorCode=" + CpiKafkaPlusErrorCode.KP_PROD_001.code()));
        assertTrue(line, line.contains("classification=FATAL_PRODUCER_UNUSABLE"));
        assertTrue(line, line.contains("countInWindow=5"));
        assertTrue(line, line.contains("windowMinutes=20"));
        assertTrue(line, line.contains("producerPath=SHARED"));
        assertTrue(line, line.contains("clientId=client-1"));
        assertTrue(line, line.contains("topic=test-topic"));
        assertTrue(line, line.contains("recurred 5 times within 20 minutes"));
        assertTrue(line, line.contains("jvmUptimeMs="));
        assertTrue(line, line.contains("jvmHeapMaxMB="));
        assertFalse(line, line.contains("threadDump"));
    }

    @Test
    public void escalationCarriesABoundedThreadDumpAtFullLevel() throws Exception {
        NodeFaultEscalation escalation = newEscalation(true);
        String logged = captureStderr(() -> sameFault(escalation, 5));
        assertEquals(1, escalationLines(logged));
        String line = escalationLine(logged);
        assertTrue(line, line.contains("threadDump="));
        assertTrue(line, line.contains("threadDumpThreads="));
        assertTrue(line, line.contains("threadDumpTruncated="));
        assertTrue(line, line.contains("threadDumpTotalThreads="));
        assertTrue(line, line.contains("threadDumpMonitorInfoSupported="));
    }

    @Test
    public void failingTopicAndDiagnosticsLookupsDoNotSuppressTheEscalation() throws Exception {
        NodeFaultEscalation escalation = new NodeFaultEscalation(
                () -> { throw new IllegalStateException("topic"); },
                () -> { throw new IllegalStateException("level"); },
                () -> null, clock::get);
        String logged = captureStderr(() -> sameFault(escalation, 5));
        assertEquals(1, escalationLines(logged));
        String line = escalationLine(logged);
        assertTrue(line, line.contains("topic=<unavailable>"));
        assertFalse(line, line.contains("clientId="));
        assertFalse(line, line.contains("threadDump"));
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Wiring in CpiKafkaPlusProducer
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    public void producerFeedsSendFailuresWithEndpointTopicClientIdAndLevel() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
            endpoint.setCamelContext(ctx);
            endpoint.setBootstrapServers("broker-a:9092");
            endpoint.setTopic("test-topic");
            endpoint.setSecurityProtocol("PLAINTEXT");
            endpoint.setDiagnosticsLevel("FULL");
            CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
            setField(producer, "tracingHelper", new AdapterTracingHelper(endpoint));
            setField(producer, "resolvedClientId", "client-1");

            Map<String, String> context = new LinkedHashMap<>();
            context.put("topic", "test-topic");
            String logged = captureStderr(() -> {
                for (int i = 0; i < 5; i++) {
                    producer.handleSendFailure(new IllegalStateException("unclassified"), "producer.send", context);
                }
            });
            assertEquals(1, escalationLines(logged));
            String line = escalationLine(logged);
            assertTrue(line, line.contains("ERROR " + CpiKafkaPlusProducer.class.getName() + " - "));
            assertTrue(line, line.contains("classification=UNKNOWN_FATAL"));
            assertTrue(line, line.contains("countInWindow=5"));
            assertTrue(line, line.contains("clientId=client-1"));
            assertTrue(line, line.contains("topic=test-topic"));
            assertTrue(line, line.contains("threadDump="));
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }
}
