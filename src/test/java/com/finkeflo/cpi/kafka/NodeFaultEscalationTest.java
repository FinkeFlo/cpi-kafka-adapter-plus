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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.finkeflo.cpi.kafka.KafkaErrorHelper.Classification;

/**
 * Characterization tests for the node-fault escalation (e4b, c6, c6b): fault identity for causes
 * that are not an {@link Exception}, the classifications that are tracked, the window reset and the
 * content and logger category of the {@code producer.node.fault.escalation} diagnostic.
 */
public class NodeFaultEscalationTest {

    private static final String ESCALATION = "producer.node.fault.escalation";

    private DefaultCamelContext camelContext;

    @Before
    public void setUp() throws Exception {
        camelContext = new DefaultCamelContext();
        camelContext.start();
    }

    @After
    public void tearDown() throws Exception {
        camelContext.stop();
    }

    private CpiKafkaPlusProducer newProducer(boolean diagnosticsFull) throws Exception {
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint();
        endpoint.setCamelContext(camelContext);
        endpoint.setBootstrapServers("broker-a:9092");
        endpoint.setTopic("test-topic");
        endpoint.setSecurityProtocol("PLAINTEXT");
        if (diagnosticsFull) {
            endpoint.setDiagnosticsLevel("FULL");
        }
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        Field clientId = CpiKafkaPlusProducer.class.getDeclaredField("resolvedClientId");
        clientId.setAccessible(true);
        clientId.set(producer, "client-1");
        return producer;
    }

    private static void check(CpiKafkaPlusProducer producer, Classification classification,
                              CpiKafkaPlusErrorCode errorCode, Throwable cause) throws Exception {
        Method m = CpiKafkaPlusProducer.class.getDeclaredMethod(
                "checkNodeFaultEscalation", Classification.class, CpiKafkaPlusErrorCode.class, Throwable.class);
        m.setAccessible(true);
        try {
            m.invoke(producer, classification, errorCode, cause);
        } catch (InvocationTargetException e) {
            throw (Exception) e.getCause();
        }
    }

    private static Throwable resolveCause(CpiKafkaPlusProducer producer, Throwable cause) throws Exception {
        Method m = CpiKafkaPlusProducer.class.getDeclaredMethod("resolveNodeFaultIdentityCause", Throwable.class);
        m.setAccessible(true);
        return (Throwable) m.invoke(producer, cause);
    }

    private static Object field(CpiKafkaPlusProducer producer, String name) throws Exception {
        Field f = CpiKafkaPlusProducer.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(producer);
    }

    private static void setField(CpiKafkaPlusProducer producer, String name, Object value) throws Exception {
        Field f = CpiKafkaPlusProducer.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(producer, value);
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

    // ─────────────────────────────────────────────────────────────────────────────
    // Fault identity for causes that are not an Exception
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    public void nullCauseHasNoIdentityCause() throws Exception {
        assertNull(resolveCause(newProducer(false), null));
    }

    @Test
    public void errorCauseResolvesToTheRootOfItsChain() throws Exception {
        IOException root = new IOException("root");
        Error error = new Error("outer", new IllegalStateException("middle", root));
        assertSame(root, resolveCause(newProducer(false), error));
    }

    @Test
    public void errorWithoutCauseResolvesToItself() throws Exception {
        Error error = new OutOfMemoryError("no cause");
        assertSame(error, resolveCause(newProducer(false), error));
    }

    @Test
    public void nullCauseEscalatesAsUnknownFaultClass() throws Exception {
        CpiKafkaPlusProducer producer = newProducer(false);
        String logged = captureStderr(() -> {
            for (int i = 0; i < 5; i++) {
                check(producer, Classification.UNKNOWN_FATAL, CpiKafkaPlusErrorCode.KP_GEN_001, null);
            }
        });
        assertEquals(1, escalationLines(logged));
        assertTrue(logged, escalationLine(logged).contains("faultClass=unknown"));
    }

    @Test
    public void errorCauseEscalatesWithTheRootCauseClass() throws Exception {
        CpiKafkaPlusProducer producer = newProducer(false);
        String logged = captureStderr(() -> {
            for (int i = 0; i < 5; i++) {
                check(producer, Classification.FATAL_PRODUCER_UNUSABLE, CpiKafkaPlusErrorCode.KP_PROD_001,
                        new Error("wrapper " + i, new IllegalMonitorStateException("root")));
            }
        });
        assertEquals(1, escalationLines(logged));
        assertTrue(logged, escalationLine(logged).contains("faultClass=java.lang.IllegalMonitorStateException"));
    }

    @Test
    public void nullAndErrorCausesAreDifferentFaults() throws Exception {
        CpiKafkaPlusProducer producer = newProducer(false);
        for (int i = 0; i < 4; i++) {
            check(producer, Classification.UNKNOWN_FATAL, CpiKafkaPlusErrorCode.KP_GEN_001, null);
        }
        check(producer, Classification.UNKNOWN_FATAL, CpiKafkaPlusErrorCode.KP_GEN_001, new Error("other"));
        assertFalse((boolean) field(producer, "nodeFaultEscalated"));
        assertEquals(1, (int) field(producer, "nodeFaultCountInWindow"));
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Tracked classifications and window
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    public void retriableAndDataErrorsAreNotTracked() throws Exception {
        CpiKafkaPlusProducer producer = newProducer(false);
        String logged = captureStderr(() -> {
            for (int i = 0; i < 10; i++) {
                check(producer, Classification.RETRIABLE, CpiKafkaPlusErrorCode.KP_PROD_001,
                        new RuntimeException("retriable"));
                check(producer, Classification.FATAL_DATA_ERROR, CpiKafkaPlusErrorCode.KP_PROD_001,
                        new RuntimeException("data"));
            }
        });
        assertEquals(0, escalationLines(logged));
        assertEquals(0, (int) field(producer, "nodeFaultCountInWindow"));
        assertNull(field(producer, "nodeFaultIdentity"));
    }

    @Test
    public void expiredWindowRestartsTheCount() throws Exception {
        CpiKafkaPlusProducer producer = newProducer(false);
        for (int i = 0; i < 4; i++) {
            check(producer, Classification.FATAL_PRODUCER_UNUSABLE, CpiKafkaPlusErrorCode.KP_PROD_001,
                    new RuntimeException("same"));
        }
        // Move the window start just over 20 minutes into the past.
        setField(producer, "nodeFaultWindowStartMs", System.currentTimeMillis() - 20 * 60 * 1000L - 1000L);

        check(producer, Classification.FATAL_PRODUCER_UNUSABLE, CpiKafkaPlusErrorCode.KP_PROD_001,
                new RuntimeException("same"));
        assertEquals(1, (int) field(producer, "nodeFaultCountInWindow"));
        assertFalse((boolean) field(producer, "nodeFaultEscalated"));
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Escalation diagnostic: content, logger category, thread dump gating
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    public void escalationIsLoggedOnceUnderTheProducerCategoryWithJvmState() throws Exception {
        CpiKafkaPlusProducer producer = newProducer(false);
        String logged = captureStderr(() -> {
            for (int i = 0; i < 8; i++) {
                check(producer, Classification.FATAL_PRODUCER_UNUSABLE, CpiKafkaPlusErrorCode.KP_PROD_001,
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
        CpiKafkaPlusProducer producer = newProducer(true);
        String logged = captureStderr(() -> {
            for (int i = 0; i < 5; i++) {
                check(producer, Classification.FATAL_PRODUCER_UNUSABLE, CpiKafkaPlusErrorCode.KP_PROD_001,
                        new RuntimeException("same"));
            }
        });
        assertEquals(1, escalationLines(logged));
        String line = escalationLine(logged);
        assertTrue(line, line.contains("threadDump="));
        assertTrue(line, line.contains("threadDumpThreads="));
        assertTrue(line, line.contains("threadDumpTruncated="));
        assertTrue(line, line.contains("threadDumpTotalThreads="));
        assertTrue(line, line.contains("threadDumpMonitorInfoSupported="));
    }
}
