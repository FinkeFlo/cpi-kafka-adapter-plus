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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Guards the properties the CPI tenant trace format depends on: one line, one marker, and a stack
 * trace carried inside the message text rather than in the (discarded) {@code Throwable} argument.
 */
public class AdapterDiagnosticsTest {

    private static Throwable thrown(Runnable r) {
        try {
            r.run();
            throw new AssertionError("expected the runnable to throw");
        } catch (Throwable t) {
            return t;
        }
    }

    @Test
    public void rendersMarkerExactlyOnce() {
        String line = AdapterDiagnostics.event("producer.batch.send").with("topic", "test-topic").render();
        Assert.assertTrue(line.startsWith(AdapterDiagnostics.MARKER));
        Assert.assertEquals(AdapterDiagnostics.MARKER + " producer.batch.send topic=test-topic", line);
    }

    @Test
    public void quotesValuesContainingWhitespaceOrEquals() {
        String line = AdapterDiagnostics.event("op")
                .with("plain", "value")
                .with("spaced", "two words")
                .with("equalsy", "a=b")
                .render();
        Assert.assertTrue(line.contains("plain=value"));
        Assert.assertTrue(line.contains("spaced='two words'"));
        Assert.assertTrue(line.contains("equalsy='a=b'"));
    }

    @Test
    public void rendersNullValueRatherThanSkippingTheKey() {
        // "the value was absent" is itself diagnostic information.
        String line = AdapterDiagnostics.event("op").with("transactionalId", null).render();
        Assert.assertTrue(line, line.contains("transactionalId=null"));
    }

    @Test
    public void withOptionalSkipsNull() {
        String line = AdapterDiagnostics.event("op").withOptional("slotId", null).render();
        Assert.assertFalse(line, line.contains("slotId"));
    }

    @Test
    public void neverEmitsALineBreak() {
        // The tenant trace format is one record per physical line; a wrapped diagnostic cannot be
        // correlated back to its record.
        String line = AdapterDiagnostics.event("op")
                .with("multi", "first\nsecond\r\nthird\ttabbed")
                .withThrowable(new IllegalStateException("boom\nand more"))
                .render();
        Assert.assertEquals(-1, line.indexOf('\n'));
        Assert.assertEquals(-1, line.indexOf('\r'));
    }

    @Test
    public void serialisesStackFramesIntoTheMessageText() {
        Throwable t = thrown(() -> {
            throw new IllegalStateException("boom");
        });
        String rendered = AdapterDiagnostics.describeThrowable(t);
        Assert.assertTrue(rendered, rendered.contains("java.lang.IllegalStateException"));
        Assert.assertTrue(rendered, rendered.contains("boom"));
        Assert.assertTrue(rendered, rendered.contains(AdapterDiagnosticsTest.class.getName()));
        Assert.assertTrue(rendered, rendered.contains(" at ["));
    }

    @Test
    public void rendersTheWholeCauseChain() {
        Exception root = new IllegalArgumentException("root cause");
        Exception middle = new IllegalStateException("middle", root);
        Exception top = new RuntimeException("top", middle);

        String rendered = AdapterDiagnostics.describeThrowable(top);
        Assert.assertTrue(rendered, rendered.contains("top"));
        Assert.assertTrue(rendered, rendered.contains("middle"));
        Assert.assertTrue(rendered, rendered.contains("root cause"));
        Assert.assertEquals(2, rendered.split("CAUSED_BY", -1).length - 1);
    }

    @Test
    public void rendersSuppressedExceptions() {
        Exception primary = new IllegalStateException("primary");
        primary.addSuppressed(new IllegalStateException("failed to abort transaction"));

        String rendered = AdapterDiagnostics.describeThrowable(primary);
        Assert.assertTrue(rendered, rendered.contains("SUPPRESSED"));
        Assert.assertTrue(rendered, rendered.contains("failed to abort transaction"));
    }

    @Test
    public void terminatesOnACyclicCauseChain() {
        // Reachable in practice when a retry wraps the exception that triggered it.
        Exception first = new IllegalStateException("first");
        Exception second = new IllegalStateException("second", first);
        first.initCause(second);

        String rendered = AdapterDiagnostics.describeThrowable(first);
        Assert.assertTrue(rendered, rendered.contains("first"));
        Assert.assertTrue(rendered, rendered.contains("second"));
        Assert.assertTrue(rendered, rendered.contains("chain truncated"));
    }

    @Test
    public void handlesNullThrowable() {
        Assert.assertEquals("null", AdapterDiagnostics.describeThrowable(null));
    }

    @Test
    public void reportsAnEmptyStackTraceExplicitly() {
        // A JIT-optimised throw can arrive without frames; saying so beats an empty bracket that
        // reads like a rendering bug.
        Exception noFrames = new IllegalStateException("no frames");
        noFrames.setStackTrace(new StackTraceElement[0]);
        String rendered = AdapterDiagnostics.describeThrowable(noFrames);
        Assert.assertTrue(rendered, rendered.contains("no stack trace available"));
    }

    @Test
    public void appliesTheOverallLengthBudget() {
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 5000; i++) {
            huge.append("payload").append(i).append(' ');
        }
        String line = AdapterDiagnostics.event("op").with("body", huge.toString()).render();
        Assert.assertTrue(line, line.contains("truncated"));
        Assert.assertTrue("line was " + line.length() + " chars", line.length() < 9000);
    }

    @Test
    public void capturesTheProductionIncidentSignature() {
        // Reproduces the shape of the failure that went entirely unlogged: an
        // IllegalMonitorStateException whose message is the bare "current thread is not owner",
        // wrapped by the adapter. Uses a synthetic topic name on purpose.
        IllegalMonitorStateException imse = thrownMonitorState();
        RuntimeException wrapper = new RuntimeException(
                "Batch send failed at record index 0: " + imse.getMessage(), imse);

        String line = AdapterDiagnostics.event("producer.batch.send")
                .with("producerPath", "SHARED")
                .with("topic", "test-topic")
                .with("recordIndex", 0)
                .withThrowable(wrapper)
                .render();

        Assert.assertTrue(line, line.contains("java.lang.IllegalMonitorStateException"));
        Assert.assertTrue(line, line.contains("current thread is not owner"));
        Assert.assertTrue(line, line.contains("CAUSED_BY"));
        Assert.assertTrue(line, line.contains("producerPath=SHARED"));
        Assert.assertTrue(line, line.contains("recordIndex=0"));
        // The decisive part: the frames must be in the text, because the tenant appender drops the
        // Throwable argument entirely.
        Assert.assertTrue(line, line.contains(AdapterDiagnosticsTest.class.getName()));
    }

    @Test
    public void errorLoggingSurvivesABraceInTheExceptionMessage() throws Exception {
        // SLF4J treats the first String as a format string. A "{}" inside an exception message —
        // realistic as soon as a JSON payload ends up in it — would otherwise consume the Throwable
        // argument as a placeholder and silently drop the stack trace.
        Exception withBraces = new IllegalStateException("payload rejected: {} was empty");

        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        String logged;
        try {
            System.setErr(new PrintStream(captured, true, "UTF-8"));
            AdapterDiagnostics.error(LoggerFactory.getLogger(AdapterDiagnosticsTest.class),
                    AdapterDiagnostics.event("producer.batch.send").with("topic", "test-topic"),
                    withBraces);
        } finally {
            System.setErr(original);
        }
        logged = captured.toString("UTF-8");

        Assert.assertTrue(logged, logged.contains("ERROR"));
        Assert.assertTrue(logged, logged.contains(AdapterDiagnostics.MARKER));
        Assert.assertTrue(logged, logged.contains("topic=test-topic"));
        // The brace pair must survive verbatim rather than being substituted away.
        Assert.assertTrue(logged, logged.contains("payload rejected: {} was empty"));
        // And the Throwable must still have reached the appender as a Throwable.
        Assert.assertTrue(logged, logged.contains("java.lang.IllegalStateException"));
    }

    private static IllegalMonitorStateException thrownMonitorState() {
        Object monitor = new Object();
        try {
            // Object.wait() without holding the monitor is the only construct that produces the
            // exact message "current thread is not owner"; ReentrantLock and friends throw with a
            // null message. This is what makes the production message attributable at all.
            monitor.wait(1L);
            throw new AssertionError("expected IllegalMonitorStateException");
        } catch (IllegalMonitorStateException expected) {
            return expected;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
