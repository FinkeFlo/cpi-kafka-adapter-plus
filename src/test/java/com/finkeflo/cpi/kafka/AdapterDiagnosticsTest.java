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

    /**
     * The production message that motivated this class quotes the topic name in single quotes, and
     * so do Kafka's own exception messages. Without escaping, such a value produces three quotes in
     * one field and there is no way for a reader or a parser to tell where the value ends — on the
     * exact record class this class exists to make readable.
     */
    @Test
    public void aQuoteInsideAValueCannotTerminateTheValueEarly() {
        String line = AdapterDiagnostics.event("producer.batch.record.send")
                .with("detail", "Failed to send batch to Kafka topic 'test-topic': boom")
                .with("producerPath", "SHARED")
                .render();

        Assert.assertTrue("quotes inside the value must be doubled, not left bare: " + line,
                line.contains("'Failed to send batch to Kafka topic ''test-topic'': boom'"));

        // A field appended afterwards must still be parseable, which is the point of escaping.
        Assert.assertTrue(line, line.endsWith("producerPath=SHARED"));

        // Counting quotes is how a parser finds the end of a value: doubling keeps the count even.
        long quotes = line.chars().filter(c -> c == '\'').count();
        Assert.assertEquals("an odd number of quotes means an unterminated value: " + line,
                0, quotes % 2);
    }

    /** A value consisting solely of a quote must still be quoted, or a field boundary is lost. */
    @Test
    public void aValueConsistingSolelyOfAQuoteIsStillQuoted() {
        String line = AdapterDiagnostics.event("test").with("k", "'").with("next", "x").render();
        Assert.assertTrue(line, line.contains("k='''"));
        Assert.assertTrue(line, line.endsWith("next=x"));
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

    // --- renderFullStackTrace: the attachment form ---

    /** A throwable whose message cannot be produced. Rare in the JDK, real in lazily built messages. */
    private static final class HostileThrowable extends RuntimeException {
        private static final long serialVersionUID = 1L;

        HostileThrowable() {
            super("never read");
        }

        @Override
        public String getMessage() {
            throw new IllegalStateException("message computation failed");
        }
    }

    private static Throwable nestedFailure() {
        Throwable root = thrown(() -> { throw new IllegalArgumentException("root cause"); });
        Throwable middle = new IllegalStateException("middle layer", root);
        RuntimeException top = new RuntimeException("top layer", middle);
        top.addSuppressed(new IllegalStateException("suppressed close failure"));
        return top;
    }

    @Test
    public void fullStackTraceCarriesEveryCauseLevelAndSuppressedException() {
        String rendered = AdapterDiagnostics.renderFullStackTrace(nestedFailure(), 0);

        Assert.assertTrue(rendered, rendered.contains("java.lang.RuntimeException: top layer"));
        Assert.assertTrue(rendered, rendered.contains("Caused by: java.lang.IllegalStateException: middle layer"));
        Assert.assertTrue(rendered, rendered.contains("Caused by: java.lang.IllegalArgumentException: root cause"));
        Assert.assertTrue(rendered, rendered.contains("Suppressed: java.lang.IllegalStateException: suppressed close failure"));
        // Multi-line by construction — this is the property the compact form cannot have, and the
        // reason the attachment exists at all.
        Assert.assertTrue(rendered, rendered.contains("\n\tat "));
    }

    /**
     * The compact line form caps at 12 frames per level. The attachment must not, because a failure
     * raised outside the adapter puts its actual cause far below the adapter's own frames.
     */
    @Test
    public void fullStackTraceIsNotLimitedToTwelveFramesPerLevel() {
        Throwable deep = thrown(() -> recurseThenThrow(40));

        String compact = AdapterDiagnostics.describeThrowable(deep);
        String full = AdapterDiagnostics.renderFullStackTrace(deep, 0);

        Assert.assertTrue(compact, compact.contains("more)"));
        Assert.assertTrue("the full form must show more frames than the compact one",
                countOccurrences(full, "\tat ") > 12);
    }

    @Test
    public void fullStackTraceIsCappedAndSaysWhatItDropped() {
        Throwable deep = thrown(() -> recurseThenThrow(40));
        String uncapped = AdapterDiagnostics.renderFullStackTrace(deep, 0);

        int cap = 200;
        String capped = AdapterDiagnostics.renderFullStackTrace(deep, cap);

        Assert.assertTrue("the fixture must be long enough to be truncated", uncapped.length() > cap);
        Assert.assertTrue(capped, capped.startsWith(uncapped.substring(0, cap)));
        // The numbers must be the real ones: a truncation notice that cannot be checked against the
        // original is not evidence, it is decoration.
        Assert.assertTrue(capped, capped.endsWith("…[truncated: " + (uncapped.length() - cap)
                + " of " + uncapped.length() + " chars omitted]"));
    }

    @Test
    public void fullStackTraceLeavesShortTracesUntouched() {
        Throwable t = thrown(() -> { throw new IllegalStateException("short"); });
        String rendered = AdapterDiagnostics.renderFullStackTrace(t, AdapterDiagnostics.MAX_ATTACHMENT_CHARS);
        Assert.assertFalse(rendered, rendered.contains("truncated"));
    }

    @Test
    public void fullStackTraceSurvivesNull() {
        Assert.assertEquals("null", AdapterDiagnostics.renderFullStackTrace(null, 4096));
    }

    /**
     * The attachment is written while an incident is being reported. A diagnostic that throws would
     * replace the very failure it was meant to describe — the failure mode ADR 0004 exists to
     * prevent — so a throwable that cannot render itself must degrade, not propagate.
     */
    @Test
    public void fullStackTraceSurvivesAThrowableThatCannotRenderItself() {
        String rendered = AdapterDiagnostics.renderFullStackTrace(new HostileThrowable(), 4096);

        Assert.assertTrue(rendered, rendered.contains("stack trace rendering failed"));
        Assert.assertTrue(rendered, rendered.contains("java.lang.IllegalStateException"));
    }

    private static void recurseThenThrow(int depth) {
        if (depth <= 0) {
            throw new IllegalStateException("deep failure");
        }
        recurseThenThrow(depth - 1);
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length())) {
            count++;
        }
        return count;
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
