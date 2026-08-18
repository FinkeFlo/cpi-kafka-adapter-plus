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

import org.slf4j.Logger;

/**
 * Builds the single-line, structured diagnostic messages this adapter writes on failure paths.
 *
 * <p>Two properties of the SAP Cloud Integration runtime drive the whole design and are the reason
 * a plain {@code LOG.error(msg, throwable)} is not sufficient here:
 *
 * <ul>
 *   <li>The tenant trace file only receives <b>ERROR</b>. Anything logged at WARN, INFO or DEBUG is
 *       invisible in production, so a diagnostic that matters must be an ERROR.</li>
 *   <li>The tenant trace appender <b>discards the {@code Throwable}</b> that is passed alongside a
 *       log message; only the rendered message text survives. The stack trace must therefore be
 *       serialised <i>into</i> the message, which is what {@link #describeThrowable} does.</li>
 * </ul>
 *
 * <p>Output is a flat {@code key=value} sequence prefixed with a single, constant marker so that a
 * whole incident can be retrieved with one grep and parsed without knowing column positions. Values
 * are quoted when they contain whitespace and are always stripped of newlines, because a diagnostic
 * spread over several physical lines cannot be correlated in the tenant trace format.
 *
 * <p>Everything here stays within the Java 11 API level and adds no runtime dependency beyond
 * SLF4J, as required for an ADK adapter bundle.
 */
final class AdapterDiagnostics {

    /**
     * The single marker for every diagnostic line of this adapter. Kept as one constant so that
     * operators have exactly one string to grep for and so no second, competing marker can drift in.
     */
    static final String MARKER = "[CPI-KAFKA-PLUS-DIAG]";

    /** Stack frames rendered per exception level. Deep enough to cross the Kafka client boundary. */
    private static final int MAX_FRAMES_PER_LEVEL = 12;
    /** Levels of the cause chain to render. */
    private static final int MAX_CAUSE_DEPTH = 5;
    /** Suppressed exceptions rendered per level (try-with-resources, close failures). */
    private static final int MAX_SUPPRESSED = 3;
    /** Per-message truncation, so one pathological exception text cannot crowd out the frames. */
    private static final int MAX_MESSAGE_CHARS = 300;
    /** Overall budget for a single rendered line. */
    private static final int MAX_TOTAL_CHARS = 8000;

    private AdapterDiagnostics() {}

    /**
     * Logs an event at ERROR together with its throwable.
     *
     * <p>Centralised because the correct SLF4J call is not the obvious one. {@code log.error(text,
     * t)} treats {@code text} as a format string, so a {@code {}} anywhere inside it — entirely
     * possible once an exception message carries a JSON payload — would consume {@code t} as a
     * placeholder argument and silently drop the stack trace. A constant {@code "{}"} format with
     * the rendered line as the single argument cannot be affected by its own content.
     *
     * <p>The throwable is passed on as well as serialised into the message: the tenant trace
     * appender drops it, but local runs, unit tests and any other appender do render it.
     */
    static void error(Logger log, Event event, Throwable t) {
        log.error("{}", event.withThrowable(t).render(), t);
    }

    /** Logs an event at ERROR with no associated throwable. */
    static void error(Logger log, Event event) {
        log.error("{}", event.render());
    }

    /**
     * Starts a diagnostic event. {@code operation} names the failing activity in a stable,
     * greppable form, for example {@code producer.batch.send}.
     */
    static Event event(String operation) {
        return new Event(operation);
    }

    /**
     * Accumulates the fields of one diagnostic line. Not thread-safe; instances are meant to be
     * created, filled and rendered inside a single catch block.
     */
    static final class Event {

        private final StringBuilder sb = new StringBuilder(256);

        private Event(String operation) {
            sb.append(MARKER).append(' ').append(sanitize(operation, 120));
        }

        /**
         * Appends a field. {@code null} values are rendered as {@code null} rather than skipped,
         * because "the value was absent" is itself diagnostic information.
         */
        Event with(String key, Object value) {
            if (key == null) {
                return this;
            }
            sb.append(' ').append(sanitize(key, 60)).append('=');
            appendValue(value);
            return this;
        }

        /** Appends {@code key} only when {@code value} is non-null. */
        Event withOptional(String key, Object value) {
            return value == null ? this : with(key, value);
        }

        /**
         * Appends the serialised throwable under {@code error=}. Call this last: the stack trace is
         * the longest field and truncation trims from the end.
         */
        Event withThrowable(Throwable t) {
            return with("error", describeThrowable(t));
        }

        private void appendValue(Object value) {
            String text = value == null ? "null" : sanitize(String.valueOf(value), MAX_TOTAL_CHARS);
            boolean needsQuotes = text.isEmpty() || text.indexOf(' ') >= 0 || text.indexOf('=') >= 0
                    || text.indexOf('\'') >= 0;
            if (needsQuotes) {
                sb.append('\'').append(escapeQuotes(text)).append('\'');
            } else {
                sb.append(text);
            }
        }

        /**
         * Doubles single quotes inside a quoted value, so that a lone {@code '} always terminates
         * the value and {@code ''} always means a literal quote.
         *
         * <p>This is not hypothetical. Kafka routinely quotes identifiers in its own exception
         * messages, and so does this adapter, so an unescaped value would produce
         * {@code error='Failed to send to topic 'x': …'} — three quotes, no way for a reader or a
         * parser to tell where the value ends. The runbook instructs operators to parse these
         * lines, so an ambiguous record defeats the purpose of emitting it.
         */
        private static String escapeQuotes(String text) {
            return text.indexOf('\'') < 0 ? text : text.replace("'", "''");
        }

        /** Renders the line, applying the overall length budget. */
        String render() {
            if (sb.length() <= MAX_TOTAL_CHARS) {
                return sb.toString();
            }
            return sb.substring(0, MAX_TOTAL_CHARS) + "…(truncated " + (sb.length() - MAX_TOTAL_CHARS) + " chars)";
        }

        @Override
        public String toString() {
            return render();
        }
    }

    /**
     * Serialises a throwable, its full cause chain and its suppressed exceptions into one line,
     * including stack frames.
     *
     * <p>This exists because the CPI trace appender drops the {@code Throwable} argument of a log
     * call. {@code Throwable.getStackTrace()} is used deliberately: it returns the frames captured
     * when the exception was created, which is what is needed here. {@code StackWalker} would walk
     * the stack of the <i>current</i> thread and is therefore the wrong tool, quite apart from the
     * Java 11 API ceiling.
     */
    static String describeThrowable(Throwable t) {
        if (t == null) {
            return "null";
        }
        StringBuilder out = new StringBuilder(512);
        Throwable current = t;
        int depth = 0;
        while (current != null && depth < MAX_CAUSE_DEPTH) {
            if (depth > 0) {
                out.append(" CAUSED_BY ");
            }
            appendSingle(out, current);
            appendSuppressed(out, current);
            Throwable next = current.getCause();
            if (next == current) {
                break;
            }
            current = next;
            depth++;
        }
        if (current != null && depth >= MAX_CAUSE_DEPTH) {
            out.append(" CAUSED_BY …(chain truncated)");
        }
        return out.toString();
    }

    private static void appendSingle(StringBuilder out, Throwable t) {
        out.append(t.getClass().getName());
        String message = t.getMessage();
        if (message != null) {
            out.append("(\"").append(sanitize(message, MAX_MESSAGE_CHARS)).append("\")");
        }
        appendFrames(out, t.getStackTrace());
    }

    private static void appendFrames(StringBuilder out, StackTraceElement[] frames) {
        out.append(" at [");
        if (frames == null || frames.length == 0) {
            // A JIT-optimised throw can carry no frames at all; saying so is better than an
            // empty bracket that reads like a rendering bug.
            out.append("no stack trace available");
        } else {
            int shown = Math.min(MAX_FRAMES_PER_LEVEL, frames.length);
            for (int i = 0; i < shown; i++) {
                if (i > 0) {
                    out.append(" <- ");
                }
                StackTraceElement f = frames[i];
                out.append(f.getClassName()).append('.').append(f.getMethodName())
                        .append(':').append(f.getLineNumber());
            }
            if (frames.length > shown) {
                out.append(" <- …(").append(frames.length - shown).append(" more)");
            }
        }
        out.append(']');
    }

    private static void appendSuppressed(StringBuilder out, Throwable t) {
        Throwable[] suppressed = t.getSuppressed();
        if (suppressed == null || suppressed.length == 0) {
            return;
        }
        int shown = Math.min(MAX_SUPPRESSED, suppressed.length);
        for (int i = 0; i < shown; i++) {
            Throwable s = suppressed[i];
            out.append(" SUPPRESSED ").append(s.getClass().getName());
            String message = s.getMessage();
            if (message != null) {
                out.append("(\"").append(sanitize(message, MAX_MESSAGE_CHARS)).append("\")");
            }
        }
        if (suppressed.length > shown) {
            out.append(" SUPPRESSED …(").append(suppressed.length - shown).append(" more)");
        }
    }

    /**
     * Collapses newlines, carriage returns and tabs into spaces and truncates. A diagnostic that
     * spans several physical lines cannot be correlated in the tenant trace format, where a record
     * is one line.
     */
    private static String sanitize(String value, int maxChars) {
        if (value == null) {
            return "null";
        }
        String flattened = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
        if (flattened.length() <= maxChars) {
            return flattened;
        }
        return flattened.substring(0, maxChars) + "…";
    }
}
