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

import javax.net.ssl.SSLException;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

/**
 * Shared error-handling utilities used by both Consumer and Producer.
 */
final class KafkaErrorHelper {

    /** After this many consecutive init failures, log level escalates from WARN to ERROR. */
    static final int INIT_FAILURE_ESCALATION_THRESHOLD = 10;

    /** Maximum depth when walking the cause chain to avoid infinite loops. */
    private static final int MAX_CAUSE_DEPTH = 10;

    private KafkaErrorHelper() {}

    /** Three-way classification of Kafka-related exceptions. */
    enum Classification {
        RETRIABLE,
        FATAL_PRODUCER_UNUSABLE,
        FATAL_DATA_ERROR,
        UNKNOWN_FATAL
    }

    /** Classifies a throwable by walking its entire cause chain. */
    static Classification classify(Throwable t) {
        if (t == null) return Classification.RETRIABLE;
        boolean sawKafkaException = false;
        Throwable current = t;
        int depth = 0;
        while (current != null && depth < MAX_CAUSE_DEPTH) {
            Classification c = classifySingle(current);
            if (c != null) return c;
            if (current instanceof org.apache.kafka.common.KafkaException) sawKafkaException = true;
            Throwable next = current.getCause();
            if (next == current) break;
            current = next;
            depth++;
        }
        return sawKafkaException ? Classification.FATAL_PRODUCER_UNUSABLE : Classification.UNKNOWN_FATAL;
    }

    private static Classification classifySingle(Throwable t) {
        if (t instanceof org.apache.kafka.common.errors.RetriableException) return Classification.RETRIABLE;
        if (t instanceof org.apache.kafka.common.errors.ProducerFencedException) return Classification.FATAL_PRODUCER_UNUSABLE;
        if (t instanceof org.apache.kafka.common.errors.OutOfOrderSequenceException) return Classification.FATAL_PRODUCER_UNUSABLE;
        if (t.getClass().getName().equals("org.apache.kafka.common.errors.InvalidPidMappingException")) return Classification.FATAL_PRODUCER_UNUSABLE;
        if (t instanceof AuthenticationException) return Classification.FATAL_PRODUCER_UNUSABLE;
        if (t instanceof AuthorizationException) return Classification.FATAL_PRODUCER_UNUSABLE;
        if (t instanceof UnsupportedVersionException) return Classification.FATAL_PRODUCER_UNUSABLE;
        if (t instanceof org.apache.kafka.common.errors.RecordTooLargeException) return Classification.FATAL_DATA_ERROR;
        if (t instanceof org.apache.kafka.common.errors.InvalidTopicException) return Classification.FATAL_DATA_ERROR;
        if (t instanceof org.apache.kafka.common.errors.SerializationException) return Classification.FATAL_DATA_ERROR;
        return null;
    }

    /**
     * Wraps a Throwable in an Exception if it is not already one.
     * Needed because Kafka can throw Errors (e.g. OutOfMemoryError)
     * but Camel APIs expect Exception.
     */
    static Exception wrapIfError(Throwable t) {
        if (t instanceof Exception) {
            return (Exception) t;
        }
        return new RuntimeException(t.getClass().getSimpleName() + ": " + t.getMessage(), t);
    }

    /**
     * Returns true for Kafka exceptions that indicate a broken connection
     * which cannot recover without creating a new client instance.
     * @deprecated Use {@link #classify(Throwable)} for three-way classification.
     */
    @Deprecated
    static boolean isFatalKafkaException(Throwable cause) {
        if (cause == null) return false;
        Classification c = classifySingle(cause);
        return c == Classification.FATAL_PRODUCER_UNUSABLE;
    }

    /**
     * Walks the exception cause chain to find the deepest Kafka-related cause.
     * Stops early if a fatal exception is found.
     */
    static Throwable extractKafkaCause(Exception e) {
        Throwable cause = e.getCause();
        while (cause != null && cause.getCause() != null && cause.getCause() != cause) {
            if (isFatalKafkaException(cause)) {
                return cause;
            }
            cause = cause.getCause();
        }
        return cause != null ? cause : e;
    }

    /**
     * Returns true when the cause chain contains a failure that definitively rules out talking to
     * the broker with the current configuration: a rejected authentication or a failed TLS
     * handshake. Both are only ever raised after a real connection attempt, so a client that hits
     * one will hit it again — reporting it immediately is safe and far more useful than the
     * metadata timeout it would otherwise degenerate into.
     *
     * <p>Deliberately <em>not</em> {@link AuthorizationException}: its subclasses
     * {@code TopicAuthorizationException} and {@code ClusterAuthorizationException} only mean that
     * a specific operation (typically DESCRIBE) is not permitted, which says nothing about whether
     * producing or consuming would work.
     */
    static boolean isDefinitiveAuthOrTlsFailure(Throwable t) {
        Throwable cur = t;
        int depth = 0;
        while (cur != null && depth < 10) {
            if (cur instanceof AuthenticationException || cur instanceof SSLException) {
                return true;
            }
            if (cur.getCause() == cur) {
                break;
            }
            cur = cur.getCause();
            depth++;
        }
        return false;
    }

    /**
     * Returns true when the cause chain contains the signature of KAFKA-10902, a defect in the Kafka
     * client that is still open and unfixed and is present in the version this adapter embeds.
     *
     * <p>The mechanism, verified in the bytecode of the shipped {@code kafka-clients} jar:
     * {@code ProducerMetadata.awaitUpdate} is declared {@code synchronized} and hands {@code this}
     * to {@code SystemTime.waitObject}, which enters the monitor of that same object a second time
     * and then calls {@code Object.wait()}. The wait therefore happens at monitor recursion depth 2,
     * and under conditions the JDK does not guarantee the thread can lose ownership and
     * {@code Object.wait()} throws.
     *
     * <p>Two properties make this identifiable from the exception alone:
     *
     * <ul>
     *   <li>{@code Object.wait()} is the only construct that produces the bare message
     *       {@code "current thread is not owner"}. {@code ReentrantLock} and the other
     *       {@code java.util.concurrent} locks throw {@link IllegalMonitorStateException} with a
     *       {@code null} message, so they cannot be confused with it.</li>
     *   <li>This adapter contains no {@code wait}, {@code notify} or {@code synchronized} block on
     *       the send path, so an occurrence originates inside the Kafka client.</li>
     * </ul>
     *
     * <p>When frames are available they must confirm a Kafka origin; a match without frames is
     * accepted, since a repeatedly thrown exception can be optimised down to none.
     *
     * @see #isTransientMetadataMonitorFaultRetryable(Throwable) for why acting on this is safe
     */
    static boolean isMetadataMonitorFault(Throwable t) {
        Throwable cur = t;
        int depth = 0;
        while (cur != null && depth < 10) {
            if (cur instanceof IllegalMonitorStateException
                    && cur.getMessage() != null
                    && cur.getMessage().contains(MONITOR_FAULT_MESSAGE)) {
                return hasKafkaOriginOrNoFrames(cur);
            }
            if (cur.getCause() == cur) {
                break;
            }
            cur = cur.getCause();
            depth++;
        }
        return false;
    }

    /** The exact text {@code Object.wait()} uses when the calling thread does not own the monitor. */
    private static final String MONITOR_FAULT_MESSAGE = "current thread is not owner";

    private static boolean hasKafkaOriginOrNoFrames(Throwable t) {
        StackTraceElement[] frames = t.getStackTrace();
        if (frames == null || frames.length == 0) {
            return true;
        }
        for (StackTraceElement frame : frames) {
            if (frame.getClassName().startsWith("org.apache.kafka")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true when a {@link #isMetadataMonitorFault} failure raised by a <em>synchronous</em>
     * {@code KafkaProducer.send(...)} call may be retried without any risk of producing a duplicate.
     *
     * <p>This is not a judgement call. {@code KafkaProducer.doSend()} calls
     * {@code waitOnMetadata(...)} <b>before</b> {@code accumulator.append(...)}. If
     * {@code waitOnMetadata} throws synchronously, the record was never appended to the accumulator,
     * was never assigned a sequence number and was never handed to the sender thread — so there is
     * nothing that could be sent twice. The guarantee comes from the ordering of those two calls and
     * holds independently of idempotence, which is additionally enabled.
     *
     * <p>The restriction to the synchronous throw matters: the same exception surfacing from
     * {@code Future.get()} would mean the record had already been accepted, and retrying it could
     * duplicate. Callers must therefore only use this at the {@code send(...)} call site.
     */
    static boolean isTransientMetadataMonitorFaultRetryable(Throwable synchronousSendFailure) {
        return isMetadataMonitorFault(synchronousSendFailure);
    }

    /**
     * Renders a throwable and its cause chain as a compact one-line {@code SimpleName: message}
     * sequence joined by {@code " <- "}. Intended for exception messages that operators read in the
     * CPI Message Processing Log, where only the message text of the outermost exception is shown.
     */
    static String describeChain(Throwable t) {
        if (t == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        Throwable cur = t;
        int depth = 0;
        while (cur != null && depth < 10) {
            if (sb.length() > 0) {
                sb.append(" <- ");
            }
            sb.append(cur.getClass().getSimpleName()).append(": ").append(cur.getMessage());
            if (cur.getCause() == cur) {
                break;
            }
            cur = cur.getCause();
            depth++;
        }
        return sb.toString();
    }

    /**
     * Returns a compact one-line string representation of the top N stack frames of a throwable
     * plus its cause chain. Used because the CPI trace-log appender drops the exception object's
     * stack trace; we therefore encode it into the log message itself.
     */
    static String describeTopStack(Throwable t, int maxFrames) {
        if (t == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        Throwable cur = t;
        int depth = 0;
        while (cur != null && depth < 4) {
            if (depth > 0) {
                sb.append(" CAUSED_BY ");
            }
            sb.append(cur.getClass().getSimpleName());
            String msg = cur.getMessage();
            if (msg != null) {
                String trimmed = msg.length() > 200 ? msg.substring(0, 200) + "…" : msg;
                sb.append("('").append(trimmed.replace('\n', ' ')).append("')");
            }
            StackTraceElement[] frames = cur.getStackTrace();
            sb.append("[");
            int shown = Math.min(maxFrames, frames.length);
            for (int i = 0; i < shown; i++) {
                if (i > 0) {
                    sb.append(" <- ");
                }
                StackTraceElement f = frames[i];
                sb.append(f.getClassName()).append('.').append(f.getMethodName())
                        .append(':').append(f.getLineNumber());
            }
            if (frames.length > shown) {
                sb.append(" <- …(" ).append(frames.length - shown).append(" more)");
            }
            sb.append("]");
            cur = cur.getCause();
            depth++;
        }
        return sb.toString();
    }

    /**
     * Returns an operator-facing hint when a security protocol without TLS is configured. A
     * {@code PLAINTEXT} or {@code SASL_PLAINTEXT} client against a TLS-only listener never
     * completes a connection and never sees a handshake error either — the broker simply drops it,
     * so the only symptom is a metadata timeout that names no cause at all. Since virtually every
     * managed Kafka offering serves TLS only, this is the first thing to check.
     *
     * <p>Matching is on the configured protocol alone, deliberately not on the bootstrap host name:
     * vendor domain patterns would be brittle and would miss self-hosted TLS brokers.
     *
     * @return the hint, or {@code null} when the protocol already uses TLS or is not configured
     */
    static String tlsMismatchHint(String securityProtocol) {
        if (securityProtocol == null || securityProtocol.trim().isEmpty()) {
            return null;
        }
        String protocol = securityProtocol.trim();
        if (protocol.toUpperCase(java.util.Locale.ROOT).contains("SSL")) {
            return null;
        }
        return "securityProtocol=" + protocol + " does not use TLS. Managed brokers such as "
                + "Confluent Cloud accept TLS connections only — if the broker is reachable but "
                + "never answers, switch the Security Protocol to SASL_SSL (or SSL).";
    }
}
