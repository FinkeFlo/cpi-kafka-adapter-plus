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

    private KafkaErrorHelper() {}

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
     */
    static boolean isFatalKafkaException(Throwable cause) {
        return cause instanceof AuthenticationException
                || cause instanceof AuthorizationException
                || cause instanceof UnsupportedVersionException;
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
