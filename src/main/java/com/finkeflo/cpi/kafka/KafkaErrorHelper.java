/*-
 * #%L
 * SAP CPI Kafka Adapter Plus
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
}
