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

import javax.net.ssl.SSLHandshakeException;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;

/**
 * Decides which kind of failure a failed poll of the sender's Kafka consumer is. Package-private.
 */
final class PollFailureClassifier {

    private PollFailureClassifier() {
    }

    /**
     * Returns {@code true} when the poll failure is caused by a permanent configuration or
     * access problem that cannot heal via reconnects alone. Walks the full cause chain because
     * Kafka often wraps the effective broker error inside transport/runtime exceptions.
     * Unknown failures deliberately default to retryable to avoid false-positive permanent stops.
     */
    static boolean isNonRetryablePollFailure(Throwable failure) {
        return findNonRetryablePollFailureCause(failure) != null;
    }

    /**
     * Detects the class-space break of issue #148 on the poll path: either the OSGi
     * "bundle wiring ... no longer valid" text, or a class/link/native-library fault while this
     * route's loader belongs to a bundle revision an adapter update has already replaced.
     */
    static boolean isClassSpaceGoneFailure(Throwable failure, Class<?> routeClass) {
        return ClassSpaceFaults.isOnStaleClassSpace(failure, routeClass);
    }

    static Throwable findNonRetryablePollFailureCause(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof SaslAuthenticationException
                    || current instanceof AuthenticationException
                    || current instanceof AuthorizationException
                    || current instanceof SslAuthenticationException
                    || current instanceof SSLHandshakeException) {
                return current;
            }
            Throwable next = current.getCause();
            if (next == current) {
                break;
            }
            current = next;
        }
        return null;
    }

    /**
     * Detects a {@link FencedInstanceIdException} anywhere in the cause chain — the broker
     * rejecting a poll/rebalance because another member already holds this consumer's static
     * {@code group.instance.id} (KIP-345 conflict, issue #146).
     */
    static boolean isFencedInstanceIdFailure(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof FencedInstanceIdException) {
                return true;
            }
            Throwable next = current.getCause();
            if (next == current) {
                break;
            }
            current = next;
        }
        return false;
    }
}
