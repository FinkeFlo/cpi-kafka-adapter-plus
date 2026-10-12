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

/**
 * Sorts a failed poll of the sender's Kafka consumer ({@link CpiKafkaPlusConsumer}) into the
 * {@link Kind} that decides how the consumer reacts. Moved out of the consumer, whose emit poll and
 * keep-alive poll both switch on {@link #classify}; the handler of each kind, and what each poll
 * does with {@link Kind#OTHER}, stay in the consumer. The emit poll passes every {@link Throwable},
 * the keep-alive poll only {@link Exception}s. Does not log. Package-private.
 */
final class PollFailureClassifier {

    /** What a failed poll is. The consumer has one handler for each kind except {@link #OTHER}. */
    enum Kind {
        /** Rejected credentials, permissions or certificates: polling stops until a redeploy. */
        NON_RETRYABLE,
        /** Another member holds this consumer's static {@code group.instance.id}: polling cools down. */
        FENCED,
        /** An adapter update replaced the class space of the route (issue #148): polling stops. */
        CLASS_SPACE_GONE,
        /** Anything else; the emit poll and the keep-alive poll each handle it their own way. */
        OTHER
    }

    private PollFailureClassifier() {
    }

    /**
     * The kind of a failed poll: {@link Kind#NON_RETRYABLE}, then {@link Kind#FENCED}, then
     * {@link Kind#CLASS_SPACE_GONE} are checked, and the first match wins. A failure whose cause
     * chain holds both rejected credentials and a fenced member is therefore
     * {@link Kind#NON_RETRYABLE}.
     *
     * @param routeClass a class of the running route; its loader tells whether an adapter update
     *                   has replaced the route's bundle revision
     *                   (see {@link ClassSpaceFaults#isOnStaleClassSpace})
     */
    static Kind classify(Throwable failure, Class<?> routeClass) {
        if (isNonRetryablePollFailure(failure)) {
            return Kind.NON_RETRYABLE;
        }
        if (isFencedInstanceIdFailure(failure)) {
            return Kind.FENCED;
        }
        if (isClassSpaceGoneFailure(failure, routeClass)) {
            return Kind.CLASS_SPACE_GONE;
        }
        return Kind.OTHER;
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
    private static boolean isClassSpaceGoneFailure(Throwable failure, Class<?> routeClass) {
        return ClassSpaceFaults.isOnStaleClassSpace(failure, routeClass);
    }

    /**
     * The first link of the cause chain, from the outside in, that makes the failure
     * non-retryable, or {@code null} when there is none. The consumer names its class in the
     * message of the policy error ({@link CpiKafkaPlusConsumer#buildStoppedByErrorPolicyException}).
     */
    static Throwable findNonRetryablePollFailureCause(Throwable failure) {
        for (Throwable current : CauseChain.of(failure)) {
            // AuthenticationException covers SaslAuthenticationException and SslAuthenticationException.
            if (current instanceof AuthenticationException
                    || current instanceof AuthorizationException
                    || current instanceof SSLHandshakeException) {
                return current;
            }
        }
        return null;
    }

    /**
     * Detects a {@link FencedInstanceIdException} anywhere in the cause chain — the broker
     * rejecting a poll/rebalance because another member already holds this consumer's static
     * {@code group.instance.id} (KIP-345 conflict, issue #146).
     */
    static boolean isFencedInstanceIdFailure(Throwable failure) {
        for (Throwable current : CauseChain.of(failure)) {
            if (current instanceof FencedInstanceIdException) {
                return true;
            }
        }
        return false;
    }
}
