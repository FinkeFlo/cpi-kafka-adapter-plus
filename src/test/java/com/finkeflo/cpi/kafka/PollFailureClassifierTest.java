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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.Assert;
import org.junit.Test;

import com.finkeflo.cpi.kafka.PollFailureClassifier.Kind;

/**
 * {@link PollFailureClassifier} without a consumer: the kind of each failure, the order in which the
 * kinds win, and the cause-chain walks behind them. What the emit poll and the keep-alive poll do
 * with each kind is pinned in {@link ConsumerPollFailureTest}.
 */
public class PollFailureClassifierTest {

    /** Outside OSGi no bundle revision is ever replaced, so only the wiring text marks a gone class space. */
    private static final Class<?> ROUTE = CpiKafkaPlusConsumer.class;

    /** The text OSGi puts into a class load over the purged revision of an updated adapter. */
    private static final String WIRING_GONE = "Unable to load class "
            + "'org.apache.kafka.clients.consumer.internals.ConsumerCoordinator$3' because the bundle wiring "
            + "for com.finkeflo.cpi.kafka.cpi-kafka-adapter-plus is no longer valid.";

    // --- classify: one kind per failure ---

    @Test
    public void rejectedCredentialsAreNonRetryable() {
        assertKind(Kind.NON_RETRYABLE, new SaslAuthenticationException("bad credentials"));
        assertKind(Kind.NON_RETRYABLE, new KafkaException("wrapped", new SSLHandshakeException("handshake failed")));
    }

    @Test
    public void aFencedInstanceIsFenced() {
        assertKind(Kind.FENCED, new FencedInstanceIdException("fenced"));
        assertKind(Kind.FENCED, new KafkaException("wrapped", new FencedInstanceIdException("fenced")));
    }

    @Test
    public void aGoneClassSpaceIsClassSpaceGone() {
        NoClassDefFoundError failure = new NoClassDefFoundError(
                "org/apache/kafka/clients/consumer/internals/ConsumerCoordinator$3");
        failure.initCause(new ClassNotFoundException(WIRING_GONE));

        assertKind(Kind.CLASS_SPACE_GONE, failure);
        assertKind(Kind.CLASS_SPACE_GONE, new KafkaException("Unexpected error in join group response",
                new ClassNotFoundException(WIRING_GONE)));
    }

    @Test
    public void anyOtherFailureIsOther() {
        assertKind(Kind.OTHER, new TimeoutException("broker timeout"));
        assertKind(Kind.OTHER, new KafkaException("wrapped", new RuntimeException("unknown")));
    }

    @Test
    public void aClassLoadingFaultWithoutTheWiringTextIsOtherOnACurrentClassSpace() {
        assertKind(Kind.OTHER, new NoClassDefFoundError("org/example/Missing"));
        assertKind(Kind.OTHER, new KafkaException("wrapped", new ClassNotFoundException("org.example.DoesNotExist")));
    }

    @Test
    public void noFailureIsOther() {
        assertKind(Kind.OTHER, null);
    }

    @Test(timeout = 10_000)
    public void aSelfCausedFailureIsOther() {
        assertKind(Kind.OTHER, new SelfCausedException());
    }

    // --- classify: a failure of more than one kind; the order of Kind wins, not the place in the chain ---

    @Test
    public void rejectedCredentialsWinOverAFencedInstance() {
        assertKind(Kind.NON_RETRYABLE, new SaslAuthenticationException("bad credentials",
                new FencedInstanceIdException("fenced")));
        assertKind(Kind.NON_RETRYABLE, new FencedInstanceIdException("fenced",
                new SaslAuthenticationException("bad credentials")));
    }

    @Test
    public void rejectedCredentialsWinOverAGoneClassSpace() {
        assertKind(Kind.NON_RETRYABLE, new SslAuthenticationException("SSL handshake failed",
                new ClassNotFoundException(WIRING_GONE)));
        assertKind(Kind.NON_RETRYABLE, new ClassNotFoundException(WIRING_GONE,
                new SslAuthenticationException("SSL handshake failed")));
    }

    @Test
    public void aFencedInstanceWinsOverAGoneClassSpace() {
        assertKind(Kind.FENCED, new FencedInstanceIdException("fenced", new ClassNotFoundException(WIRING_GONE)));
        assertKind(Kind.FENCED, new ClassNotFoundException(WIRING_GONE, new FencedInstanceIdException("fenced")));
    }

    // --- the cause the consumer's policy error names ---

    @Test
    public void theNonRetryableCauseIsTheOutermostMatch() {
        SaslAuthenticationException outer = new SaslAuthenticationException("outer",
                new SslAuthenticationException("inner"));

        Assert.assertSame(outer, PollFailureClassifier.findNonRetryablePollFailureCause(
                new KafkaException("wrapped", outer)));
    }

    @Test
    public void anyOtherFailureHasNoNonRetryableCause() {
        Assert.assertNull(PollFailureClassifier.findNonRetryablePollFailureCause(
                new KafkaException("wrapped", new FencedInstanceIdException("fenced"))));
        Assert.assertNull(PollFailureClassifier.findNonRetryablePollFailureCause(null));
    }

    // --- isNonRetryablePollFailure ---

    @Test
    public void testIsNonRetryablePollFailureWithSaslAuthenticationException() {
        Assert.assertTrue(PollFailureClassifier.isNonRetryablePollFailure(
                new SaslAuthenticationException("bad credentials")));
    }

    @Test
    public void testIsNonRetryablePollFailureWithTopicAuthorizationException() {
        Assert.assertTrue(PollFailureClassifier.isNonRetryablePollFailure(
                new TopicAuthorizationException(java.util.Collections.singleton("topic-a"))));
    }

    @Test
    public void testIsNonRetryablePollFailureWithSslHandshakeCause() {
        Assert.assertTrue(PollFailureClassifier.isNonRetryablePollFailure(
                new RuntimeException("wrapped", new SSLHandshakeException("handshake failed"))));
    }

    @Test
    public void testIsNonRetryablePollFailureWithWrappedAuthenticationCause() {
        Assert.assertTrue(PollFailureClassifier.isNonRetryablePollFailure(
                new RuntimeException("wrapped",
                        new IllegalStateException("mid",
                                new SaslAuthenticationException("auth failed")))));
    }

    @Test
    public void testIsNonRetryablePollFailureWithPlainAuthenticationException() {
        Assert.assertTrue(PollFailureClassifier.isNonRetryablePollFailure(
                new AuthenticationException("authentication failed")));
    }

    @Test
    public void testIsNonRetryablePollFailureWithSslAuthenticationException() {
        Assert.assertTrue(PollFailureClassifier.isNonRetryablePollFailure(
                new SslAuthenticationException("SSL handshake failed")));
    }

    @Test
    public void testIsNonRetryablePollFailureWithGroupAuthorizationException() {
        Assert.assertTrue(PollFailureClassifier.isNonRetryablePollFailure(
                new GroupAuthorizationException("Not authorized to access group: group-a")));
    }

    @Test(timeout = 10_000)
    public void testIsNonRetryablePollFailureStopsAtASelfCausedThrowable() {
        Assert.assertFalse(PollFailureClassifier.isNonRetryablePollFailure(new SelfCausedException()));
    }

    @Test
    public void testIsNonRetryablePollFailureWithNull() {
        Assert.assertFalse(PollFailureClassifier.isNonRetryablePollFailure(null));
    }

    @Test
    public void testIsNonRetryablePollFailureWithTimeoutExceptionDefaultsToRetryable() {
        Assert.assertFalse(PollFailureClassifier.isNonRetryablePollFailure(
                new TimeoutException("broker timeout")));
    }

    @Test
    public void testIsNonRetryablePollFailureWithUnknownExceptionDefaultsToRetryable() {
        Assert.assertFalse(PollFailureClassifier.isNonRetryablePollFailure(
                new RuntimeException("unknown")));
    }

    // --- isFencedInstanceIdFailure ---

    @Test
    public void testIsFencedInstanceIdFailureWithDirectException() {
        Assert.assertTrue(PollFailureClassifier.isFencedInstanceIdFailure(
                new org.apache.kafka.common.errors.FencedInstanceIdException(
                        "The broker rejected this static consumer since another consumer with the same "
                                + "group.instance.id has registered with a different member.id.")));
    }

    @Test
    public void testIsFencedInstanceIdFailureWithWrappedCause() {
        Assert.assertTrue(PollFailureClassifier.isFencedInstanceIdFailure(
                new RuntimeException("wrapped",
                        new org.apache.kafka.common.errors.FencedInstanceIdException("fenced"))));
    }

    @Test
    public void testIsFencedInstanceIdFailureFalseForUnrelatedException() {
        Assert.assertFalse(PollFailureClassifier.isFencedInstanceIdFailure(
                new RuntimeException("unrelated")));
    }

    @Test(timeout = 10_000)
    public void testIsFencedInstanceIdFailureStopsAtASelfCausedThrowable() {
        Assert.assertFalse(PollFailureClassifier.isFencedInstanceIdFailure(new SelfCausedException()));
    }

    @Test
    public void testIsFencedInstanceIdFailureWithNull() {
        Assert.assertFalse(PollFailureClassifier.isFencedInstanceIdFailure(null));
    }

    private static void assertKind(Kind expected, Throwable failure) {
        Assert.assertEquals(String.valueOf(failure), expected, PollFailureClassifier.classify(failure, ROUTE));
    }

    /** Reports itself as its own cause; a walk along the cause chain has to stop there. */
    private static final class SelfCausedException extends RuntimeException {
        SelfCausedException() {
            super("self-caused");
        }

        @Override
        public synchronized Throwable getCause() {
            return this;
        }
    }
}
