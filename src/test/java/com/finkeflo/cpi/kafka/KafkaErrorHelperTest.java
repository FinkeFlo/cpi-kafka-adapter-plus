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
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.junit.Assert;
import org.junit.Test;

public class KafkaErrorHelperTest {

    @Test
    public void testWrapIfErrorPassesThroughException() {
        RuntimeException ex = new RuntimeException("test");
        Exception result = KafkaErrorHelper.wrapIfError(ex);
        Assert.assertSame(ex, result);
    }

    @Test
    public void testWrapIfErrorWrapsError() {
        Error error = new OutOfMemoryError("oom");
        Exception result = KafkaErrorHelper.wrapIfError(error);
        Assert.assertTrue(result instanceof RuntimeException);
        Assert.assertSame(error, result.getCause());
        Assert.assertTrue(result.getMessage().contains("OutOfMemoryError"));
    }

    @Test
    public void testIsFatalKafkaExceptionAuthentication() {
        Assert.assertTrue(KafkaErrorHelper.isFatalKafkaException(
                new AuthenticationException("auth")));
    }

    @Test
    public void testIsFatalKafkaExceptionAuthorization() {
        Assert.assertTrue(KafkaErrorHelper.isFatalKafkaException(
                new AuthorizationException("authz")));
    }

    @Test
    public void testIsFatalKafkaExceptionUnsupportedVersion() {
        Assert.assertTrue(KafkaErrorHelper.isFatalKafkaException(
                new UnsupportedVersionException("ver")));
    }

    @Test
    public void testIsFatalKafkaExceptionNonFatal() {
        Assert.assertFalse(KafkaErrorHelper.isFatalKafkaException(
                new RuntimeException("transient")));
    }

    @Test
    public void testIsFatalKafkaExceptionNull() {
        Assert.assertFalse(KafkaErrorHelper.isFatalKafkaException(null));
    }

    @Test
    public void testExtractKafkaCauseFindsNestedFatalException() {
        AuthenticationException authEx = new AuthenticationException("auth failed");
        RuntimeException wrapper = new RuntimeException("wrapper",
                new RuntimeException("mid", authEx));
        Throwable result = KafkaErrorHelper.extractKafkaCause(wrapper);
        Assert.assertSame(authEx, result);
    }

    @Test
    public void testExtractKafkaCauseReturnsDeepestCause() {
        RuntimeException inner = new RuntimeException("inner");
        RuntimeException outer = new RuntimeException("outer", inner);
        Throwable result = KafkaErrorHelper.extractKafkaCause(outer);
        Assert.assertSame(inner, result);
    }

    @Test
    public void testExtractKafkaCauseReturnsOriginalWhenNoCause() {
        RuntimeException ex = new RuntimeException("no cause");
        Throwable result = KafkaErrorHelper.extractKafkaCause(ex);
        Assert.assertSame(ex, result);
    }

    // ------------------------------------------------------------------
    //  isDefinitiveAuthOrTlsFailure
    // ------------------------------------------------------------------

    @Test
    public void testDefinitiveFailureSslAuthentication() {
        Assert.assertTrue(KafkaErrorHelper.isDefinitiveAuthOrTlsFailure(
                new SslAuthenticationException("SSL handshake failed")));
    }

    @Test
    public void testDefinitiveFailureSaslAuthentication() {
        Assert.assertTrue(KafkaErrorHelper.isDefinitiveAuthOrTlsFailure(
                new SaslAuthenticationException("Authentication failed")));
    }

    @Test
    public void testDefinitiveFailureNestedSslHandshake() {
        RuntimeException wrapper = new RuntimeException("outer",
                new RuntimeException("mid", new SSLHandshakeException("no cipher suites in common")));
        Assert.assertTrue(KafkaErrorHelper.isDefinitiveAuthOrTlsFailure(wrapper));
    }

    /**
     * A missing DESCRIBE permission says nothing about whether producing works, so it must not
     * short-circuit the send. This is the distinction between authentication and authorization.
     */
    @Test
    public void testDefinitiveFailureExcludesTopicAuthorization() {
        Assert.assertFalse(KafkaErrorHelper.isDefinitiveAuthOrTlsFailure(
                new TopicAuthorizationException("not authorized to describe")));
        Assert.assertFalse(KafkaErrorHelper.isDefinitiveAuthOrTlsFailure(
                new ClusterAuthorizationException("not authorized")));
    }

    @Test
    public void testDefinitiveFailureExcludesTimeout() {
        Assert.assertFalse(KafkaErrorHelper.isDefinitiveAuthOrTlsFailure(
                new TimeoutException("Topic x not present in metadata after 60000 ms")));
    }

    @Test
    public void testDefinitiveFailureNullAndSelfReferencingCause() {
        Assert.assertFalse(KafkaErrorHelper.isDefinitiveAuthOrTlsFailure(null));
        Assert.assertFalse(KafkaErrorHelper.isDefinitiveAuthOrTlsFailure(selfCausingException()));
    }

    /**
     * {@code initCause(this)} is rejected by the JVM, so a self-referencing chain can only be built
     * by overriding {@link Throwable#getCause()} — which some wrapper exceptions in the wild do.
     */
    private static RuntimeException selfCausingException() {
        return new RuntimeException("loop") {
            private static final long serialVersionUID = 1L;

            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };
    }

    // ------------------------------------------------------------------
    //  describeChain / describeTopStack
    // ------------------------------------------------------------------

    @Test
    public void testDescribeChainJoinsCauses() {
        RuntimeException ex = new RuntimeException("outer",
                new IllegalStateException("inner", new TimeoutException("timed out")));
        String chain = KafkaErrorHelper.describeChain(ex);
        Assert.assertEquals("RuntimeException: outer <- IllegalStateException: inner"
                + " <- TimeoutException: timed out", chain);
    }

    @Test
    public void testDescribeChainHandlesNullAndSelfReference() {
        Assert.assertEquals("null", KafkaErrorHelper.describeChain(null));
        String chain = KafkaErrorHelper.describeChain(selfCausingException());
        Assert.assertTrue(chain, chain.endsWith(": loop"));
        Assert.assertFalse("must not loop over the self-reference", chain.contains(" <- "));
    }

    @Test
    public void testDescribeTopStackEncodesFramesAndCauses() {
        String described = KafkaErrorHelper.describeTopStack(
                new RuntimeException("outer", new TimeoutException("inner")), 2);
        Assert.assertTrue(described, described.startsWith("RuntimeException('outer')["));
        Assert.assertTrue(described, described.contains(" CAUSED_BY TimeoutException('inner')"));
        Assert.assertEquals("null", KafkaErrorHelper.describeTopStack(null, 2));
    }

    // ------------------------------------------------------------------
    //  tlsMismatchHint
    // ------------------------------------------------------------------

    @Test
    public void testTlsMismatchHintOnlyForProtocolsWithoutTls() {
        Assert.assertNull(KafkaErrorHelper.tlsMismatchHint("SASL_SSL"));
        Assert.assertNull(KafkaErrorHelper.tlsMismatchHint("SSL"));
        Assert.assertNull(KafkaErrorHelper.tlsMismatchHint("sasl_ssl"));
        Assert.assertNull(KafkaErrorHelper.tlsMismatchHint(null));
        Assert.assertNull(KafkaErrorHelper.tlsMismatchHint("   "));

        String hint = KafkaErrorHelper.tlsMismatchHint("SASL_PLAINTEXT");
        Assert.assertNotNull(hint);
        Assert.assertTrue(hint, hint.contains("SASL_PLAINTEXT"));
        Assert.assertTrue(hint, hint.contains("SASL_SSL"));
        Assert.assertNotNull(KafkaErrorHelper.tlsMismatchHint("PLAINTEXT"));
    }
}
