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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
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
}
