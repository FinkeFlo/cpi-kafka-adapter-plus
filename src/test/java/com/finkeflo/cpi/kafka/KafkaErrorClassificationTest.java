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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.*;
import org.junit.Test;

/**
 * Tests for {@link KafkaErrorHelper.Classification} and the classify methods.
 *
 * <p>Some tests explicitly pin the Kafka exception class hierarchy. If a future kafka-clients
 * upgrade reshuffles the hierarchy, these tests must fail loudly rather than the classifier
 * quietly degrading — the same reasoning as the AdapterTraceMessageType enum constant tests.
 */
public class KafkaErrorClassificationTest {

    // --- Basic classification tests ---

    @Test
    public void testRetriable() {
        assertEquals(KafkaErrorHelper.Classification.RETRIABLE,
                KafkaErrorHelper.classify(new TimeoutException("t")));
    }

    @Test
    public void testProducerFenced() {
        assertEquals(KafkaErrorHelper.Classification.FATAL_PRODUCER_UNUSABLE,
                KafkaErrorHelper.classify(new ProducerFencedException("f")));
    }

    @Test
    public void testAuth() {
        assertEquals(KafkaErrorHelper.Classification.FATAL_PRODUCER_UNUSABLE,
                KafkaErrorHelper.classify(new AuthenticationException("a")));
    }

    @Test
    public void testRecordTooLarge() {
        assertEquals(KafkaErrorHelper.Classification.FATAL_DATA_ERROR,
                KafkaErrorHelper.classify(new RecordTooLargeException("r")));
    }

    @Test
    public void testSerialization() {
        assertEquals(KafkaErrorHelper.Classification.FATAL_DATA_ERROR,
                KafkaErrorHelper.classify(new SerializationException("s")));
    }

    @Test
    public void testUnknown() {
        // Non-Kafka RuntimeException is UNKNOWN_FATAL — this was the blind spot in the production incident
        assertEquals(KafkaErrorHelper.Classification.UNKNOWN_FATAL,
                KafkaErrorHelper.classify(new IllegalStateException("i")));
    }

    @Test
    public void testNull() {
        // Null is UNKNOWN_FATAL — the safe direction is toward caution, not a retry loop
        assertEquals(KafkaErrorHelper.Classification.UNKNOWN_FATAL,
                KafkaErrorHelper.classify(null));
    }

    @Test
    public void testWrapped() {
        // A retriable exception wrapped in a RuntimeException should still be RETRIABLE
        assertEquals(KafkaErrorHelper.Classification.RETRIABLE,
                KafkaErrorHelper.classify(new RuntimeException("o", new TimeoutException("i"))));
    }

    @Test
    public void testUnrecognisedKafkaExceptionIsUnknown() {
        // An unrecognised KafkaException is UNKNOWN_FATAL, NOT FATAL_PRODUCER_UNUSABLE.
        // Returning FATAL_PRODUCER_UNUSABLE would trigger unnecessary producer rebuilds
        // and destroy the information that we do not know.
        assertEquals(KafkaErrorHelper.Classification.UNKNOWN_FATAL,
                KafkaErrorHelper.classify(new KafkaException("k")));
    }

    @Test
    public void testIsFatalBackcompat() {
        // isFatalKafkaException is deprecated but must still work for backwards compatibility
        assertEquals(true, KafkaErrorHelper.isFatalKafkaException(new AuthenticationException("a")));
        assertEquals(false, KafkaErrorHelper.isFatalKafkaException(new TimeoutException("t")));
    }

    // --- Tests that pin the Kafka exception class hierarchy ---
    // If kafka-clients changes its hierarchy, these tests must fail loudly.

    @Test
    public void pinProducerFencedExtendsApplicationRecoverable() {
        // ProducerFencedException extends ApplicationRecoverableException (Kafka 4.x)
        assertTrue("ProducerFencedException must extend ApplicationRecoverableException",
                ApplicationRecoverableException.class.isAssignableFrom(ProducerFencedException.class));
    }

    @Test
    public void pinInvalidPidMappingExtendsApplicationRecoverable() {
        // InvalidPidMappingException extends ApplicationRecoverableException (Kafka 4.x)
        assertTrue("InvalidPidMappingException must extend ApplicationRecoverableException",
                ApplicationRecoverableException.class.isAssignableFrom(InvalidPidMappingException.class));
    }

    @Test
    public void pinUnknownProducerIdExtendsOutOfOrderSequence() {
        // UnknownProducerIdException extends OutOfOrderSequenceException
        // This is an inheritance detail, but we rely on it — pin it explicitly
        assertTrue("UnknownProducerIdException must extend OutOfOrderSequenceException",
                OutOfOrderSequenceException.class.isAssignableFrom(UnknownProducerIdException.class));
    }

    @Test
    public void pinRetriableExceptionIsBase() {
        // TimeoutException, NetworkException extend RetriableException
        assertTrue("TimeoutException must extend RetriableException",
                RetriableException.class.isAssignableFrom(TimeoutException.class));
        assertTrue("NetworkException must extend RetriableException",
                RetriableException.class.isAssignableFrom(NetworkException.class));
    }

    // --- UnknownProducerIdException classification (relies on hierarchy) ---

    @Test
    public void testUnknownProducerIdIsFatal() {
        // UnknownProducerIdException extends OutOfOrderSequenceException and must be FATAL_PRODUCER_UNUSABLE
        assertEquals(KafkaErrorHelper.Classification.FATAL_PRODUCER_UNUSABLE,
                KafkaErrorHelper.classify(new UnknownProducerIdException("u")));
    }

    @Test
    public void testInvalidPidMappingIsFatal() {
        // InvalidPidMappingException extends ApplicationRecoverableException and must be FATAL_PRODUCER_UNUSABLE
        assertEquals(KafkaErrorHelper.Classification.FATAL_PRODUCER_UNUSABLE,
                KafkaErrorHelper.classify(new InvalidPidMappingException("p")));
    }

    // --- extractKafkaCause stopping behaviour ---

    @Test
    public void testExtractKafkaCauseStopsAtFatalException() {
        // extractKafkaCause stops early when it finds a fatal exception
        AuthenticationException authEx = new AuthenticationException("auth failed");
        RuntimeException deeperCause = new RuntimeException("deeper");
        RuntimeException midWrapper = new RuntimeException("mid", authEx);
        // Build chain: outer -> mid -> authEx; authEx is set to have deeperCause
        // But extractKafkaCause should stop at authEx because it's fatal
        
        // Create: outer -> mid -> authEx -> deeper
        RuntimeException outer = new RuntimeException("outer", 
                new RuntimeException("mid", 
                        new AuthenticationException("auth", deeperCause)));
        
        Throwable result = KafkaErrorHelper.extractKafkaCause(outer);
        // Should stop at AuthenticationException, not go deeper
        assertTrue("Should stop at AuthenticationException",
                result instanceof AuthenticationException);
    }

    @Test
    public void testExtractKafkaCauseStopsAtProducerFenced() {
        // extractKafkaCause now stops at ProducerFencedException (new behaviour)
        ProducerFencedException fencedEx = new ProducerFencedException("fenced");
        RuntimeException deeperCause = new RuntimeException("deeper");
        
        RuntimeException outer = new RuntimeException("outer",
                new RuntimeException("mid", fencedEx));
        
        Throwable result = KafkaErrorHelper.extractKafkaCause(outer);
        assertTrue("Should stop at ProducerFencedException",
                result instanceof ProducerFencedException);
    }
}
