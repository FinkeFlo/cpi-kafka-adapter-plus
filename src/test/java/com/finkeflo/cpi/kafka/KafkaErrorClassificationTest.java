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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.*;
import org.junit.Test;

public class KafkaErrorClassificationTest {
    @Test public void testRetriable() { assertEquals(KafkaErrorHelper.Classification.RETRIABLE, KafkaErrorHelper.classify(new TimeoutException("t"))); }
    @Test public void testProducerFenced() { assertEquals(KafkaErrorHelper.Classification.FATAL_PRODUCER_UNUSABLE, KafkaErrorHelper.classify(new ProducerFencedException("f"))); }
    @Test public void testAuth() { assertEquals(KafkaErrorHelper.Classification.FATAL_PRODUCER_UNUSABLE, KafkaErrorHelper.classify(new AuthenticationException("a"))); }
    @Test public void testRecordTooLarge() { assertEquals(KafkaErrorHelper.Classification.FATAL_DATA_ERROR, KafkaErrorHelper.classify(new RecordTooLargeException("r"))); }
    @Test public void testSerialization() { assertEquals(KafkaErrorHelper.Classification.FATAL_DATA_ERROR, KafkaErrorHelper.classify(new SerializationException("s"))); }
    @Test public void testUnknown() { assertEquals(KafkaErrorHelper.Classification.UNKNOWN_FATAL, KafkaErrorHelper.classify(new IllegalStateException("i"))); }
    @Test public void testNull() { assertEquals(KafkaErrorHelper.Classification.RETRIABLE, KafkaErrorHelper.classify(null)); }
    @Test public void testWrapped() { assertEquals(KafkaErrorHelper.Classification.RETRIABLE, KafkaErrorHelper.classify(new RuntimeException("o", new TimeoutException("i")))); }
    @Test public void testKafkaException() { assertEquals(KafkaErrorHelper.Classification.FATAL_PRODUCER_UNUSABLE, KafkaErrorHelper.classify(new KafkaException("k"))); }
    @Test public void testIsFatalBackcompat() { assertEquals(true, KafkaErrorHelper.isFatalKafkaException(new AuthenticationException("a"))); assertEquals(false, KafkaErrorHelper.isFatalKafkaException(new TimeoutException("t"))); }
}
