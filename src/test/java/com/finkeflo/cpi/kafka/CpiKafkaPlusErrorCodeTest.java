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

import static org.junit.Assert.*;
import org.apache.kafka.common.errors.*;
import org.junit.Test;

public class CpiKafkaPlusErrorCodeTest {
    @Test public void testPrefixes() { for (CpiKafkaPlusErrorCode c : CpiKafkaPlusErrorCode.values()) assertTrue(c.code().startsWith("KP-")); }
    @Test public void testTimeout() { assertEquals(CpiKafkaPlusErrorCode.KP_PROD_002, CpiKafkaPlusErrorCode.fromThrowable(new TimeoutException("t"))); }
    @Test public void testMetadataTimeout() { assertEquals(CpiKafkaPlusErrorCode.KP_META_001, CpiKafkaPlusErrorCode.fromThrowable(new TimeoutException("metadata timeout"))); }
    @Test public void testRecordTooLarge() { assertEquals(CpiKafkaPlusErrorCode.KP_PROD_003, CpiKafkaPlusErrorCode.fromThrowable(new RecordTooLargeException("r"))); }
    @Test public void testFenced() { assertEquals(CpiKafkaPlusErrorCode.KP_TXN_001, CpiKafkaPlusErrorCode.fromThrowable(new ProducerFencedException("f"))); }
    @Test public void testAuth() { assertEquals(CpiKafkaPlusErrorCode.KP_SEC_001, CpiKafkaPlusErrorCode.fromThrowable(new AuthenticationException("a"))); }
    @Test public void testNull() { assertEquals(CpiKafkaPlusErrorCode.KP_GEN_001, CpiKafkaPlusErrorCode.fromThrowable(null)); }
    @Test public void testDlq() { assertEquals(CpiKafkaPlusErrorCode.KP_DLQ_001, CpiKafkaPlusErrorCode.fromSendFailure(new TimeoutException("t"), true)); }
    @Test public void testInitGeneric() { assertEquals(CpiKafkaPlusErrorCode.KP_PROD_001, CpiKafkaPlusErrorCode.fromProducerInitFailure(new RuntimeException("r"))); }
    @Test public void testInitSpecific() { assertEquals(CpiKafkaPlusErrorCode.KP_SEC_001, CpiKafkaPlusErrorCode.fromProducerInitFailure(new AuthenticationException("a"))); }
    @Test public void testJsonSchema() { assertEquals(CpiKafkaPlusErrorCode.KP_CFG_002, CpiKafkaPlusErrorCode.fromJsonSchemaValidationFailure(new RuntimeException("r"))); }
    @Test public void testUnique() { CpiKafkaPlusErrorCode[] v = CpiKafkaPlusErrorCode.values(); for (int i=0;i<v.length;i++) for (int j=i+1;j<v.length;j++) assertNotEquals(v[i].code(), v[j].code()); }
}
