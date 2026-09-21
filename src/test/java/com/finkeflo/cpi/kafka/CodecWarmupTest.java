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

import java.util.Locale;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.internal.CompressionType;
import org.junit.Assert;
import org.junit.Test;

public class CodecWarmupTest {

    private static boolean zstdNativeAvailableHere() {
        // The bundle ships zstd-jni with the linux_amd64 classifier only (CPI's platform).
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        return os.contains("linux") && (arch.equals("amd64") || arch.equals("x86_64"));
    }

    @Test
    public void roundTripsEveryCodecWhoseNativeIsShippedForThisPlatform() {
        CodecWarmup.Result result = CodecWarmup.warmAll();

        Assert.assertEquals("ok", result.outcomes.get(CompressionType.GZIP.name));
        Assert.assertEquals("ok", result.outcomes.get(CompressionType.SNAPPY.name));
        Assert.assertEquals("ok", result.outcomes.get(CompressionType.LZ4.name));
        if (zstdNativeAvailableHere()) {
            Assert.assertEquals("ok", result.outcomes.get(CompressionType.ZSTD.name));
        } else {
            Assert.assertNotNull(result.outcomes.get(CompressionType.ZSTD.name));
        }
        Assert.assertFalse(result.outcomes.containsKey(CompressionType.NONE.name));
        Assert.assertTrue(result.summary(), result.summary().contains("snappy:ok"));
    }

    @Test
    public void roundTripNeverThrows() {
        // Whatever the platform, a codec failure is reported as text, not propagated.
        for (CompressionType type : CompressionType.values()) {
            String outcome = CodecWarmup.roundTrip(type);
            Assert.assertNotNull(outcome);
            Assert.assertFalse(outcome.isEmpty());
        }
    }

    @Test
    public void classSpaceFaultSignatureCoversNativeLoaderErrorsAndWrappedLinkageErrors() {
        // snappy-java's SnappyError extends Error directly (no LinkageError in the chain).
        Error snappyLike = new Error("[FAILED_TO_LOAD_NATIVE_LIBRARY] no native library is found") {};
        Assert.assertTrue(ClassSpaceFaults.hasFaultSignature(snappyLike));

        KafkaException wrapped = new KafkaException("Received exception when fetching the next record",
                new NoClassDefFoundError("Could not initialize class org.xerial.snappy.Snappy"));
        Assert.assertTrue(ClassSpaceFaults.hasFaultSignature(wrapped));

        Assert.assertTrue(ClassSpaceFaults.hasFaultSignature(
                new RuntimeException(new ClassNotFoundException("x"))));
        Assert.assertTrue(ClassSpaceFaults.hasFaultSignature(new UnsatisfiedLinkError("libx.so")));

        Assert.assertFalse(ClassSpaceFaults.hasFaultSignature(new KafkaException("timeout")));
        Assert.assertFalse(ClassSpaceFaults.hasFaultSignature(new OutOfMemoryError("heap")));
        Assert.assertFalse(ClassSpaceFaults.hasFaultSignature(new RuntimeException("plain")));
    }

    @Test
    public void classSpaceStalenessIsUnknownOutsideOsgi() {
        Assert.assertNull(OsgiBundleInfo.isClassSpaceStale(CodecWarmupTest.class));
        Assert.assertTrue(OsgiBundleInfo.describeClassSpace(CodecWarmupTest.class).contains("stale=null"));
    }
}
