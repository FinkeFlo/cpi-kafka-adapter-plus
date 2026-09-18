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

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

public class BundleClassWarmupTest {

    @Test
    public void addIfClassConvertsPathsAndSkipsDescriptors() {
        TreeSet<String> names = new TreeSet<>();
        BundleClassWarmup.addIfClass(names, "org/apache/kafka/common/compress/Lz4Compression$Builder.class");
        BundleClassWarmup.addIfClass(names, "META-INF/versions/9/module-info.class");
        BundleClassWarmup.addIfClass(names, "META-INF/versions/11/com/foo/Bar.class");
        BundleClassWarmup.addIfClass(names, "module-info.class");
        BundleClassWarmup.addIfClass(names, "org/foo/notaclass.txt");
        Assert.assertEquals(
                new TreeSet<>(Arrays.asList("org.apache.kafka.common.compress.Lz4Compression$Builder")), names);
    }

    @Test
    public void bundleClassPathEntriesSkipsRootAndAttributes() {
        List<String> entries = BundleClassWarmup.bundleClassPathEntries(
                ".,lz4-java-1.8.0.jar,/zstd-jni-1.5.7-12.jar;foo=bar, snappy-java-1.1.10.8.jar");
        Assert.assertEquals(Arrays.asList("lz4-java-1.8.0.jar", "zstd-jni-1.5.7-12.jar", "snappy-java-1.1.10.8.jar"),
                entries);
        Assert.assertTrue(BundleClassWarmup.bundleClassPathEntries(null).isEmpty());
    }

    @Test
    public void priorityRankPutsKafkaCodecsFirst() {
        int compress = BundleClassWarmup.priorityRank("org.apache.kafka.common.compress.ZstdCompression");
        int consumer = BundleClassWarmup.priorityRank("org.apache.kafka.clients.consumer.CloseOptions");
        int record = BundleClassWarmup.priorityRank("org.apache.kafka.common.record.DefaultRecordBatch");
        int other = BundleClassWarmup.priorityRank("com.fasterxml.jackson.databind.ObjectMapper");
        Assert.assertTrue(compress < consumer);
        Assert.assertTrue(consumer < record);
        Assert.assertTrue(record < other);
        Assert.assertEquals(BundleClassWarmup.PRIORITY_PREFIXES.length, other);
    }

    @Test
    public void stage1LoadsAllAdapterClassesFromClassesDirectory() {
        OsgiBundleInfo bundle = OsgiBundleInfo.of(BundleClassWarmup.class);
        Assert.assertFalse("unit tests run outside OSGi", bundle.isOsgi());

        BundleClassWarmup.Report report = BundleClassWarmup.warmAdapterClasses(BundleClassWarmup.class, bundle);

        Assert.assertEquals("directory", report.source);
        Assert.assertTrue("expected the adapter's own classes to be enumerated, got " + report.attempted,
                report.attempted > 20);
        Assert.assertEquals("adapter classes must all load and initialise: " + report.failures,
                0, report.failures.size());
        Assert.assertEquals(report.attempted, report.loaded);
    }

    @Test
    public void stage2CoversKafkaClientsAndReportsFailuresByName() {
        OsgiBundleInfo bundle = OsgiBundleInfo.of(BundleClassWarmup.class);
        BundleClassWarmup.Report report = BundleClassWarmup.warmAllClasses(BundleClassWarmup.class, bundle);

        // kafka-clients is unpacked into target/classes, so the observed cold-path classes must be
        // part of the enumerated set and loadable on the test class path.
        TreeSet<String> names = BundleClassWarmup.enumerateClassNames(BundleClassWarmup.class, bundle,
                new BundleClassWarmup.Report(null), true);
        Assert.assertTrue(names.contains("org.apache.kafka.common.compress.Lz4Compression$Builder"));
        Assert.assertTrue(names.contains("org.apache.kafka.clients.consumer.CloseOptions"));
        Assert.assertFalse(report.failures.containsKey("org.apache.kafka.common.compress.Lz4Compression$Builder"));
        Assert.assertFalse(report.failures.containsKey("org.apache.kafka.clients.consumer.CloseOptions"));
        Assert.assertNull(report.aborted);
        Assert.assertTrue(report.loaded > 1000);
    }

    @Test
    public void ensureStartedIsIdempotentPerClassSpace() {
        BundleClassWarmup.resetForTests();
        BundleClassWarmup.ensureStarted(BundleClassWarmup.class, "test-1");
        BundleClassWarmup.ensureStarted(BundleClassWarmup.class, "test-2");
        // No assertion beyond "does not throw": the once-guard is a static AtomicBoolean.
    }

    @Test
    public void describeClassSpaceWorksOutsideOsgi() {
        String description = OsgiBundleInfo.describeClassSpace(BundleClassWarmup.class);
        Assert.assertTrue(description, description.contains("bundleVersion=null"));
        Assert.assertTrue(description, description.contains("loader="));
    }
}
