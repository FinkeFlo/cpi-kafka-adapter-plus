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

import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;
import org.osgi.framework.Bundle;

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
        Assert.assertTrue("first call must start the warm-up",
                BundleClassWarmup.ensureStarted(BundleClassWarmup.class, "test-1"));
        Assert.assertFalse("second call must be a no-op",
                BundleClassWarmup.ensureStarted(BundleClassWarmup.class, "test-2"));
        Assert.assertFalse("guard is per class space, not per trigger or anchor",
                BundleClassWarmup.ensureStarted(BundleClassWarmupTest.class, "test-1"));
        BundleClassWarmup.resetForTests();
        Assert.assertTrue("reset re-arms the guard (models a new bundle revision)",
                BundleClassWarmup.ensureStarted(BundleClassWarmup.class, "test-3"));
    }

    @Test
    public void describeClassSpaceWorksOutsideOsgi() {
        String description = OsgiBundleInfo.describeClassSpace(BundleClassWarmup.class);
        Assert.assertTrue(description, description.contains("bundleVersion=null"));
        Assert.assertTrue(description, description.contains("loader="));
    }
    @Test
    public void summarisePackagesTalliesPerPackageMostFrequentFirstAndCaps() {
        List<String> names = Arrays.asList(
                "a.b.C1", "a.b.C2", "a.b.C3",
                "x.y.z.K1", "x.y.z.K2",
                "m.N",
                "NoPackage");
        Assert.assertEquals("a.b:3,x.y.z:2,(default):1,m:1",
                BundleClassWarmup.summarisePackages(names, 20));
        Assert.assertEquals("a.b:3,x.y.z:2,…+2 packages",
                BundleClassWarmup.summarisePackages(names, 2));
        Assert.assertEquals("", BundleClassWarmup.summarisePackages(Arrays.<String>asList(), 20));
    }
    /**
     * A {@code Bundle} stub that reports the given state and, via {@code findEntries}, exposes the
     * anchor's real code source as bundle content (enough entries to cross the state-check interval).
     */
    private static OsgiBundleInfo bundleInState(int state) {
        Bundle bundle = (Bundle) Proxy.newProxyInstance(Bundle.class.getClassLoader(), new Class<?>[] {Bundle.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getState":
                            return state;
                        case "findEntries":
                            return Collections.enumeration(codeSourceClassUrls());
                        case "getHeaders":
                            return new java.util.Hashtable<String, String>();
                        default:
                            return null;
                    }
                });
        return OsgiBundleInfo.forTests(bundle, Bundle.class);
    }

    private static List<URL> codeSourceClassUrls() throws Exception {
        OsgiBundleInfo none = OsgiBundleInfo.of(BundleClassWarmupTest.class);
        BundleClassWarmup.Report report = new BundleClassWarmup.Report(null);
        List<URL> urls = new ArrayList<>();
        for (String name : BundleClassWarmup.enumerateClassNames(org.apache.kafka.clients.consumer.KafkaConsumer.class,
                none, report, false)) {
            urls.add(new URL("file:/" + name.replace('.', '/') + ".class"));
        }
        return urls;
    }

    @Test
    public void isStoppingOrStoppedOnlyForNonRunningBundleStates() {
        Assert.assertFalse(bundleInState(Bundle.STARTING).isStoppingOrStopped());
        Assert.assertFalse(bundleInState(Bundle.ACTIVE).isStoppingOrStopped());
        Assert.assertTrue(bundleInState(Bundle.STOPPING).isStoppingOrStopped());
        Assert.assertTrue(bundleInState(Bundle.RESOLVED).isStoppingOrStopped());
        Assert.assertTrue(bundleInState(Bundle.INSTALLED).isStoppingOrStopped());
        Assert.assertTrue(bundleInState(Bundle.UNINSTALLED).isStoppingOrStopped());
        Assert.assertFalse(OsgiBundleInfo.of(BundleClassWarmupTest.class).isStoppingOrStopped());
    }

    @Test
    public void warmAllClassesAbortsWhenBundleIsStopping() {
        BundleClassWarmup.Report report = BundleClassWarmup.warmAllClasses(BundleClassWarmup.class,
                bundleInState(Bundle.STOPPING));
        Assert.assertEquals("bundle-stopping", report.aborted);
        Assert.assertTrue("should stop after at most one state-check interval, attempted=" + report.attempted,
                report.attempted <= BundleClassWarmup.BUNDLE_STATE_CHECK_INTERVAL);
    }
}
