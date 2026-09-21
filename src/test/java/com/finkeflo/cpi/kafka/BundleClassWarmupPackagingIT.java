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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;

import org.junit.Assert;
import org.junit.Test;

/**
 * Packaging guard for the class warm-up (issue #148): every class shipped in the built bundle —
 * root content and every {@code Bundle-ClassPath} jar — must be enumerated by
 * {@link BundleClassWarmup}, and the classes that failed cold in production must load from the
 * jar. Failures are only tolerated for classes whose optional dependencies are intentionally
 * absent from the bundle.
 */
public class BundleClassWarmupPackagingIT {

    /**
     * Class-name prefixes allowed to fail loading from the packaged jar. Each one references an
     * optional dependency that is neither embedded nor on CPI (see pom Import-Package). Growth of
     * this list means new optional dependencies were pulled in — review before extending.
     */
    private static final List<String> TOLERATED_FAILURE_PREFIXES = Arrays.asList(
            "org.apache.kafka.shaded.io.opentelemetry.proto.collector.", // io.grpc stubs (44 classes)
            "org.apache.kafka.common.security.oauthbearer.internals.secured.", // org.jose4j (4 classes)
            "com.networknt.schema.regex.",            // org.joni / org.graalvm (2 classes)
            // org.objectweb.asm (11 classes). Passes locally because asm is on the test classpath,
            // fails on CPI where the bundle class space has no asm (observed: 61 failures total).
            "org.apache.commons.compress.harmony.pack200."
    );

    @Test
    public void everyPackagedClassIsEnumeratedAndColdPathClassesLoad() throws Exception {
        File jar = locateBundleJar();

        // Independent walk (plain JarFile/JarInputStream, no shared code) as the reference set.
        TreeSet<String> reference = new TreeSet<>();
        List<String> nestedJars = new ArrayList<>();
        try (JarFile jf = new JarFile(jar)) {
            String bcp = jf.getManifest().getMainAttributes().getValue("Bundle-ClassPath");
            Assert.assertNotNull("Bundle-ClassPath missing from manifest", bcp);
            for (String clause : bcp.split(",")) {
                String p = clause.split(";")[0].trim();
                if (!p.isEmpty() && !".".equals(p)) {
                    nestedJars.add(p);
                }
            }
            Enumeration<JarEntry> entries = jf.entries();
            while (entries.hasMoreElements()) {
                JarEntry e = entries.nextElement();
                collect(reference, e.getName());
                if (nestedJars.contains(e.getName())) {
                    try (JarInputStream jin = new JarInputStream(jf.getInputStream(e))) {
                        JarEntry n;
                        while ((n = jin.getNextJarEntry()) != null) {
                            collect(reference, n.getName());
                        }
                    }
                }
            }
        }
        Assert.assertTrue("expected the codec jars on Bundle-ClassPath, got " + nestedJars, nestedJars.size() >= 3);
        Assert.assertTrue("suspiciously few classes: " + reference.size(), reference.size() > 5000);

        TreeSet<String> enumerated = new TreeSet<>();
        BundleClassWarmup.enumerateJarFile(jar, enumerated, true);
        Assert.assertEquals("warm-up enumeration must cover every packaged class", reference, enumerated);

        for (String mustHave : Arrays.asList(
                "org.apache.kafka.common.compress.Lz4Compression$Builder",
                "org.apache.kafka.common.compress.ZstdCompression",
                "org.apache.kafka.common.compress.SnappyCompression",
                "org.apache.kafka.clients.consumer.CloseOptions",
                "net.jpountz.lz4.LZ4Factory",
                "com.github.luben.zstd.ZstdInputStreamNoFinalizer",
                "org.xerial.snappy.Snappy",
                "com.finkeflo.cpi.kafka.CpiKafkaPlusConsumer")) {
            Assert.assertTrue("missing from bundle: " + mustHave, enumerated.contains(mustHave));
        }

        // Load everything from the jar itself (child-first; the test class path only backs
        // platform-provided packages such as Camel, SLF4J and the SAP APIs).
        Path tmp = Files.createTempDirectory("warmup-it");
        List<URL> urls = new ArrayList<>();
        urls.add(jar.toURI().toURL());
        try (JarFile jf = new JarFile(jar)) {
            for (String nested : nestedJars) {
                JarEntry e = jf.getJarEntry(nested);
                Assert.assertNotNull("Bundle-ClassPath entry missing in jar: " + nested, e);
                Path out = tmp.resolve(new File(nested).getName());
                try (InputStream in = jf.getInputStream(e)) {
                    Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING);
                }
                urls.add(out.toUri().toURL());
            }
        }

        Map<String, String> failures = new TreeMap<>();
        int loaded = 0;
        try (ChildFirstClassLoader loader = new ChildFirstClassLoader(urls.toArray(new URL[0]), getClass().getClassLoader())) {
            for (String name : enumerated) {
                try {
                    Class.forName(name, false, loader);
                    loaded++;
                } catch (Throwable t) {
                    failures.put(name, t.getClass().getSimpleName() + ": " + t.getMessage());
                }
            }
        }
        Assert.assertTrue("too few classes loaded: " + loaded, loaded > 5000);

        // Build artifact for review: which packaged classes cannot load and why.
        List<String> lines = new ArrayList<>();
        lines.add("enumerated=" + enumerated.size() + " loaded=" + loaded + " failed=" + failures.size());
        failures.forEach((k, v) -> lines.add(k + " [" + v + "]"));
        Files.write(new File("target", "warmup-it-failures.txt").toPath(), lines);

        List<String> unexpected = new ArrayList<>();
        for (Map.Entry<String, String> f : failures.entrySet()) {
            if (TOLERATED_FAILURE_PREFIXES.stream().noneMatch(f.getKey()::startsWith)) {
                unexpected.add(f.getKey() + " [" + f.getValue() + "]");
            }
        }
        Assert.assertTrue("classes failed to load outside the tolerated set:\n  " + String.join("\n  ", unexpected),
                unexpected.isEmpty());
    }

    private static void collect(TreeSet<String> names, String path) {
        if (!path.endsWith(".class") || path.startsWith("META-INF/") || path.endsWith("module-info.class")) {
            return;
        }
        names.add(path.substring(0, path.length() - ".class".length()).replace('/', '.'));
    }

    private static File locateBundleJar() {
        String finalName = System.getProperty("project.build.finalName");
        Assert.assertNotNull("project.build.finalName not set (run via failsafe)", finalName);
        File jar = new File("target", finalName + ".jar");
        Assert.assertTrue("bundle jar not built: " + jar, jar.isFile());
        return jar;
    }

    /** Loads from its own URLs first, falling back to the parent only for classes absent from the jar. */
    private static final class ChildFirstClassLoader extends URLClassLoader {
        ChildFirstClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> c = findLoadedClass(name);
                if (c == null) {
                    try {
                        c = findClass(name);
                    } catch (ClassNotFoundException e) {
                        return super.loadClass(name, resolve);
                    }
                }
                if (resolve) {
                    resolveClass(c);
                }
                return c;
            }
        }
    }
}
