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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pre-loads every class the adapter bundle can ever need, so that a later CPI adapter update
 * cannot break the still-running routes (issue #148).
 *
 * <p>Background: when a new adapter version is deployed, CPI/Aries updates the subsystem and
 * purges the <em>old</em> bundle revision immediately — while the iFlow routes created from that
 * revision keep running on its class loader. Any class that has not been loaded yet at that
 * moment fails with {@code NoClassDefFoundError}/{@code ClassNotFoundException … bundle wiring …
 * no longer valid}. Nothing inside the old revision can recover from that; the only structural
 * defence is to leave no class un-loaded. Kafka's compression codecs, the consumer close path
 * ({@code CloseOptions}) and rebalance internals are the usual first-time loads that trip this.
 *
 * <p>Two stages, both idempotent per class space (the guard is a {@code static} and therefore
 * automatically per bundle revision):
 * <ol>
 *   <li><b>Synchronous</b>: all {@code com.finkeflo.cpi.kafka.*} classes, initialised. Cheap
 *       (~70 classes) and covers the adapter's own stop/error paths. Followed by one
 *       compress/decompress round trip per Kafka codec ({@link CodecWarmup}): snappy, zstd and
 *       lz4 extract their native library from the bundle jar via {@code getResourceAsStream} on
 *       first use — a <em>resource</em> lookup that class loading alone does not cover and that
 *       fails just the same on a purged revision (verified on DEV: {@code SnappyError
 *       FAILED_TO_LOAD_NATIVE_LIBRARY} on the first snappy batch after an update).</li>
 *   <li><b>Background daemon thread</b>: every {@code .class} in the bundle root and in every
 *       {@code Bundle-ClassPath} jar, loaded without initialisation. Kafka codec, consumer and
 *       record packages go first. Failures are expected for classes whose optional dependencies
 *       are not present on CPI; they are logged by name so the set can be reviewed.</li>
 * </ol>
 *
 * <p>Runs equally outside OSGi (unit/integration tests) by walking the code source of the anchor
 * class — a jar (including its nested {@code Bundle-ClassPath} jars) or a classes directory.
 */
final class BundleClassWarmup {

    // ERROR on purpose: the CPI tenant trace only records ERROR lines from adapter loggers, and
    // these few lines per bundle revision are the evidence that the class space is fully loaded.
    private static final Logger LOG = LoggerFactory.getLogger(BundleClassWarmup.class);

    static final String ADAPTER_PACKAGE_PREFIX = "com.finkeflo.cpi.kafka.";

    /** Loaded first: the packages whose first-time loads have been observed to fail in production. */
    static final String[] PRIORITY_PREFIXES = {
        "org.apache.kafka.common.compress.",
        "org.apache.kafka.clients.consumer.",
        "org.apache.kafka.common.record.",
        "org.apache.kafka.clients.producer.",
        "org.apache.kafka.common.",
        "org.apache.kafka.",
    };

    /** Abort the background pass once the class space is evidently gone. */
    private static final int MAX_CONSECUTIVE_WIRING_FAILURES = 25;
    private static final int MAX_FAILURES_LOGGED = 300;

    private static final AtomicBoolean STARTED = new AtomicBoolean(false);

    private BundleClassWarmup() {}

    /**
     * Starts the warm-up once per class space. Safe to call from every component/consumer/producer
     * start; all but the first call return immediately.
     *
     * @param anchor a class of this bundle, used to find the class loader and the bundle content
     * @param trigger free-text origin for the log line (e.g. {@code component.createEndpoint})
     */
    static void ensureStarted(Class<?> anchor, String trigger) {
        if (!STARTED.compareAndSet(false, true)) {
            return;
        }
        OsgiBundleInfo bundle = OsgiBundleInfo.of(anchor);
        LOG.error("[CPI-KAFKA-PLUS-DIAG] class-warmup.start trigger={} loader={} bundleId={} bundleVersion={} "
                        + "bundleLastModified={} revisions={}",
                trigger, System.identityHashCode(anchor.getClassLoader()), bundle.getBundleId(),
                bundle.getVersion(), bundle.getLastModified(), bundle.getRevisionCount());

        Report adapterReport;
        try {
            adapterReport = warmAdapterClasses(anchor, bundle);
            LOG.error("[CPI-KAFKA-PLUS-DIAG] class-warmup.stage1.completed scope=adapter classes={} loaded={} failed={} durationMs={}",
                    adapterReport.attempted, adapterReport.loaded, adapterReport.failures.size(), adapterReport.durationMs);
            logFailures("stage1", adapterReport);
        } catch (Throwable t) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] class-warmup.stage1.failed exClass={} exMsg='{}'",
                    t.getClass().getName(), t.getMessage());
        }

        try {
            CodecWarmup.Result codecs = CodecWarmup.warmAll();
            LOG.error("[CPI-KAFKA-PLUS-DIAG] class-warmup.codecs.completed codecs={} failed={} durationMs={}",
                    codecs.summary(), codecs.failed(), codecs.durationMs);
        } catch (Throwable t) {
            LOG.error("[CPI-KAFKA-PLUS-DIAG] class-warmup.codecs.failed exClass={} exMsg='{}'",
                    t.getClass().getName(), t.getMessage());
        }

        Thread worker = new Thread(() -> {
            try {
                Report report = warmAllClasses(anchor, bundle);
                LOG.error("[CPI-KAFKA-PLUS-DIAG] class-warmup.stage2.completed scope=bundle source={} classes={} loaded={} failed={} "
                                + "aborted={} durationMs={}",
                        report.source, report.attempted, report.loaded, report.failures.size(), report.aborted, report.durationMs);
                logFailures("stage2", report);
            } catch (Throwable t) {
                LOG.error("[CPI-KAFKA-PLUS-DIAG] class-warmup.stage2.failed exClass={} exMsg='{}'",
                        t.getClass().getName(), t.getMessage());
            }
        }, "cpi-kafka-plus-class-warmup");
        worker.setDaemon(true);
        worker.setPriority(Thread.MIN_PRIORITY);
        worker.start();
    }

    /** Visible for tests: resets the once-guard. */
    static void resetForTests() {
        STARTED.set(false);
    }

    /** Stage 1: adapter's own classes, initialised, synchronously. */
    static Report warmAdapterClasses(Class<?> anchor, OsgiBundleInfo bundle) {
        long t0 = System.nanoTime();
        Report report = new Report("adapter");
        ClassLoader loader = anchor.getClassLoader();
        for (String name : enumerateClassNames(anchor, bundle, report, false)) {
            if (!name.startsWith(ADAPTER_PACKAGE_PREFIX)) {
                continue;
            }
            load(name, true, loader, report);
        }
        report.durationMs = (System.nanoTime() - t0) / 1_000_000L;
        return report;
    }

    /** Stage 2: every class in the bundle (root + Bundle-ClassPath jars), not initialised. */
    static Report warmAllClasses(Class<?> anchor, OsgiBundleInfo bundle) {
        long t0 = System.nanoTime();
        Report report = new Report(null);
        ClassLoader loader = anchor.getClassLoader();
        List<String> names = new ArrayList<>(enumerateClassNames(anchor, bundle, report, true));
        names.sort(Comparator.comparingInt(BundleClassWarmup::priorityRank).thenComparing(Comparator.naturalOrder()));

        int consecutiveWiringFailures = 0;
        for (String name : names) {
            if (name.startsWith(ADAPTER_PACKAGE_PREFIX)) {
                continue; // stage 1 already covered these
            }
            if (Thread.currentThread().isInterrupted()) {
                report.aborted = "interrupted";
                break;
            }
            Throwable failure = load(name, false, loader, report);
            if (failure != null && isWiringGone(failure)) {
                if (++consecutiveWiringFailures >= MAX_CONSECUTIVE_WIRING_FAILURES || bundle.isUninstalled()) {
                    report.aborted = "class-space-gone";
                    break;
                }
            } else {
                consecutiveWiringFailures = 0;
            }
        }
        report.durationMs = (System.nanoTime() - t0) / 1_000_000L;
        return report;
    }

    static int priorityRank(String className) {
        for (int i = 0; i < PRIORITY_PREFIXES.length; i++) {
            if (className.startsWith(PRIORITY_PREFIXES[i])) {
                return i;
            }
        }
        return PRIORITY_PREFIXES.length;
    }

    private static Throwable load(String name, boolean initialize, ClassLoader loader, Report report) {
        report.attempted++;
        try {
            Class.forName(name, initialize, loader);
            report.loaded++;
            return null;
        } catch (Throwable t) {
            report.failures.put(name, t.getClass().getSimpleName() + (t.getMessage() == null ? "" : ": " + t.getMessage()));
            return t;
        }
    }

    private static boolean isWiringGone(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            String msg = c.getMessage();
            if (msg != null && msg.contains("no longer valid")) {
                return true;
            }
        }
        return false;
    }

    private static void logFailures(String stage, Report report) {
        if (report.failures.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        int n = 0;
        for (Map.Entry<String, String> e : report.failures.entrySet()) {
            if (n++ >= MAX_FAILURES_LOGGED) {
                sb.append(", …+").append(report.failures.size() - MAX_FAILURES_LOGGED);
                break;
            }
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(e.getKey()).append(" [").append(e.getValue()).append(']');
        }
        LOG.error("[CPI-KAFKA-PLUS-DIAG] class-warmup.{}.failures count={} (expected for classes whose optional dependencies "
                        + "are absent on CPI; review if the set changes) classes={}",
                stage, report.failures.size(), sb);
    }

    // ---------------------------------------------------------------------------------------------
    // Class enumeration
    // ---------------------------------------------------------------------------------------------

    /**
     * All binary class names reachable through the bundle class loader: bundle root plus, when
     * {@code includeNestedJars} is set, each {@code Bundle-ClassPath} jar. Uses the OSGi bundle
     * when available, otherwise the anchor's code source (jar or directory).
     */
    static TreeSet<String> enumerateClassNames(Class<?> anchor, OsgiBundleInfo bundle, Report report,
            boolean includeNestedJars) {
        TreeSet<String> names = new TreeSet<>();
        if (bundle.isOsgi()) {
            report.source = "osgi";
            for (URL url : bundle.findEntries("/", "*.class")) {
                addIfClass(names, stripLeadingSlash(url.getPath()));
            }
            if (!includeNestedJars) {
                return names;
            }
            for (String entry : bundleClassPathEntries(bundle.getHeader("Bundle-ClassPath"))) {
                URL jar = bundle.getEntry(entry);
                if (jar == null) {
                    continue;
                }
                try (InputStream in = jar.openStream()) {
                    readNestedJar(in, names);
                } catch (IOException e) {
                    LOG.debug("class-warmup: cannot read Bundle-ClassPath entry {}: {}", entry, e.toString());
                }
            }
            return names;
        }
        try {
            URL location = anchor.getProtectionDomain().getCodeSource().getLocation();
            File file = new File(location.toURI());
            if (file.isDirectory()) {
                report.source = "directory";
                enumerateDirectory(file.toPath(), names);
            } else {
                report.source = "jar";
                enumerateJarFile(file, names, includeNestedJars);
            }
        } catch (Exception e) {
            LOG.debug("class-warmup: cannot enumerate code source of {}: {}", anchor.getName(), e.toString());
        }
        return names;
    }

    /** Independent jar walk (root + nested Bundle-ClassPath jars); shared with the packaging IT. */
    static void enumerateJarFile(File file, TreeSet<String> names, boolean includeNestedJars) throws IOException {
        try (JarFile jar = new JarFile(file)) {
            Manifest manifest = jar.getManifest();
            String bcp = manifest == null ? null : manifest.getMainAttributes().getValue("Bundle-ClassPath");
            List<String> nested = includeNestedJars ? bundleClassPathEntries(bcp) : new ArrayList<>();
            for (JarEntry entry : (Iterable<JarEntry>) jar.stream()::iterator) {
                String path = entry.getName();
                if (path.endsWith(".class")) {
                    addIfClass(names, path);
                } else if (nested.contains(path)) {
                    try (InputStream in = jar.getInputStream(entry)) {
                        readNestedJar(in, names);
                    }
                }
            }
        }
    }

    private static void enumerateDirectory(Path root, TreeSet<String> names) throws IOException {
        try (Stream<Path> walk = Files.walk(root)) {
            walk.filter(p -> p.toString().endsWith(".class"))
                .forEach(p -> addIfClass(names, root.relativize(p).toString().replace(File.separatorChar, '/')));
        }
    }

    private static void readNestedJar(InputStream in, TreeSet<String> names) throws IOException {
        try (JarInputStream jin = new JarInputStream(in)) {
            JarEntry e;
            while ((e = jin.getNextJarEntry()) != null) {
                if (e.getName().endsWith(".class")) {
                    addIfClass(names, e.getName());
                }
            }
        }
    }

    /** Bundle-ClassPath entries other than {@code .} (the root), in declaration order. */
    static List<String> bundleClassPathEntries(String header) {
        List<String> entries = new ArrayList<>();
        if (header == null) {
            return entries;
        }
        for (String clause : header.split(",")) {
            String path = clause.split(";")[0].trim();
            if (!path.isEmpty() && !".".equals(path)) {
                entries.add(stripLeadingSlash(path));
            }
        }
        return entries;
    }

    /** Converts a class file path to a binary name, skipping multi-release and module descriptors. */
    static void addIfClass(TreeSet<String> names, String path) {
        if (path == null || !path.endsWith(".class") || path.startsWith("META-INF/") || path.contains("/META-INF/")) {
            return;
        }
        String name = path.substring(0, path.length() - ".class".length()).replace('/', '.');
        if (name.endsWith("module-info")) {
            return;
        }
        names.add(name);
    }

    private static String stripLeadingSlash(String path) {
        return path.startsWith("/") ? path.substring(1) : path;
    }

    /** Outcome of one warm-up pass. */
    static final class Report {
        String source;
        int attempted;
        int loaded;
        long durationMs;
        String aborted;
        final Map<String, String> failures = new LinkedHashMap<>();

        Report(String source) {
            this.source = source;
        }
    }
}
