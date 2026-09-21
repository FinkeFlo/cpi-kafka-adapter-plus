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
import java.util.Collection;
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
 *       are not present on CPI; they are logged by name so the set can be reviewed. The thread
 *       is deliberately <em>not</em> tied to the component lifecycle: the guard is per bundle
 *       revision, so an iFlow undeploy must not abort the warm-up that protects the other
 *       routes. It stops on its own when the <em>bundle</em> is stopping/stopped (checked every
 *       {@link #BUNDLE_STATE_CHECK_INTERVAL} classes) or when class loads fail with the
 *       "wiring no longer valid" signature.</li>
 *   <li><b>Imported classes, same thread, after stage 2</b>: the bundle declares
 *       {@code DynamicImport-Package: *}, so classes owned by <em>other</em> bundles (Camel, the
 *       SAP APIs, SLF4J, …) are wired lazily on first reference and fail the same way on a purged
 *       revision (issue #154). Stage 3 re-walks the bundle content, reads the constant pool of
 *       every class ({@link ConstantPoolScanner}) and loads the types that are referenced but not
 *       contained in the bundle. It runs <em>after</em> stage 2 on purpose: stage 2 covers the
 *       failures actually observed in production and must not be delayed by the scan.</li>
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

    /**
     * Stage 3 ordering: packages provided by the CPI platform, whose classes an error or stop path
     * is most likely to touch for the first time after an adapter update.
     */
    static final String[] IMPORTED_PRIORITY_PREFIXES = {
        "org.apache.camel.",
        "com.sap.it.api.",
        "org.slf4j.",
        "org.osgi.",
        "javax.",
    };

    /** Abort the background pass once the class space is evidently gone. */    private static final int MAX_CONSECUTIVE_WIRING_FAILURES = 25;
    /** Poll the bundle state this often (in classes) so the pass ends promptly on bundle stop. */
    static final int BUNDLE_STATE_CHECK_INTERVAL = 200;
    private static final int MAX_FAILURES_LOGGED = 300;
    private static final int MAX_PACKAGES_LOGGED = 20;

    private static final AtomicBoolean STARTED = new AtomicBoolean(false);

    private BundleClassWarmup() {}

    /**
     * Starts the warm-up once per class space. Safe to call from every component/consumer/producer
     * start; all but the first call return immediately.
     *
     * @param anchor a class of this bundle, used to find the class loader and the bundle content
     * @param trigger free-text origin for the log line (e.g. {@code component.createEndpoint})
     * @return {@code true} if this call started the warm-up, {@code false} if it had already run
     *         (or is running) for this class space
     */
    static boolean ensureStarted(Class<?> anchor, String trigger) {
        if (!STARTED.compareAndSet(false, true)) {
            return false;
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
                if (report.aborted != null) {
                    return; // class space is gone or the bundle is stopping — stage 3 cannot help
                }
                Report imported = warmImportedClasses(anchor, bundle);
                LOG.error("[CPI-KAFKA-PLUS-DIAG] class-warmup.stage3.completed scope=imported source={} referenced={} classes={} "
                                + "loaded={} failed={} aborted={} durationMs={}",
                        imported.source, imported.referencedTypes, imported.attempted, imported.loaded,
                        imported.failures.size(), imported.aborted, imported.durationMs);
                logFailures("stage3", imported);
            } catch (Throwable t) {
                LOG.error("[CPI-KAFKA-PLUS-DIAG] class-warmup.stage2.failed exClass={} exMsg='{}'",
                        t.getClass().getName(), t.getMessage());
            }
        }, "cpi-kafka-plus-class-warmup");
        worker.setDaemon(true);
        worker.setPriority(Thread.MIN_PRIORITY);
        worker.start();
        return true;
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
        for (String name : buildClassIndex(anchor, bundle, report, false, false).names) {
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
        List<String> names = new ArrayList<>(buildClassIndex(anchor, bundle, report, true, false).names);
        names.sort(Comparator.comparingInt(BundleClassWarmup::priorityRank).thenComparing(Comparator.naturalOrder()));
        names.removeIf(name -> name.startsWith(ADAPTER_PACKAGE_PREFIX)); // stage 1 already covered these
        loadAll(names, anchor.getClassLoader(), bundle, report);
        report.durationMs = (System.nanoTime() - t0) / 1_000_000L;
        return report;
    }

    /**
     * Stage 3: the classes the bundle bytecode references but does not contain — everything that
     * {@code DynamicImport-Package: *} would otherwise wire lazily on first use (issue #154).
     */
    static Report warmImportedClasses(Class<?> anchor, OsgiBundleInfo bundle) {
        long t0 = System.nanoTime();
        Report report = new Report(null);
        ClassIndex index = buildClassIndex(anchor, bundle, report, true, true);
        report.referencedTypes = index.referencedTypes.size();
        List<String> names = new ArrayList<>(importedClassNames(index));
        names.sort(Comparator.comparingInt(BundleClassWarmup::importedPriorityRank).thenComparing(Comparator.naturalOrder()));
        loadAll(names, anchor.getClassLoader(), bundle, report);
        report.durationMs = (System.nanoTime() - t0) / 1_000_000L;
        return report;
    }

    /**
     * The referenced types that are not part of the bundle and therefore have to come from another
     * bundle through the wiring. Visible for the packaging IT.
     *
     * <p>{@code java.*} is left out: the OSGi core specification requires every bundle class loader
     * to delegate it to the parent, so those classes never travel over a bundle wire and cannot be
     * invalidated by a revision purge.
     */
    static TreeSet<String> importedClassNames(ClassIndex index) {
        TreeSet<String> imported = new TreeSet<>();
        for (String type : index.referencedTypes) {
            if (type.startsWith("java.") || index.names.contains(type)) {
                continue;
            }
            imported.add(type);
        }
        return imported;
    }

    /**
     * Loads {@code names} without initialising, bailing out when the bundle stops or the class
     * space turns out to be gone.
     */
    private static void loadAll(Collection<String> names, ClassLoader loader, OsgiBundleInfo bundle, Report report) {
        int consecutiveWiringFailures = 0;
        int sinceStateCheck = 0;
        for (String name : names) {
            if (Thread.currentThread().isInterrupted()) {
                report.aborted = "interrupted";
                break;
            }
            if (++sinceStateCheck >= BUNDLE_STATE_CHECK_INTERVAL) {
                sinceStateCheck = 0;
                if (bundle.isStoppingOrStopped()) {
                    report.aborted = "bundle-stopping";
                    break;
                }
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
    }

    static int priorityRank(String className) {
        return rank(className, PRIORITY_PREFIXES);
    }

    static int importedPriorityRank(String className) {
        return rank(className, IMPORTED_PRIORITY_PREFIXES);
    }

    private static int rank(String className, String[] prefixes) {
        for (int i = 0; i < prefixes.length; i++) {
            if (className.startsWith(prefixes[i])) {
                return i;
            }
        }
        return prefixes.length;
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
        // ERROR (the only level that reaches the tenant trace): count plus per-package tally, a
        // few hundred bytes. The full class list would be several KB per revision and is DEBUG.
        LOG.error("[CPI-KAFKA-PLUS-DIAG] class-warmup.{}.failures count={} (expected for classes whose optional dependencies "
                        + "are absent on CPI; review if the set changes) packages={}",
                stage, report.failures.size(), summarisePackages(report.failures.keySet(), MAX_PACKAGES_LOGGED));
        if (LOG.isDebugEnabled()) {
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
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] class-warmup.{}.failures.detail classes={}", stage, sb);
        }
    }

    /**
     * {@code package:count} pairs, most failures first, capped at {@code max} packages (the rest
     * folded into {@code …+N packages}). Visible for tests.
     */
    static String summarisePackages(Iterable<String> classNames, int max) {
        Map<String, Integer> perPackage = new LinkedHashMap<>();
        for (String name : classNames) {
            int dot = name.lastIndexOf('.');
            String pkg = dot > 0 ? name.substring(0, dot) : "(default)";
            perPackage.merge(pkg, 1, Integer::sum);
        }
        List<Map.Entry<String, Integer>> sorted = new ArrayList<>(perPackage.entrySet());
        sorted.sort(Comparator.<Map.Entry<String, Integer>>comparingInt(Map.Entry::getValue).reversed()
                .thenComparing(Map.Entry::getKey));
        StringBuilder sb = new StringBuilder();
        int n = 0;
        for (Map.Entry<String, Integer> e : sorted) {
            if (n++ >= max) {
                sb.append(",…+").append(sorted.size() - max).append(" packages");
                break;
            }
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(e.getKey()).append(':').append(e.getValue());
        }
        return sb.toString();
    }

    // ---------------------------------------------------------------------------------------------
    // Class enumeration
    // ---------------------------------------------------------------------------------------------

    /**
     * All binary class names reachable through the bundle class loader: bundle root plus, when
     * {@code includeNestedJars} is set, each {@code Bundle-ClassPath} jar. Uses the OSGi bundle
     * when available, otherwise the anchor's code source (jar or directory).
     *
     * @param scanReferences also read each class file's constant pool and collect the types it
     *        references (stage 3); costs a full read of the bundle content
     */
    static ClassIndex buildClassIndex(Class<?> anchor, OsgiBundleInfo bundle, Report report,
            boolean includeNestedJars, boolean scanReferences) {
        ClassIndex index = new ClassIndex(scanReferences);
        if (bundle.isOsgi()) {
            report.source = "osgi";
            for (URL url : bundle.findEntries("/", "*.class")) {
                addUrlEntry(index, stripLeadingSlash(url.getPath()), url);
            }
            if (!includeNestedJars) {
                return index;
            }
            for (String entry : bundleClassPathEntries(bundle.getHeader("Bundle-ClassPath"))) {
                URL jar = bundle.getEntry(entry);
                if (jar == null) {
                    continue;
                }
                try (InputStream in = jar.openStream()) {
                    readNestedJar(in, index);
                } catch (IOException e) {
                    LOG.debug("class-warmup: cannot read Bundle-ClassPath entry {}: {}", entry, e.toString());
                }
            }
            return index;
        }
        try {
            URL location = anchor.getProtectionDomain().getCodeSource().getLocation();
            File file = new File(location.toURI());
            if (file.isDirectory()) {
                report.source = "directory";
                enumerateDirectory(file.toPath(), index);
            } else {
                report.source = "jar";
                enumerateJarFile(file, index, includeNestedJars);
            }
        } catch (Exception e) {
            LOG.debug("class-warmup: cannot enumerate code source of {}: {}", anchor.getName(), e.toString());
        }
        return index;
    }

    /** Independent jar walk (root + nested Bundle-ClassPath jars); shared with the packaging IT. */
    static void enumerateJarFile(File file, ClassIndex index, boolean includeNestedJars) throws IOException {
        try (JarFile jar = new JarFile(file)) {
            Manifest manifest = jar.getManifest();
            String bcp = manifest == null ? null : manifest.getMainAttributes().getValue("Bundle-ClassPath");
            List<String> nested = includeNestedJars ? bundleClassPathEntries(bcp) : new ArrayList<>();
            for (JarEntry entry : (Iterable<JarEntry>) jar.stream()::iterator) {
                String path = entry.getName();
                if (path.endsWith(".class")) {
                    if (index.needsBytes(path)) {
                        try (InputStream in = jar.getInputStream(entry)) {
                            index.add(path, in);
                        }
                    } else {
                        index.add(path, null);
                    }
                } else if (nested.contains(path)) {
                    try (InputStream in = jar.getInputStream(entry)) {
                        readNestedJar(in, index);
                    }
                }
            }
        }
    }

    private static void addUrlEntry(ClassIndex index, String path, URL url) {
        if (!index.needsBytes(path)) {
            index.add(path, null);
            return;
        }
        try (InputStream in = url.openStream()) {
            index.add(path, in);
        } catch (IOException e) {
            index.add(path, null);
        }
    }

    private static void enumerateDirectory(Path root, ClassIndex index) throws IOException {
        try (Stream<Path> walk = Files.walk(root)) {
            for (Path p : (Iterable<Path>) walk.filter(p -> p.toString().endsWith(".class"))::iterator) {
                String path = root.relativize(p).toString().replace(File.separatorChar, '/');
                if (!index.needsBytes(path)) {
                    index.add(path, null);
                    continue;
                }
                try (InputStream in = Files.newInputStream(p)) {
                    index.add(path, in);
                }
            }
        }
    }

    private static void readNestedJar(InputStream in, ClassIndex index) throws IOException {
        try (JarInputStream jin = new JarInputStream(in)) {
            JarEntry e;
            while ((e = jin.getNextJarEntry()) != null) {
                String path = e.getName();
                if (!path.endsWith(".class")) {
                    continue;
                }
                // JarInputStream serves the current entry; it must not be closed by the index.
                index.add(path, index.needsBytes(path) ? jin : null);
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

    /** Converts a class file path to a binary name, or {@code null} for entries to skip. */
    static String binaryName(String path) {
        if (path == null || !path.endsWith(".class") || path.startsWith("META-INF/") || path.contains("/META-INF/")) {
            return null;
        }
        String name = path.substring(0, path.length() - ".class".length()).replace('/', '.');
        return name.endsWith("module-info") ? null : name;
    }

    private static String stripLeadingSlash(String path) {
        return path.startsWith("/") ? path.substring(1) : path;
    }

    /**
     * The classes the bundle contains and, when reference scanning is on, every type their
     * bytecode can make the JVM resolve.
     */
    static final class ClassIndex {

        final TreeSet<String> names = new TreeSet<>();
        final TreeSet<String> referencedTypes = new TreeSet<>();
        /** Class files whose constant pool the scanner could not read; expected to stay 0. */
        int unreadable;

        private final boolean scanReferences;

        ClassIndex(boolean scanReferences) {
            this.scanReferences = scanReferences;
        }

        /** {@code true} when the caller must open the class file so its constant pool can be read. */
        boolean needsBytes(String path) {
            return scanReferences && binaryName(path) != null;
        }

        /**
         * Records one class file entry. {@code bytes} may be {@code null} (name only) and is never
         * closed here — the walker that opened it stays the owner.
         */
        void add(String path, InputStream bytes) {
            String name = binaryName(path);
            if (name == null) {
                return;
            }
            names.add(name);
            if (bytes == null) {
                return;
            }
            try {
                if (!ConstantPoolScanner.collectReferencedTypes(bytes, referencedTypes)) {
                    unreadable++;
                }
            } catch (IOException | RuntimeException e) {
                unreadable++;
                LOG.debug("class-warmup: cannot scan constant pool of {}: {}", name, e.toString());
            }
        }
    }

    /** Outcome of one warm-up pass. */
    static final class Report {
        String source;
        int attempted;
        int loaded;
        long durationMs;
        String aborted;
        /** Stage 3 only: distinct types found in the constant pools, before filtering. */
        int referencedTypes;
        final Map<String, String> failures = new LinkedHashMap<>();

        Report(String source) {
            this.source = source;
        }
    }
}
