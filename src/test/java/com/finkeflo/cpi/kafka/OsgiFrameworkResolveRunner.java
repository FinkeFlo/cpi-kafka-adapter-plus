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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */
package com.finkeflo.cpi.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.osgi.framework.wiring.FrameworkWiring;

/**
 * Subprocess entrypoint for OSGi resolution checks.
 */
public final class OsgiFrameworkResolveRunner {

    private static final String BUNDLE_SYMBOLIC_NAME = "Bundle-SymbolicName";
    private static final String IMPORT_PACKAGE = "Import-Package";

    /**
     * Namespace of the adapter bundle's symbolic name ({@code <groupId>.<artifactId>} in the
     * pom). Derived from this class' package so it cannot drift away from the shipped bundle.
     */
    private static final String ADAPTER_BUNDLE_NAMESPACE =
            OsgiFrameworkResolveRunner.class.getPackage().getName();

    /** Symbolic-name suffix of the ADK-generated monitor bundle. */
    private static final String MONITOR_BUNDLE_SUFFIX = ".monitor";

    /**
     * The packages CPI is expected to provide to the adapter subsystem, written down here as a
     * deliberate, checked-in platform contract.
     * <p>
     * This list must <em>not</em> be derived from the built bundle's own {@code Import-Package}
     * header. Deriving it would make the resolve check pass by construction - the exact defect
     * issue #156 was raised for. Because the list is fixed, a newly calculated mandatory import
     * that CPI does not export makes {@code adapter} mode fail, which is the alarm we want.
     * <p>
     * Versions are the ones observed on a CPI tenant where that observation exists, so the list
     * models the platform the adapter actually runs on rather than a guess. Where no observation
     * exists the entry is left unversioned, which resolves against any exporter - honest about
     * what is known instead of inventing a number that would look authoritative. Packages the
     * OSGi framework already exports from the system bundle ({@code java.*}, {@code javax.*},
     * {@code org.w3c.dom}, {@code org.xml.sax}, {@code org.osgi.framework}) are intentionally
     * absent - re-exporting them here would shadow the framework's own JRE profile.
     * <p>
     * When a build legitimately introduces a new mandatory import that CPI does provide, add it
     * here together with the reason. That edit is the conscious decision this test exists to force.
     */
    private static final String[] CPI_SYSTEM_PACKAGES = {
            // Camel runtime hosting the adapter component. Version observed on a CPI tenant:
            // the platform ships org.apache.camel.camel-kafka 3.14.7.sap-56, so the Camel line
            // is 3.14.7 and the core packages are exported at that version.
            "org.apache.camel;version=3.14.7",
            "org.apache.camel.spi;version=3.14.7",
            "org.apache.camel.support;version=3.14.7",
            // Logging facade. Not covered by the tenant probe, so the version is not an
            // observation but the floor of the range the bundle declares ([1.7,2)) - an
            // unversioned export defaults to 0.0.0 and would not satisfy that import at all.
            "org.slf4j;version=1.7.0",
            "org.slf4j.event;version=1.7.0",
            "org.slf4j.helpers;version=1.7.0",
            "org.slf4j.spi;version=1.7.0",
            // SAP Integration Suite adapter APIs. Not covered by the tenant probe. The bundle
            // imports com.sap.it.api.msglog at [1.1,2) and .msglog.adapter at [1.3,2); those two
            // are pinned at the declared floor, the unversioned SAP packages stay unversioned.
            "com.sap.it.api",
            "com.sap.it.api.adapter.iflowmonitoring",
            "com.sap.it.api.keystore",
            "com.sap.it.api.msglog;version=1.1.0",
            "com.sap.it.api.msglog.adapter;version=1.3.0",
            "com.sap.it.api.securestore",
            // Consumed by the ADK-generated monitor bundle. Not covered by the tenant probe.
            "com.sap.esb.monitoring.messages.adapter",
            "com.sap.it.nm.component",
            "com.sap.it.op.component.check",
            "org.osgi.service.blueprint.container",
            // Avro's optional codecs reference these. Version observed on a CPI tenant:
            // org.apache.commons.commons-compress 1.26.1. commons-io is not in the probe.
            "org.apache.commons.compress.compressors.bzip2;version=1.26.1",
            "org.apache.commons.compress.compressors.xz;version=1.26.1",
            "org.apache.commons.io",
    };

    /**
     * Import clause injected by {@code mutation} mode. Deliberately a package no framework and no
     * platform can ever export, rather than {@code java.lang;version="[99,100)"}: {@code java.*}
     * imports are subject to parent delegation rules that differ between framework
     * implementations, so a failure there would not prove the resolve check itself works.
     */
    private static final String MUTATION_IMPORT =
            "com.finkeflo.cpi.kafka.mutationprobe.absent;version=\"[1.0,2.0)\"";

    private OsgiFrameworkResolveRunner() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: " + OsgiFrameworkResolveRunner.class.getName()
                    + " <standalone|adapter|mutation|negative> [esa]");
            System.exit(2);
        }

        String mode = args[0];
        if ("standalone".equals(mode) || "resolve".equals(mode)) {
            requireEsaArgument(args);
            runStandaloneResolve(new File(args[1]));
            System.exit(0);
        }
        if ("adapter".equals(mode)) {
            requireEsaArgument(args);
            runAdapterResolve(new File(args[1]));
            System.exit(0);
        }
        if ("mutation".equals(mode)) {
            requireEsaArgument(args);
            runMutationProbe(new File(args[1]));
            System.exit(0);
        }
        if ("negative".equals(mode)) {
            runNegativeGuard();
            System.exit(0);
        }

        System.out.println("Unknown mode: " + mode);
        System.exit(2);
    }

    private static void requireEsaArgument(String[] args) {
        if (args.length < 2) {
            System.out.println("Missing ESA path");
            System.exit(2);
        }
    }

    /**
     * Resolves the third-party bundles the ESA ships alongside the project's own bundles.
     * <p>
     * An empty set is the expected steady state: every runtime dependency is either embedded in
     * the fat bundle via {@code Embed-Dependency} or listed in the {@code copy-dependencies}
     * {@code excludeGroupIds}, so nothing should be emitted as a standalone subsystem jar. The
     * count is printed unconditionally and asserted by the caller, because a silent "nothing to
     * do" is indistinguishable from a passing check (issue #156).
     * <p>
     * A non-empty set means a transitive dependency with an unexpected {@code groupId} slipped
     * past {@code excludeGroupIds}. That is exactly how {@code at.yawk.lz4:lz4-java} broke CPI
     * deployment, so those bundles are resolved for real.
     */
    private static void runStandaloneResolve(File esa) throws Exception {
        List<BundleArchive> standaloneBundles = readBundles(esa, false);
        System.out.println("standaloneBundles=" + standaloneBundles.size());
        if (standaloneBundles.isEmpty()) {
            System.out.println("No standalone dependency bundles in ESA " + esa.getAbsolutePath()
                    + " (expected steady state: all runtime dependencies are embedded).");
            return;
        }

        String storagePath = "target/felix-resolve-" + UUID.randomUUID().toString();
        Framework framework = startFramework(storagePath, null);
        try {
            List<Bundle> installed = installBundles(framework, standaloneBundles);
            FrameworkWiring wiring = framework.adapt(FrameworkWiring.class);
            boolean resolved = wiring.resolveBundles(installed);
            if (!resolved) {
                throw new IllegalStateException("Failed to resolve ESA standalone bundles from "
                        + esa.getAbsolutePath() + "\n" + unresolvedDiagnostics(installed));
            }
            System.out.println("Resolved standalone ESA bundles: " + standaloneBundles.size());
        } finally {
            shutdown(framework, storagePath);
        }
    }

    /**
     * Installs the bundles this project ships - the fat adapter bundle and the ADK monitor bundle -
     * into a framework that exports {@link #CPI_SYSTEM_PACKAGES}, and resolves them for real.
     * <p>
     * This is the check that carries the property the suite is supposed to guarantee: the shipped
     * bundle resolves against the packages CPI actually offers.
     */
    private static void runAdapterResolve(File esa) throws Exception {
        List<BundleArchive> projectBundles = readBundles(esa, true);
        System.out.println("projectBundles=" + projectBundles.size());
        if (projectBundles.isEmpty()) {
            throw new IllegalStateException("No project-owned bundles found in ESA " + esa.getAbsolutePath()
                    + ". The ESA must contain the adapter bundle; refusing to report success.");
        }

        String storagePath = "target/felix-resolve-adapter-" + UUID.randomUUID().toString();
        Framework framework = startFramework(storagePath, systemPackagesExtra());
        try {
            List<Bundle> installed = installBundles(framework, projectBundles);
            FrameworkWiring wiring = framework.adapt(FrameworkWiring.class);
            if (!wiring.resolveBundles(installed)) {
                throw new IllegalStateException("Failed to resolve the shipped adapter bundles from "
                        + esa.getAbsolutePath()
                        + "\nEither CPI does not provide a newly calculated mandatory import, or the"
                        + " platform contract in CPI_SYSTEM_PACKAGES needs a deliberate update.\n"
                        + unresolvedDiagnostics(installed));
            }
            for (int i = 0; i < installed.size(); i++) {
                System.out.println("resolved " + bundleName(installed.get(i)));
            }
            System.out.println("Resolved project bundles: " + projectBundles.size());
        } finally {
            shutdown(framework, storagePath);
        }
    }

    /**
     * Anti-vacuity guard for {@link #runAdapterResolve}: takes the very same shipped adapter
     * bundle, injects one unsatisfiable mandatory import into its manifest and requires the
     * resolve to fail.
     * <p>
     * Without this, {@code adapter} mode could silently degrade into another check that cannot
     * fail. Running it against the real artifact rather than a synthetic bundle is what makes it
     * a mutation probe rather than a second negative guard.
     */
    private static void runMutationProbe(File esa) throws Exception {
        List<BundleArchive> projectBundles = readBundles(esa, true);
        BundleArchive adapter = findAdapterBundle(projectBundles);
        if (adapter == null) {
            throw new IllegalStateException("No adapter bundle found in ESA " + esa.getAbsolutePath());
        }

        BundleArchive mutated = new BundleArchive(adapter.entryName,
                withAdditionalImport(adapter.content, MUTATION_IMPORT), adapter.symbolicName);

        String storagePath = "target/felix-resolve-mutation-" + UUID.randomUUID().toString();
        Framework framework = startFramework(storagePath, systemPackagesExtra());
        try {
            List<Bundle> installed = installBundles(framework, Arrays.asList(mutated));
            FrameworkWiring wiring = framework.adapt(FrameworkWiring.class);
            if (wiring.resolveBundles(installed)) {
                throw new IllegalStateException("Mutation probe resolved despite the injected import "
                        + MUTATION_IMPORT + ". The adapter resolve check cannot detect an"
                        + " unsatisfiable mandatory import and is therefore vacuous.");
            }
            String diagnostics = unresolvedDiagnostics(installed);
            if (diagnostics.indexOf("com.finkeflo.cpi.kafka.mutationprobe.absent") < 0) {
                throw new IllegalStateException("Mutation probe failed to resolve, but not because of the"
                        + " injected import. Diagnostics:\n" + diagnostics);
            }
            System.out.println("mutationProbe=detected");
        } finally {
            shutdown(framework, storagePath);
        }
    }

    private static BundleArchive findAdapterBundle(List<BundleArchive> projectBundles) {
        for (int i = 0; i < projectBundles.size(); i++) {
            BundleArchive candidate = projectBundles.get(i);
            String symbolicName = stripManifestAttributes(candidate.symbolicName);
            if (symbolicName != null && symbolicName.startsWith(ADAPTER_BUNDLE_NAMESPACE)
                    && !symbolicName.endsWith(MONITOR_BUNDLE_SUFFIX)) {
                return candidate;
            }
        }
        return null;
    }

    private static String systemPackagesExtra() {
        StringBuilder packages = new StringBuilder();
        for (int i = 0; i < CPI_SYSTEM_PACKAGES.length; i++) {
            if (packages.length() > 0) {
                packages.append(',');
            }
            packages.append(CPI_SYSTEM_PACKAGES[i]);
        }
        return packages.toString();
    }

    private static void shutdown(Framework framework, String storagePath) throws Exception {
        try {
            stopFramework(framework);
        } finally {
            deleteRecursively(new File(storagePath));
        }
    }

    private static void runNegativeGuard() throws Exception {
        String storagePath = "target/felix-resolve-negative-" + UUID.randomUUID().toString();
        Framework framework = startFramework(storagePath, null);
        try {
            byte[] brokenBundle = createBundleWithImport("com.finkeflo.cpi.kafka.test.unresolvable",
                    "com.finkeflo.cpi.kafka.missing.pkg;version=\"[1.0,2.0)\"");
            Bundle bundle = framework.getBundleContext().installBundle("memory:broken-guard", new ByteArrayInputStream(
                    brokenBundle));
            FrameworkWiring wiring = framework.adapt(FrameworkWiring.class);
            boolean resolved = wiring.resolveBundles(Arrays.asList(bundle));
            if (resolved) {
                throw new IllegalStateException("Negative guard resolved unexpectedly.");
            }
            String diagnostics = unresolvedDiagnostics(Arrays.asList(bundle));
            if (diagnostics.indexOf("com.finkeflo.cpi.kafka.missing.pkg") < 0) {
                throw new IllegalStateException("Negative guard diagnostics missing unresolved package:\n"
                        + diagnostics);
            }
            System.out.println("Negative guard passed.");
        } finally {
            shutdown(framework, storagePath);
        }
    }

    private static List<BundleArchive> readBundles(File esa, boolean projectOwned) throws IOException {
        List<BundleArchive> bundles = new ArrayList<BundleArchive>();
        JarFile esaJar = new JarFile(esa);
        try {
            Enumeration<JarEntry> entries = esaJar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (entry.isDirectory() || !entry.getName().endsWith(".jar")) {
                    continue;
                }

                byte[] content = readAllBytes(esaJar.getInputStream(entry));
                Manifest manifest = readNestedManifest(content);
                if (manifest == null) {
                    continue;
                }
                Attributes attributes = manifest.getMainAttributes();
                String symbolicName = attributes.getValue(BUNDLE_SYMBOLIC_NAME);
                if (!hasText(symbolicName)) {
                    continue;
                }
                if (isProjectOwnedBundle(symbolicName) != projectOwned) {
                    continue;
                }
                bundles.add(new BundleArchive(entry.getName(), content, symbolicName));
            }
        } finally {
            esaJar.close();
        }
        return bundles;
    }

    /**
     * Decides whether a nested ESA bundle is built by this project (and therefore installed by
     * CPI itself rather than by the resolve harness). The decision is taken on the bundle's
     * declared {@code Bundle-SymbolicName}, not on the jar file name, so renamed artifacts or
     * version bumps cannot silently change which bundles the resolve test installs.
     */
    static boolean isProjectOwnedBundle(String symbolicName) {
        String name = stripManifestAttributes(symbolicName);
        if (!hasText(name)) {
            return false;
        }
        if (name.equals(ADAPTER_BUNDLE_NAMESPACE) || name.startsWith(ADAPTER_BUNDLE_NAMESPACE + ".")) {
            return true;
        }
        return name.endsWith(MONITOR_BUNDLE_SUFFIX);
    }

    /** Drops the {@code ;singleton:=true}-style attributes a symbolic-name header may carry. */
    private static String stripManifestAttributes(String header) {
        if (header == null) {
            return null;
        }
        int separator = header.indexOf(';');
        String name = separator < 0 ? header : header.substring(0, separator);
        return name.trim();
    }

    private static Manifest readNestedManifest(byte[] jarBytes) throws IOException {
        JarInputStream nested = new JarInputStream(new ByteArrayInputStream(jarBytes));
        try {
            return nested.getManifest();
        } finally {
            nested.close();
        }
    }

    private static List<Bundle> installBundles(Framework framework, List<BundleArchive> archives) throws BundleException {
        List<Bundle> bundles = new ArrayList<Bundle>();
        for (int i = 0; i < archives.size(); i++) {
            BundleArchive archive = archives.get(i);
            Bundle bundle = framework.getBundleContext().installBundle("memory:" + archive.entryName,
                    new ByteArrayInputStream(archive.content));
            bundles.add(bundle);
        }
        return bundles;
    }

    private static Framework startFramework(String storagePath, String systemPackagesExtra) throws BundleException {
        FrameworkFactory factory = findFrameworkFactory();
        Map<String, String> config = new HashMap<String, String>();
        config.put(Constants.FRAMEWORK_STORAGE, storagePath);
        config.put(Constants.FRAMEWORK_STORAGE_CLEAN, Constants.FRAMEWORK_STORAGE_CLEAN_ONFIRSTINIT);
        config.put(Constants.FRAMEWORK_BOOTDELEGATION, "");
        if (hasText(systemPackagesExtra)) {
            config.put(Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA, systemPackagesExtra);
        }
        Framework framework = factory.newFramework(config);
        framework.start();
        return framework;
    }

    private static FrameworkFactory findFrameworkFactory() {
        java.util.ServiceLoader<FrameworkFactory> loader = java.util.ServiceLoader.load(FrameworkFactory.class);
        for (FrameworkFactory candidate : loader) {
            return candidate;
        }
        throw new IllegalStateException("No OSGi FrameworkFactory found on the classpath");
    }

    private static void stopFramework(Framework framework) throws BundleException, InterruptedException {
        framework.stop();
        framework.waitForStop(5000L);
    }

    private static boolean deleteRecursively(File file) {
        try {
            if (file == null || !file.exists()) {
                return true;
            }
            if (file.isDirectory()) {
                File[] children = file.listFiles();
                if (children != null) {
                    for (int i = 0; i < children.length; i++) {
                        deleteRecursively(children[i]);
                    }
                }
            }
            return file.delete() || !file.exists();
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static String unresolvedDiagnostics(List<Bundle> bundles) {
        Map<String, List<String>> availableExports = collectExportedPackages(bundles);
        for (int i = 0; i < CPI_SYSTEM_PACKAGES.length; i++) {
            String pkg = firstPathSegment(CPI_SYSTEM_PACKAGES[i]);
            if (!availableExports.containsKey(pkg)) {
                availableExports.put(pkg, new ArrayList<String>(
                        Arrays.asList("<CPI system packages>:" + declaredVersion(CPI_SYSTEM_PACKAGES[i]))));
            }
        }
        StringBuilder diagnostic = new StringBuilder();
        for (int i = 0; i < bundles.size(); i++) {
            Bundle bundle = bundles.get(i);
            if (bundle.getState() != Bundle.INSTALLED) {
                continue;
            }
            String symbolicName = bundle.getSymbolicName();
            if (!hasText(symbolicName)) {
                symbolicName = "<unknown>";
            }
            diagnostic.append("- ").append(symbolicName).append(" [state=INSTALLED]").append('\n');
            List<String> requiredImports = mandatoryImports(bundle);
            if (requiredImports.isEmpty()) {
                diagnostic.append("  mandatory imports: <none>").append('\n');
                continue;
            }
            for (int j = 0; j < requiredImports.size(); j++) {
                String pkg = requiredImports.get(j);
                List<String> exporters = availableExports.get(packageOf(pkg));
                diagnostic.append("  import ").append(pkg).append(" -> ");
                if (exporters == null || exporters.isEmpty()) {
                    diagnostic.append("UNRESOLVED (no exporter among the installed bundles, the system"
                            + " bundle or the CPI platform contract)");
                } else {
                    diagnostic.append("exported by ").append(exporters);
                }
                diagnostic.append('\n');
            }
        }
        if (diagnostic.length() == 0) {
            return "No INSTALLED bundles remained unresolved.";
        }
        return diagnostic.toString();
    }

    private static Map<String, List<String>> collectExportedPackages(List<Bundle> bundles) {
        Map<String, List<String>> exports = new LinkedHashMap<String, List<String>>();
        for (int i = 0; i < bundles.size(); i++) {
            Bundle bundle = bundles.get(i);
            Dictionary<String, String> headers = bundle.getHeaders();
            String exportHeader = headers.get(Constants.EXPORT_PACKAGE);
            List<String> clauses = splitHeaderClauses(exportHeader);
            for (int j = 0; j < clauses.size(); j++) {
                String pkg = firstPathSegment(clauses.get(j));
                if (!hasText(pkg)) {
                    continue;
                }
                List<String> exporters = exports.get(pkg);
                if (exporters == null) {
                    exporters = new ArrayList<String>();
                    exports.put(pkg, exporters);
                }
                exporters.add(bundleName(bundle));
            }
        }
        return exports;
    }

    private static String bundleName(Bundle bundle) {
        String bsn = bundle.getSymbolicName();
        if (!hasText(bsn)) {
            return "<unknown>";
        }
        return bsn + ":" + bundle.getVersion();
    }

    private static List<String> mandatoryImports(Bundle bundle) {
        List<String> imports = new ArrayList<String>();
        Dictionary<String, String> headers = bundle.getHeaders();
        String importHeader = headers.get(IMPORT_PACKAGE);
        List<String> clauses = splitHeaderClauses(importHeader);
        for (int i = 0; i < clauses.size(); i++) {
            ImportClause clause = parseImportClause(clauses.get(i));
            if (clause.optional) {
                continue;
            }
            for (int j = 0; j < clause.packageNames.size(); j++) {
                imports.add(withVersion(clause.packageNames.get(j), clause.versionRange));
            }
        }
        return imports;
    }

    /**
     * Renders {@code pkg} and {@code version} as {@code pkg version=<range>}, so a diagnostic can
     * show why a package that <em>is</em> exported still does not satisfy an import. Without the
     * version a range mismatch reads as if everything were fine.
     */
    private static String withVersion(String name, String version) {
        return hasText(version) ? name + " version=" + version : name + " version=<any>";
    }

    /** @return the package name of a {@code pkg version=<range>} entry produced by {@link #withVersion}. */
    private static String packageOf(String annotated) {
        int space = annotated.indexOf(' ');
        return space < 0 ? annotated : annotated.substring(0, space);
    }

    private static ImportClause parseImportClause(String clause) {
        String[] segments = clause.split(";");
        List<String> packageNames = new ArrayList<String>();
        boolean optional = false;
        String versionRange = null;
        for (int i = 0; i < segments.length; i++) {
            String segment = segments[i].trim();
            if (segment.length() == 0) {
                continue;
            }
            if (segment.indexOf('=') >= 0) {
                if (segment.startsWith("resolution:=")) {
                    optional = "optional".equals(unquote(segment.substring("resolution:=".length()).trim()));
                } else if (segment.startsWith("version=")) {
                    versionRange = unquote(segment.substring("version=".length()).trim());
                }
                continue;
            }
            packageNames.add(segment);
        }
        return new ImportClause(packageNames, optional, versionRange);
    }

    /** @return the {@code version=} attribute of an {@link #CPI_SYSTEM_PACKAGES} clause, or {@code <unversioned>}. */
    private static String declaredVersion(String clause) {
        String[] segments = clause.split(";");
        for (int i = 1; i < segments.length; i++) {
            String segment = segments[i].trim();
            if (segment.startsWith("version=")) {
                return unquote(segment.substring("version=".length()).trim());
            }
        }
        return "<unversioned>";
    }

    private static String firstPathSegment(String clause) {        int semicolon = clause.indexOf(';');
        String path = semicolon >= 0 ? clause.substring(0, semicolon) : clause;
        return path.trim();
    }

    private static List<String> splitHeaderClauses(String header) {
        List<String> clauses = new ArrayList<String>();
        if (!hasText(header)) {
            return clauses;
        }
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < header.length(); i++) {
            char ch = header.charAt(i);
            if (ch == '"') {
                quoted = !quoted;
                current.append(ch);
            } else if (ch == ',' && !quoted) {
                addClause(clauses, current);
            } else {
                current.append(ch);
            }
        }
        addClause(clauses, current);
        return clauses;
    }

    private static void addClause(List<String> clauses, StringBuilder current) {
        String clause = current.toString().trim();
        if (clause.length() > 0) {
            clauses.add(clause);
        }
        current.setLength(0);
    }

    private static String unquote(String value) {
        if (value.length() >= 2 && value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static boolean hasText(String value) {
        return value != null && value.trim().length() > 0;
    }

    private static byte[] createBundleWithImport(String symbolicName, String importPackage) throws IOException {
        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.putValue("Manifest-Version", "1.0");
        attributes.putValue("Bundle-ManifestVersion", "2");
        attributes.putValue(BUNDLE_SYMBOLIC_NAME, symbolicName);
        attributes.putValue("Bundle-Version", "1.0.0");
        attributes.putValue(IMPORT_PACKAGE, importPackage);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        JarOutputStream jarOutput = new JarOutputStream(output, manifest);
        try {
            jarOutput.putNextEntry(new JarEntry("META-INF/"));
            jarOutput.closeEntry();
        } finally {
            jarOutput.close();
        }
        return output.toByteArray();
    }

    /**
     * Repacks a bundle jar with one extra clause appended to its {@code Import-Package} header,
     * leaving every other header and every entry untouched.
     * <p>
     * The jar is rewritten uncompressed: only the manifest matters for resolution and the result
     * is a throwaway in-memory artifact, so paying the deflate cost for a 17 MB fat bundle would
     * be wasted time.
     */
    static byte[] withAdditionalImport(byte[] jarBytes, String importClause) throws IOException {
        JarInputStream source = new JarInputStream(new ByteArrayInputStream(jarBytes));
        try {
            Manifest manifest = source.getManifest();
            if (manifest == null) {
                throw new IOException("Bundle jar has no manifest; cannot mutate Import-Package");
            }
            Attributes attributes = manifest.getMainAttributes();
            String existing = attributes.getValue(IMPORT_PACKAGE);
            attributes.putValue(IMPORT_PACKAGE, hasText(existing) ? existing + "," + importClause : importClause);

            ByteArrayOutputStream output = new ByteArrayOutputStream(jarBytes.length);
            JarOutputStream target = new JarOutputStream(output, manifest);
            try {
                target.setLevel(java.util.zip.Deflater.NO_COMPRESSION);
                byte[] buffer = new byte[8192];
                JarEntry entry;
                while ((entry = source.getNextJarEntry()) != null) {
                    target.putNextEntry(new JarEntry(entry.getName()));
                    int read;
                    while ((read = source.read(buffer)) >= 0) {
                        target.write(buffer, 0, read);
                    }
                    target.closeEntry();
                }
            } finally {
                target.close();
            }
            return output.toByteArray();
        } finally {
            source.close();
        }
    }

    private static byte[] readAllBytes(InputStream inputStream) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = inputStream.read(buffer)) >= 0) {
                output.write(buffer, 0, read);
            }
        } finally {
            inputStream.close();
        }
        return output.toByteArray();
    }

    private static final class BundleArchive {
        private final String entryName;
        private final byte[] content;
        private final String symbolicName;

        private BundleArchive(String entryName, byte[] content, String symbolicName) {
            this.entryName = entryName;
            this.content = content;
            this.symbolicName = symbolicName;
        }
    }

    private static final class ImportClause {
        private final List<String> packageNames;
        private final boolean optional;
        private final String versionRange;

        private ImportClause(List<String> packageNames, boolean optional, String versionRange) {
            this.packageNames = packageNames;
            this.optional = optional;
            this.versionRange = versionRange;
        }
    }
}
