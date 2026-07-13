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
import java.util.Locale;
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

    private OsgiFrameworkResolveRunner() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: " + OsgiFrameworkResolveRunner.class.getName() + " <resolve|negative> [esa]");
            System.exit(2);
        }

        String mode = args[0];
        if ("resolve".equals(mode)) {
            if (args.length < 2) {
                System.out.println("Missing ESA path for resolve mode");
                System.exit(2);
            }
            runResolve(new File(args[1]), args.length > 2 ? args[2] : "");
            System.exit(0);
        }
        if ("negative".equals(mode)) {
            runNegativeGuard();
            System.exit(0);
        }

        System.out.println("Unknown mode: " + mode);
        System.exit(2);
    }

    private static void runResolve(File esa, String finalName) throws Exception {
        List<BundleArchive> standaloneBundles = readStandaloneBundles(esa, finalName);
        if (standaloneBundles.isEmpty()) {
            System.out.println("No standalone dependency bundles found in ESA " + esa.getAbsolutePath());
            return;
        }

        Framework framework = startFramework("target/felix-resolve-" + UUID.randomUUID().toString());
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
            stopFramework(framework);
        }
    }

    private static void runNegativeGuard() throws Exception {
        Framework framework = startFramework("target/felix-resolve-negative-" + UUID.randomUUID().toString());
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
            stopFramework(framework);
        }
    }

    private static List<BundleArchive> readStandaloneBundles(File esa, String finalName) throws IOException {
        List<BundleArchive> bundles = new ArrayList<BundleArchive>();
        JarFile esaJar = new JarFile(esa);
        try {
            Enumeration<JarEntry> entries = esaJar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (entry.isDirectory() || !entry.getName().endsWith(".jar")) {
                    continue;
                }
                if (isProjectOwnedBundle(entry.getName(), finalName)) {
                    continue;
                }

                byte[] content = readAllBytes(esaJar.getInputStream(entry));
                Manifest manifest = readNestedManifest(content);
                if (manifest == null) {
                    continue;
                }
                Attributes attributes = manifest.getMainAttributes();
                if (!hasText(attributes.getValue(BUNDLE_SYMBOLIC_NAME))) {
                    continue;
                }
                bundles.add(new BundleArchive(entry.getName(), content));
            }
        } finally {
            esaJar.close();
        }
        return bundles;
    }

    private static boolean isProjectOwnedBundle(String entryName, String finalName) {
        if (!hasText(entryName)) {
            return false;
        }
        String lower = entryName.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".monitor.jar")) {
            return true;
        }
        return hasText(finalName) && entryName.endsWith(finalName + ".jar");
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

    private static Framework startFramework(String storagePath) throws BundleException {
        FrameworkFactory factory = findFrameworkFactory();
        Map<String, String> config = new HashMap<String, String>();
        config.put(Constants.FRAMEWORK_STORAGE, storagePath);
        config.put(Constants.FRAMEWORK_STORAGE_CLEAN, Constants.FRAMEWORK_STORAGE_CLEAN_ONFIRSTINIT);
        config.put(Constants.FRAMEWORK_BOOTDELEGATION, "");
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

    private static String unresolvedDiagnostics(List<Bundle> bundles) {
        Map<String, List<String>> availableExports = collectExportedPackages(bundles);
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
                List<String> exporters = availableExports.get(pkg);
                diagnostic.append("  import ").append(pkg).append(" -> ");
                if (exporters == null || exporters.isEmpty()) {
                    diagnostic.append("UNRESOLVED (no exporter in standalone ESA bundles/system)");
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
            imports.addAll(clause.packageNames);
        }
        return imports;
    }

    private static ImportClause parseImportClause(String clause) {
        String[] segments = clause.split(";");
        List<String> packageNames = new ArrayList<String>();
        boolean optional = false;
        for (int i = 0; i < segments.length; i++) {
            String segment = segments[i].trim();
            if (segment.length() == 0) {
                continue;
            }
            if (segment.indexOf('=') >= 0) {
                if (segment.startsWith("resolution:=")) {
                    optional = "optional".equals(unquote(segment.substring("resolution:=".length()).trim()));
                }
                continue;
            }
            packageNames.add(segment);
        }
        return new ImportClause(packageNames, optional);
    }

    private static String firstPathSegment(String clause) {
        int semicolon = clause.indexOf(';');
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

        private BundleArchive(String entryName, byte[] content) {
            this.entryName = entryName;
            this.content = content;
        }
    }

    private static final class ImportClause {
        private final List<String> packageNames;
        private final boolean optional;

        private ImportClause(List<String> packageNames, boolean optional) {
            this.packageNames = packageNames;
            this.optional = optional;
        }
    }
}
