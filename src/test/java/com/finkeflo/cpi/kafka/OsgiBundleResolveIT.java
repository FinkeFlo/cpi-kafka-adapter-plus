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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies the packaged OSGi bundle instead of only the plain JVM test classpath.
 */
public class OsgiBundleResolveIT {

    private static final String BUNDLE_SYMBOLIC_NAME = "Bundle-SymbolicName";
    private static final String BUNDLE_VERSION = "Bundle-Version";
    private static final String BUNDLE_CLASSPATH = "Bundle-ClassPath";
    private static final String IMPORT_PACKAGE = "Import-Package";

    @Test
    public void builtBundleHasRequiredOsgiMetadataAndEmbeddedRuntimePackages() throws Exception {
        File bundle = locateBuiltBundle();
        JarFile bundleJar = new JarFile(bundle);
        try {
            Manifest manifest = bundleJar.getManifest();
            Assert.assertNotNull("Bundle manifest is missing in " + bundle.getAbsolutePath(), manifest);

            Attributes attributes = manifest.getMainAttributes();
            assertManifestHeaderPresent(attributes, BUNDLE_SYMBOLIC_NAME);
            assertManifestHeaderPresent(attributes, BUNDLE_VERSION);

            assertBundleContainsResource(bundleJar, attributes,
                    "org/apache/kafka/clients/producer/KafkaProducer.class", "org.apache.kafka.clients");
            assertBundleContainsResource(bundleJar, attributes,
                    "org/apache/avro/Schema.class", "org.apache.avro");
            assertBundleContainsResource(bundleJar, attributes,
                    "com/fasterxml/jackson/databind/ObjectMapper.class", "com.fasterxml.jackson.databind");
            assertBundleContainsResource(bundleJar, attributes,
                    "io/confluent/kafka/schemaregistry/client/SchemaRegistryClient.class", "io.confluent");

            assertNoMandatoryImport(attributes, "org.apache.kafka");
            assertNoMandatoryImport(attributes, "org.apache.avro");
            assertNoMandatoryImport(attributes, "com.fasterxml.jackson");
        } finally {
            bundleJar.close();
        }
    }

    private static File locateBuiltBundle() throws IOException {
        File targetDirectory = new File("target");
        Assert.assertTrue("Maven target directory does not exist: " + targetDirectory.getAbsolutePath(),
                targetDirectory.isDirectory());

        // Preferred path: Failsafe injects the current build's finalName, so we validate exactly
        // the bundle produced by this build and ignore stale/renamed artifacts (e.g. a leftover
        // JAR from a previous artifactId or version) that may linger in target/ without a clean.
        String finalName = System.getProperty("project.build.finalName");
        if (hasText(finalName)) {
            File bundle = new File(targetDirectory, finalName + ".jar");
            Assert.assertTrue("Built OSGi bundle for the current build was not found: "
                    + bundle.getAbsolutePath(), bundle.isFile());
            assertHasBundleSymbolicName(bundle);
            return bundle;
        }

        File[] jarFiles = targetDirectory.listFiles();
        Assert.assertNotNull("Unable to list Maven target directory: " + targetDirectory.getAbsolutePath(), jarFiles);
        Arrays.sort(jarFiles);

        List<File> bundleCandidates = new ArrayList<File>();
        for (int i = 0; i < jarFiles.length; i++) {
            File jarFile = jarFiles[i];
            if (!isCandidateJar(jarFile)) {
                continue;
            }

            JarFile candidateJar = new JarFile(jarFile);
            try {
                Manifest manifest = candidateJar.getManifest();
                if (manifest != null && hasText(manifest.getMainAttributes().getValue(BUNDLE_SYMBOLIC_NAME))) {
                    bundleCandidates.add(jarFile);
                }
            } finally {
                candidateJar.close();
            }
        }

        Assert.assertEquals("Expected exactly one built OSGi bundle JAR with a Bundle-SymbolicName in "
                + targetDirectory.getAbsolutePath() + " but found: " + bundleCandidates, 1, bundleCandidates.size());
        return bundleCandidates.get(0);
    }

    private static void assertHasBundleSymbolicName(File bundle) throws IOException {
        JarFile bundleJar = new JarFile(bundle);
        try {
            Manifest manifest = bundleJar.getManifest();
            Assert.assertTrue("Built OSGi bundle is missing a " + BUNDLE_SYMBOLIC_NAME + " header: "
                    + bundle.getAbsolutePath(),
                    manifest != null && hasText(manifest.getMainAttributes().getValue(BUNDLE_SYMBOLIC_NAME)));
        } finally {
            bundleJar.close();
        }
    }

    private static boolean isCandidateJar(File file) {
        if (!file.isFile()) {
            return false;
        }
        String name = file.getName();
        return name.endsWith(".jar") && !name.endsWith("-sources.jar") && !name.endsWith("-javadoc.jar");
    }

    private static void assertManifestHeaderPresent(Attributes attributes, String headerName) {
        Assert.assertTrue("Manifest header " + headerName + " is missing or empty", hasText(attributes.getValue(headerName)));
    }

    private static void assertBundleContainsResource(JarFile bundleJar, Attributes attributes, String resourcePath,
            String packageName) throws IOException {
        Assert.assertTrue("Expected embedded package " + packageName + " to be present as resource " + resourcePath
                + " either inline in the bundle or inside an embedded JAR listed on Bundle-ClassPath",
                containsResource(bundleJar, attributes, resourcePath));
    }

    private static boolean containsResource(JarFile bundleJar, Attributes attributes, String resourcePath)
            throws IOException {
        if (bundleJar.getEntry(resourcePath) != null) {
            return true;
        }

        List<String> classPathEntries = splitHeaderClauses(attributes.getValue(BUNDLE_CLASSPATH));
        for (int i = 0; i < classPathEntries.size(); i++) {
            String classPathEntry = firstPathSegment(classPathEntries.get(i));
            if (".".equals(classPathEntry) || classPathEntry.length() == 0 || !classPathEntry.endsWith(".jar")) {
                continue;
            }
            if (nestedJarContains(bundleJar, classPathEntry, resourcePath)) {
                return true;
            }
        }
        return false;
    }

    private static boolean nestedJarContains(JarFile bundleJar, String nestedJarPath, String resourcePath)
            throws IOException {
        JarEntry nestedJarEntry = bundleJar.getJarEntry(nestedJarPath);
        if (nestedJarEntry == null) {
            return false;
        }

        InputStream inputStream = bundleJar.getInputStream(nestedJarEntry);
        try {
            JarInputStream nestedJar = new JarInputStream(inputStream);
            try {
                JarEntry entry;
                while ((entry = nestedJar.getNextJarEntry()) != null) {
                    if (resourcePath.equals(entry.getName())) {
                        return true;
                    }
                }
            } finally {
                nestedJar.close();
            }
        } finally {
            inputStream.close();
        }
        return false;
    }

    private static void assertNoMandatoryImport(Attributes attributes, String packagePrefix) {
        String importPackageHeader = attributes.getValue(IMPORT_PACKAGE);
        if (!hasText(importPackageHeader)) {
            return;
        }

        List<String> clauses = splitHeaderClauses(importPackageHeader);
        for (int i = 0; i < clauses.size(); i++) {
            ImportClause clause = parseImportClause(clauses.get(i));
            if (clause.optional) {
                continue;
            }

            for (int j = 0; j < clause.packageNames.size(); j++) {
                String packageName = clause.packageNames.get(j);
                Assert.assertFalse("Manifest must not declare mandatory Import-Package for embedded package "
                        + packagePrefix + "; offending clause: " + clauses.get(i),
                        packageName.equals(packagePrefix) || packageName.startsWith(packagePrefix + "."));
            }
        }
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
            } else {
                packageNames.add(segment);
            }
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

    private static boolean hasText(String value) {
        return value != null && value.trim().length() > 0;
    }

    private static String unquote(String value) {
        if (value.length() >= 2 && value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
            return value.substring(1, value.length() - 1);
        }
        return value;
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
