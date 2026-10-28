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
import java.io.StringReader;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.utils.internals.AppInfoParser;
import org.junit.Assert;
import org.junit.Test;

/**
 * What the built bundle ships besides classes (#249): only the files the build declares, the
 * licence files under {@code META-INF/}, and Kafka's version file.
 */
public class BundleContentIT {

    /**
     * Every file in the bundle that is neither a class nor a jar on its {@code Bundle-ClassPath}.
     * Up to 1.4.1 the bundle also packed every {@code *.txt} file of the build's working tree. A new
     * entry here means every adapter installation ships one more file: review it before adding it.
     */
    private static final List<Pattern> DECLARED_FILES = Arrays.asList(
            Pattern.compile("META-INF/MANIFEST\\.MF"),
            Pattern.compile("META-INF/maven/com\\.finkeflo\\.cpi\\.kafka/cpi-kafka-adapter-plus/pom\\.(properties|xml)"),
            Pattern.compile("META-INF/services/org/apache/camel/component/cpi-kafka-plus"),
            Pattern.compile("META-INF/(LICENSE|NOTICE|THIRD-PARTY\\.txt)"),
            Pattern.compile("META-INF/kafka-clients/(LICENSE|NOTICE)"),
            Pattern.compile("kafka/kafka-version\\.properties"),
            Pattern.compile("metadata/metadata-(sender|receiver)-\\d+\\.\\d+\\.\\d+\\.xml"));

    /** A dependency in THIRD-PARTY.txt: {@code (groupId:artifactId:version - url)}. */
    private static final Pattern THIRD_PARTY_COORDINATES = Pattern.compile("\\(([^\\s:()]+):([^\\s:()]+):([^\\s:()]+) - ");

    @Test
    public void theBundleShipsOnlyDeclaredFiles() throws Exception {
        List<String> undeclared = new ArrayList<>();
        try (JarFile jar = new JarFile(locateBundleJar())) {
            Set<String> embeddedJars = bundleClassPathJars(jar);
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                String name = entries.nextElement().getName();
                if (name.endsWith("/") || name.endsWith(".class") || embeddedJars.contains(name)) {
                    continue;
                }
                if (DECLARED_FILES.stream().noneMatch(p -> p.matcher(name).matches())) {
                    undeclared.add(name);
                }
            }
        }
        Assert.assertTrue("files in the bundle that the build does not declare:\n  " + String.join("\n  ", undeclared),
                undeclared.isEmpty());
    }

    @Test
    public void theLicenceFilesAreShippedUnderMetaInf() throws Exception {
        File kafkaClients = kafkaClientsJar();
        try (JarFile jar = new JarFile(locateBundleJar()); JarFile kafka = new JarFile(kafkaClients)) {
            Assert.assertEquals(new String(Files.readAllBytes(new File("LICENSE").toPath()), StandardCharsets.UTF_8),
                    text(jar, "META-INF/LICENSE"));
            Assert.assertEquals(new String(Files.readAllBytes(new File("NOTICE").toPath()), StandardCharsets.UTF_8),
                    text(jar, "META-INF/NOTICE"));
            Assert.assertEquals(text(kafka, "LICENSE"), text(jar, "META-INF/kafka-clients/LICENSE"));
            Assert.assertEquals(text(kafka, "NOTICE"), text(jar, "META-INF/kafka-clients/NOTICE"));

            // The licence list names every library the bundle embeds: kafka-clients, whose classes
            // are unpacked into the bundle, and every jar on the Bundle-ClassPath.
            String thirdParty = text(jar, "META-INF/THIRD-PARTY.txt");
            Set<String> listed = new LinkedHashSet<>();
            Matcher m = THIRD_PARTY_COORDINATES.matcher(thirdParty);
            while (m.find()) {
                listed.add(m.group(2) + "-" + m.group(3));
            }
            List<String> embedded = new ArrayList<>(bundleClassPathJars(jar));
            embedded.add(kafkaClients.getName());
            for (String embeddedJar : embedded) {
                Assert.assertTrue(embeddedJar + " is not in META-INF/THIRD-PARTY.txt",
                        listed.stream().anyMatch(artifact -> embeddedJar.startsWith(artifact)));
            }
        }
    }

    /**
     * Kafka reads its version from {@code kafka/kafka-version.properties} next to its classes. Without
     * the file it logs a WARN for every client and reports the client version "unknown" to the
     * broker (KIP-511), as every adapter version up to 1.4.1 did.
     */
    @Test
    public void kafkaReportsItsClientVersionFromTheBundle() throws Exception {
        Properties expected = new Properties();
        try (JarFile kafka = new JarFile(kafkaClientsJar())) {
            expected.load(new StringReader(text(kafka, "kafka/kafka-version.properties")));
        }
        try (BundleContentClassLoader loader = new BundleContentClassLoader(locateBundleJar(), getClass().getClassLoader())) {
            Class<?> appInfoParser = Class.forName(AppInfoParser.class.getName(), true, loader);
            Assert.assertSame("AppInfoParser must come from the bundle", loader, appInfoParser.getClassLoader());
            Assert.assertEquals(expected.getProperty("version"), appInfoParser.getMethod("getVersion").invoke(null));
        }
    }

    private static Set<String> bundleClassPathJars(JarFile jar) throws IOException {
        Set<String> jars = new LinkedHashSet<>();
        String bundleClassPath = jar.getManifest().getMainAttributes().getValue("Bundle-ClassPath");
        Assert.assertNotNull("Bundle-ClassPath missing from manifest", bundleClassPath);
        for (String clause : bundleClassPath.split(",")) {
            String path = clause.split(";")[0].trim();
            if (!path.isEmpty() && !".".equals(path)) {
                jars.add(path);
            }
        }
        return jars;
    }

    private static String text(JarFile jar, String name) throws IOException {
        JarEntry entry = jar.getJarEntry(name);
        Assert.assertNotNull(name + " is missing from " + jar.getName(), entry);
        try (InputStream in = jar.getInputStream(entry)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * The kafka-clients jar of the test class path. Not found through {@code AppInfoParser}'s code
     * source: during the integration tests the bundle jar, which contains the Kafka classes too, comes
     * first on the class path.
     */
    private static File kafkaClientsJar() throws Exception {
        Enumeration<URL> urls = BundleContentIT.class.getClassLoader().getResources("kafka/kafka-version.properties");
        while (urls.hasMoreElements()) {
            URL url = urls.nextElement();
            if ("jar".equals(url.getProtocol())) {
                File jar = new File(((JarURLConnection) url.openConnection()).getJarFileURL().toURI());
                if (jar.getName().startsWith("kafka-clients-")) {
                    return jar;
                }
            }
        }
        throw new AssertionError("no kafka-clients jar on the test class path");
    }

    private static File locateBundleJar() {
        String finalName = System.getProperty("project.build.finalName");
        Assert.assertNotNull("project.build.finalName not set (run via failsafe)", finalName);
        File jar = new File("target", finalName + ".jar");
        Assert.assertTrue("bundle jar not built: " + jar, jar.isFile());
        return jar;
    }

    /**
     * Loads classes from the bundle first and resources only from the bundle, as a bundle class
     * loader does: the kafka-clients jar on the test class path must not supply the version file.
     */
    private static final class BundleContentClassLoader extends URLClassLoader {
        BundleContentClassLoader(File bundle, ClassLoader parent) throws IOException {
            super(new URL[] {bundle.toURI().toURL()}, parent);
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

        @Override
        public URL getResource(String name) {
            return findResource(name);
        }
    }
}
