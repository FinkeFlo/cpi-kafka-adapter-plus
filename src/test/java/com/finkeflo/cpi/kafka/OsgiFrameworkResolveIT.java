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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Verifies ESA standalone bundle resolution using a real OSGi runtime in an isolated JVM.
 * <p>
 * Isolation avoids classpath interference from provided SAP dependencies while still checking
 * the real resolver behavior of the produced ESA content.
 */
public class OsgiFrameworkResolveIT {

    @Test
    public void esaStandaloneBundlesResolveInOsgiFramework() throws Exception {
        File esa = locateBuiltEsa();
        if (esa == null) {
            Assume.assumeTrue(
                    "No ESA found under target/. Run this test in an ESA-producing build path (e.g. mvn install).",
                    false);
            return;
        }

        RunnerResult result = runResolver("resolve", esa.getAbsolutePath(),
                System.getProperty("project.build.finalName", ""));
        Assert.assertEquals("OSGi resolver failed for ESA standalone bundles:\n" + result.output, 0, result.exitCode);
    }

    @Test
    public void resolverHarnessFailsForUnresolvableBundle() throws Exception {
        RunnerResult result = runResolver("negative");
        Assert.assertEquals("Negative resolver guard failed:\n" + result.output, 0, result.exitCode);
    }

    private static File locateBuiltEsa() {
        File targetDirectory = new File("target");
        Assert.assertTrue("Maven target directory does not exist: " + targetDirectory.getAbsolutePath(),
                targetDirectory.isDirectory());

        List<File> esaCandidates = new ArrayList<File>();
        findEsaFiles(targetDirectory, esaCandidates);
        if (esaCandidates.isEmpty()) {
            if (Boolean.parseBoolean(System.getProperty("osgi.resolution.requireEsa", "false"))) {
                Assert.fail("No .esa file found under " + targetDirectory.getAbsolutePath()
                        + " (required by -Dosgi.resolution.requireEsa=true)");
            }
            return null;
        }
        java.util.Collections.sort(esaCandidates, new java.util.Comparator<File>() {
            @Override
            public int compare(File left, File right) {
                return left.getAbsolutePath().compareTo(right.getAbsolutePath());
            }
        });
        return esaCandidates.get(esaCandidates.size() - 1);
    }

    private static void findEsaFiles(File directory, List<File> collector) {
        File[] children = directory.listFiles();
        if (children == null) {
            return;
        }
        for (int i = 0; i < children.length; i++) {
            File child = children[i];
            if (child.isDirectory()) {
                findEsaFiles(child, collector);
            } else if (child.getName().endsWith(".esa")) {
                collector.add(child);
            }
        }
    }

    private static RunnerResult runResolver(String mode, String... args) throws Exception {
        String javaBinary = new File(new File(System.getProperty("java.home"), "bin"), "java").getAbsolutePath();
        List<String> command = new ArrayList<String>();
        command.add(javaBinary);
        command.add("-cp");
        command.add(filteredClasspath());
        command.add(OsgiFrameworkResolveRunner.class.getName());
        command.add(mode);
        for (int i = 0; i < args.length; i++) {
            command.add(args[i]);
        }

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.directory(new File("."));
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        String output = readFully(process.getInputStream());
        int exitCode = process.waitFor();
        return new RunnerResult(exitCode, output);
    }

    private static String filteredClasspath() {
        String classpath = System.getProperty("java.class.path", "");
        String separator = System.getProperty("path.separator");
        String[] entries = classpath.split(java.util.regex.Pattern.quote(separator));
        StringBuilder filtered = new StringBuilder();
        for (int i = 0; i < entries.length; i++) {
            String entry = entries[i];
            if (entry.contains("osgi.cmpn-")) {
                continue;
            }
            if (filtered.length() > 0) {
                filtered.append(separator);
            }
            filtered.append(entry);
        }
        return filtered.toString();
    }

    private static String readFully(InputStream input) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                output.write(buffer, 0, read);
            }
        } finally {
            input.close();
        }
        return output.toString("UTF-8");
    }

    private static final class RunnerResult {
        private final int exitCode;
        private final String output;

        private RunnerResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }
}
