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
 * Verifies that the shipped ESA resolves in a real OSGi runtime, in an isolated JVM.
 * <p>
 * Isolation avoids classpath interference from provided SAP dependencies while still checking the
 * real resolver behavior of the produced ESA content. The suite covers four properties:
 * <ol>
 * <li>the ESA ships the expected number of third-party bundles (baseline zero);</li>
 * <li>the adapter and monitor bundles resolve against the packages CPI provides;</li>
 * <li>that resolve check rejects an unsatisfiable mandatory import (mutation probe);</li>
 * <li>the harness itself can fail (negative guard).</li>
 * </ol>
 * Every assertion checks a marker in the runner output on top of the exit code. Exit code alone is
 * what let the previous version of this test pass without resolving a single bundle (issue #156).
 */
public class OsgiFrameworkResolveIT {

    /**
     * The number of third-party bundles the ESA is expected to ship alongside the project's own.
     * <p>
     * Zero is the intended steady state: every runtime dependency is embedded in the fat bundle or
     * excluded from {@code copy-dependencies}. The baseline is asserted rather than tolerated, so
     * a dependency that slips past {@code excludeGroupIds} - the way {@code at.yawk.lz4:lz4-java}
     * once did - is reported instead of silently passing (issue #156).
     */
    private static final int EXPECTED_STANDALONE_BUNDLES = 0;

    @Test
    public void esaShipsTheExpectedNumberOfStandaloneBundles() throws Exception {
        File esa = requireBuiltEsa();
        if (esa == null) {
            return;
        }

        RunnerResult result = runResolver("standalone", esa.getAbsolutePath());
        Assert.assertEquals("OSGi resolver failed for ESA standalone bundles:\n" + result.output, 0, result.exitCode);
        assertOutputContains(result, "standaloneBundles=" + EXPECTED_STANDALONE_BUNDLES,
                "The ESA no longer ships the expected number of standalone bundles. A transitive"
                        + " dependency likely slipped past the copy-dependencies excludeGroupIds in pom.xml."
                        + " Verify it carries a CPI-resolvable manifest, then update"
                        + " EXPECTED_STANDALONE_BUNDLES.");
    }

    @Test
    public void shippedAdapterBundleResolvesAgainstCpiPlatformPackages() throws Exception {
        File esa = requireBuiltEsa();
        if (esa == null) {
            return;
        }

        RunnerResult result = runResolver("adapter", esa.getAbsolutePath());
        Assert.assertEquals("The shipped adapter bundle does not resolve against the packages CPI"
                + " provides:\n" + result.output, 0, result.exitCode);
        assertOutputContains(result, "Resolved project bundles: 2",
                "Expected the adapter bundle and the ADK monitor bundle to be installed and resolved.");
    }

    /**
     * Keeps {@link #shippedAdapterBundleResolvesAgainstCpiPlatformPackages} honest: the same
     * shipped bundle with one unsatisfiable mandatory import must not resolve. Without this probe
     * the resolve check could degrade into a test that cannot fail.
     */
    @Test
    public void adapterResolveCatchesAnUnsatisfiableMandatoryImport() throws Exception {
        File esa = requireBuiltEsa();
        if (esa == null) {
            return;
        }

        RunnerResult result = runResolver("mutation", esa.getAbsolutePath());
        Assert.assertEquals("Mutation probe failed:\n" + result.output, 0, result.exitCode);
        assertOutputContains(result, "mutationProbe=detected",
                "The adapter resolve check did not reject an unsatisfiable mandatory import.");
    }

    @Test
    public void resolverHarnessFailsForUnresolvableBundle() throws Exception {
        RunnerResult result = runResolver("negative");
        Assert.assertEquals("Negative resolver guard failed:\n" + result.output, 0, result.exitCode);
        assertOutputContains(result, "Negative guard passed.", "Negative guard did not run.");
    }

    private static void assertOutputContains(RunnerResult result, String expected, String explanation) {
        Assert.assertTrue(explanation + "\nExpected runner output to contain: " + expected + "\nActual output:\n"
                + result.output, result.output.contains(expected));
    }

    /**
     * @return the built ESA, or {@code null} when the build path does not produce one and the
     *         caller should skip.
     */
    private static File requireBuiltEsa() {
        File esa = locateBuiltEsa();
        if (esa == null) {
            Assume.assumeTrue(
                    "No ESA found under target/. Run this test in an ESA-producing build path (e.g. mvn install).",
                    false);
        }
        return esa;
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
        if (esaCandidates.size() == 1) {
            return esaCandidates.get(0);
        }

        StringBuilder message = new StringBuilder();
        message.append("Multiple ESA archives found under ");
        message.append(targetDirectory.getAbsolutePath());
        message.append(" (ambiguous build state):");
        for (int i = 0; i < esaCandidates.size(); i++) {
            message.append('\n');
            message.append(esaCandidates.get(i).getAbsolutePath());
        }
        Assert.fail(message.toString());
        return null;
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
