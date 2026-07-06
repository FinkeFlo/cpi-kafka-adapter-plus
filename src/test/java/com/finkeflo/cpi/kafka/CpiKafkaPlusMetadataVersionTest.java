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
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

/**
 * Guards the SAP-conform multi-version metadata model (spec 2026-06-30):
 *  - each metadata file declares exactly ONE version (one variant version per file),
 *  - the highest metadata version equals config.adk Adapter-Version.
 */
public class CpiKafkaPlusMetadataVersionTest {

    private static final Pattern VERSION_MARKER =
            Pattern.compile("version::(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern ADAPTER_VERSION =
            Pattern.compile("(?m)^Adapter-Version=(\\d+\\.\\d+\\.\\d+)\\s*$");

    @Test
    public void eachMetadataFileHasExactlyOneVersion() throws IOException {
        for (File f : metadataFiles()) {
            Set<String> versions = versionsIn(f);
            Assert.assertEquals(
                "metadata file " + f.getName() + " must declare exactly ONE version, found " + versions,
                1, versions.size());
        }
    }

    @Test
    public void highestMetadataVersionMatchesConfigAdk() throws IOException {
        String configVersion = adapterVersionFromConfigAdk();
        String highest = highestMetadataVersion();
        Assert.assertEquals(
            "config.adk Adapter-Version must equal the highest metadata version",
            configVersion, highest);
    }

    /**
     * Frozen-file guard: any metadata file whose version is BELOW the current config.adk
     * Adapter-Version is "released" and must never change. Its SHA-256 is recorded (grow-only)
     * in {@code src/test/resources/released-metadata-checksums.txt}. A frozen file that is
     * missing from the manifest, or whose content changed, fails the build.
     */
    @Test
    public void releasedMetadataFilesUnchanged() throws Exception {
        String configVersion = adapterVersionFromConfigAdk();
        Map<String, String> checksums = loadReleasedChecksums();
        for (File f : metadataFiles()) {
            String fileVersion = versionsIn(f).iterator().next();
            if (compareSemver(fileVersion, configVersion) >= 0) {
                continue; // current line — still editable (micro bumps happen in place)
            }
            String expected = checksums.get(f.getName());
            Assert.assertNotNull(
                "released metadata file " + f.getName() + " (version " + fileVersion
                    + " < config.adk " + configVersion + ") is not recorded in " + CHECKSUMS_FILE
                    + " — add its SHA-256 to freeze it",
                expected);
            Assert.assertEquals(
                "released metadata file " + f.getName()
                    + " was modified — frozen files must never change (would break existing iFlows)",
                expected, sha256(f));
        }
    }

    private static File[] metadataFiles() {
        File dir = new File("src/main/resources/metadata");
        Assert.assertTrue("metadata dir not found at " + dir.getAbsolutePath(), dir.isDirectory());
        File[] files = dir.listFiles(new FilenameFilter() {
            public boolean accept(File d, String name) { return name.endsWith(".xml"); }
        });
        Assert.assertNotNull(files);
        Assert.assertTrue("no metadata .xml files found", files.length > 0);
        return files;
    }

    private static Set<String> versionsIn(File f) throws IOException {
        String text = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
        Matcher m = VERSION_MARKER.matcher(text);
        Set<String> out = new LinkedHashSet<String>();
        while (m.find()) {
            out.add(m.group(1) + "." + m.group(2) + "." + m.group(3));
        }
        Assert.assertFalse("no version:: marker in " + f.getName(), out.isEmpty());
        return out;
    }

    private static String highestMetadataVersion() throws IOException {
        List<String> all = new ArrayList<String>();
        for (File f : metadataFiles()) {
            all.addAll(versionsIn(f));
        }
        String highest = all.get(0);
        for (String v : all) {
            if (compareSemver(v, highest) > 0) {
                highest = v;
            }
        }
        return highest;
    }

    private static String adapterVersionFromConfigAdk() throws IOException {
        File cfg = new File("config.adk");
        Assert.assertTrue("config.adk not found at " + cfg.getAbsolutePath(), cfg.isFile());
        String text = new String(Files.readAllBytes(cfg.toPath()), StandardCharsets.UTF_8);
        Matcher m = ADAPTER_VERSION.matcher(text);
        Assert.assertTrue("Adapter-Version not found in config.adk", m.find());
        return m.group(1);
    }

    private static int compareSemver(String a, String b) {
        String[] pa = a.split("\\.");
        String[] pb = b.split("\\.");
        for (int i = 0; i < 3; i++) {
            int d = Integer.parseInt(pa[i]) - Integer.parseInt(pb[i]);
            if (d != 0) return d;
        }
        return 0;
    }

    private static final String CHECKSUMS_FILE = "src/test/resources/released-metadata-checksums.txt";

    private static Map<String, String> loadReleasedChecksums() throws IOException {
        Map<String, String> out = new HashMap<String, String>();
        File f = new File(CHECKSUMS_FILE);
        if (!f.isFile()) {
            return out;
        }
        String text = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
        for (String line : text.split("\n")) {
            String s = line.trim();
            if (s.isEmpty() || s.startsWith("#")) {
                continue;
            }
            int eq = s.indexOf('=');
            if (eq > 0) {
                out.put(s.substring(0, eq).trim(), s.substring(eq + 1).trim());
            }
        }
        return out;
    }

    private static String sha256(File f) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] hash = md.digest(Files.readAllBytes(f.toPath()));
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
