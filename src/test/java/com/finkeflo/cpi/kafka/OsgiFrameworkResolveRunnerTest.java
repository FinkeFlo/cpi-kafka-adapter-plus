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

import org.junit.Assert;
import org.junit.Test;

public class OsgiFrameworkResolveRunnerTest {

    @Test
    public void adapterAndMonitorBundlesAreProjectOwned() {
        Assert.assertTrue(OsgiFrameworkResolveRunner
                .isProjectOwnedBundle("com.finkeflo.cpi.kafka.cpi-kafka-adapter-plus"));
        Assert.assertTrue(OsgiFrameworkResolveRunner.isProjectOwnedBundle("com.finkeflo.cpi.kafka"));
        Assert.assertTrue(OsgiFrameworkResolveRunner.isProjectOwnedBundle("kafkaAdapterPlus_FlorianKube.monitor"));
    }

    @Test
    public void symbolicNameAttributesAreIgnored() {
        Assert.assertTrue(OsgiFrameworkResolveRunner
                .isProjectOwnedBundle("com.finkeflo.cpi.kafka.cpi-kafka-adapter-plus;singleton:=true"));
        Assert.assertTrue(OsgiFrameworkResolveRunner
                .isProjectOwnedBundle("  kafkaAdapterPlus_FlorianKube.monitor ;singleton:=true"));
    }

    @Test
    public void thirdPartyBundlesAreNotProjectOwned() {
        Assert.assertFalse(OsgiFrameworkResolveRunner.isProjectOwnedBundle("org.apache.kafka.kafka-clients"));
        Assert.assertFalse(OsgiFrameworkResolveRunner.isProjectOwnedBundle("org.lz4.lz4-java"));
        Assert.assertFalse(OsgiFrameworkResolveRunner.isProjectOwnedBundle("org.xerial.snappy.snappy-java"));
    }

    @Test
    public void lookalikeNamesAreNotProjectOwned() {
        // Prefix must end at a package separator, not at an arbitrary character.
        Assert.assertFalse(OsgiFrameworkResolveRunner.isProjectOwnedBundle("com.finkeflo.cpi.kafkaesque"));
        // A third-party bundle whose jar is named like ours must no longer be misclassified.
        Assert.assertFalse(OsgiFrameworkResolveRunner.isProjectOwnedBundle("org.example.monitoring"));
    }

    @Test
    public void missingSymbolicNameIsNotProjectOwned() {
        Assert.assertFalse(OsgiFrameworkResolveRunner.isProjectOwnedBundle(null));
        Assert.assertFalse(OsgiFrameworkResolveRunner.isProjectOwnedBundle(""));
        Assert.assertFalse(OsgiFrameworkResolveRunner.isProjectOwnedBundle("   "));
    }
}
