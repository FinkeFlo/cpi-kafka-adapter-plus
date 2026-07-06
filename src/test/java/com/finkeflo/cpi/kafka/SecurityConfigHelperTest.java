/*-
 * #%L
 * SAP CPI Kafka Adapter Plus
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

public class SecurityConfigHelperTest {

    @Test
    public void testBuildJaasConfigPlain() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user", "pass");
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";",
                result);
    }

    @Test
    public void testBuildJaasConfigScramSha256() {
        String result = SecurityConfigHelper.buildJaasConfig("SCRAM-SHA-256", "user", "pass");
        Assert.assertTrue(result.startsWith("org.apache.kafka.common.security.scram.ScramLoginModule"));
    }

    @Test
    public void testBuildJaasConfigScramSha512() {
        String result = SecurityConfigHelper.buildJaasConfig("SCRAM-SHA-512", "user", "pass");
        Assert.assertTrue(result.startsWith("org.apache.kafka.common.security.scram.ScramLoginModule"));
    }

    @Test
    public void testBuildJaasConfigEscapesDoubleQuotes() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user", "pass\"word");
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\\\"word\";",
                result);
    }

    @Test
    public void testBuildJaasConfigEscapesBackslashes() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user", "pass\\word");
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\\\\word\";",
                result);
    }

    @Test
    public void testBuildJaasConfigEscapesBackslashBeforeQuote() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user", "pass\\\"end");
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\\\\\\\"end\";",
                result);
    }

    @Test
    public void testBuildJaasConfigEscapesUsernameSpecialChars() {
        String result = SecurityConfigHelper.buildJaasConfig("PLAIN", "user\"name", "pass");
        Assert.assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\\\"name\" password=\"pass\";",
                result);
    }
}
