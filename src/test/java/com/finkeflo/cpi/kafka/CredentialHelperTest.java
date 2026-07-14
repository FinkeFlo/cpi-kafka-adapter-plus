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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class CredentialHelperTest {

    @Test
    public void testToStringAndZeroReturnsCorrectPassword() {
        char[] passwordChars = "s3cr3t".toCharArray();
        String result = CredentialHelper.toStringAndZero(passwordChars);
        Assert.assertEquals("s3cr3t", result);
    }

    @Test
    public void testToStringAndZeroWipesOriginalArray() {
        char[] passwordChars = "s3cr3t".toCharArray();
        CredentialHelper.toStringAndZero(passwordChars);

        char[] expectedWiped = new char[passwordChars.length];
        Arrays.fill(expectedWiped, ' ');
        Assert.assertArrayEquals(
                "password char[] must be zeroed out after being copied into a String",
                expectedWiped, passwordChars);
    }

    @Test
    public void testToStringAndZeroHandlesNull() {
        Assert.assertNull(CredentialHelper.toStringAndZero(null));
    }

    @Test
    public void testToStringAndZeroHandlesEmptyArray() {
        char[] passwordChars = new char[0];
        String result = CredentialHelper.toStringAndZero(passwordChars);
        Assert.assertEquals("", result);
    }
}
