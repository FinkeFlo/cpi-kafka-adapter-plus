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

import java.util.Locale;

import org.junit.Assert;
import org.junit.Test;

/**
 * The patterns of <em>Allowed Headers</em> ({@code allowedHeaders}), and the credential headers that
 * no wildcard lets through (#195).
 */
public class HeaderFilterStrategyTest {

    private static final String[] CREDENTIAL_HEADERS = {
            "Authorization", "Proxy-Authorization", "Cookie", "Set-Cookie"};

    @Test
    public void anEmptyPatternSendsNoHeader() {
        Assert.assertFalse(HeaderFilterStrategy.isHeaderAllowed("SAP_MessageProcessingLogID", null));
        Assert.assertFalse(HeaderFilterStrategy.isHeaderAllowed("SAP_MessageProcessingLogID", ""));
        Assert.assertFalse(HeaderFilterStrategy.isHeaderAllowed("SAP_MessageProcessingLogID", "  "));
    }

    @Test
    public void aStarSendsEveryHeaderButTheCredentialHeaders() {
        Assert.assertTrue(HeaderFilterStrategy.isHeaderAllowed("SAP_MessageProcessingLogID", "*"));
        Assert.assertTrue(HeaderFilterStrategy.isHeaderAllowed("orderId", " * "));
        Assert.assertTrue("a name that only contains a credential header's name is no credential header",
                HeaderFilterStrategy.isHeaderAllowed("X-Authorization-Hint", "*"));
        for (String name : CREDENTIAL_HEADERS) {
            Assert.assertFalse(name, HeaderFilterStrategy.isHeaderAllowed(name, "*"));
            Assert.assertFalse(name, HeaderFilterStrategy.isHeaderAllowed(name.toUpperCase(Locale.ROOT), "*"));
            Assert.assertFalse(name, HeaderFilterStrategy.isHeaderAllowed(name.toLowerCase(Locale.ROOT), "*"));
        }
    }

    @Test
    public void noOtherWildcardSendsACredentialHeaderEither() {
        Assert.assertFalse(HeaderFilterStrategy.isHeaderAllowed("Authorization", "Auth*"));
        Assert.assertFalse(HeaderFilterStrategy.isHeaderAllowed("Set-Cookie", "*cookie"));
        Assert.assertFalse(HeaderFilterStrategy.isHeaderAllowed("Proxy-Authorization", "*Author*"));
        Assert.assertFalse(HeaderFilterStrategy.isHeaderAllowed("Cookie", "SAP_*|*"));
    }

    @Test
    public void aCredentialHeaderNamedExactlyIsSent() {
        for (String name : CREDENTIAL_HEADERS) {
            Assert.assertTrue(name, HeaderFilterStrategy.isHeaderAllowed(name, name));
            Assert.assertTrue(name, HeaderFilterStrategy.isHeaderAllowed(name, "*|" + name.toLowerCase(Locale.ROOT)));
            Assert.assertTrue(name, HeaderFilterStrategy.isHeaderAllowed(name.toUpperCase(Locale.ROOT),
                    "SAP_*| " + name + " "));
        }
    }

    @Test
    public void patternsMatchExactlyByPrefixBySuffixOrByAPartInAnyLetterCase() {
        Assert.assertTrue(HeaderFilterStrategy.isHeaderAllowed("MyHeader", "myheader"));
        Assert.assertFalse(HeaderFilterStrategy.isHeaderAllowed("MyHeader2", "MyHeader"));
        Assert.assertTrue(HeaderFilterStrategy.isHeaderAllowed("SAP_Sender", "sap_*"));
        Assert.assertFalse(HeaderFilterStrategy.isHeaderAllowed("XSAP_Sender", "SAP_*"));
        Assert.assertTrue(HeaderFilterStrategy.isHeaderAllowed("orderId", "*ID"));
        Assert.assertFalse(HeaderFilterStrategy.isHeaderAllowed("orderIdx", "*Id"));
        Assert.assertTrue(HeaderFilterStrategy.isHeaderAllowed("newOrderNumber", "*order*"));
        Assert.assertFalse(HeaderFilterStrategy.isHeaderAllowed("invoice", "*order*"));
        Assert.assertTrue("a later pattern of the list matches",
                HeaderFilterStrategy.isHeaderAllowed("tenant", "SAP_*|| tenant "));
        Assert.assertFalse(HeaderFilterStrategy.isHeaderAllowed("tenant", "SAP_*|MyHeader"));
    }
}
