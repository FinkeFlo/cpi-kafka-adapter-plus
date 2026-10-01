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
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * The sender's Error Handling option as the CPI UI offers it. iFlows on the 1.3 line have no such
 * field and run with the Java default, iFlows on 1.4 with the metadata default: both must be Retry,
 * and every value the dropdown offers must deploy.
 */
public class SenderErrorHandlingMetadataTest {

    private static final File SENDER_1_4 = new File("src/main/resources/metadata/metadata-sender-1.4.0.xml");

    @Test
    public void metadataDefaultIsTheRuntimeDefault() throws Exception {
        Element attribute = errorHandlingAttribute();

        Assert.assertEquals("RETRY", text(attribute, "Default"));
        Assert.assertEquals(new CpiKafkaPlusEndpoint().getErrorHandling(), text(attribute, "Default"));
    }

    @Test
    public void everyOfferedValueDeploys() throws Exception {
        List<String> values = fixedValues(errorHandlingAttribute());
        Assert.assertEquals(2, values.size());

        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
            ctx.start();
            for (String value : values) {
                CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                        "cpi-kafka-plus:orders?bootstrapServers=localhost:9092&groupId=g"
                        + "&securityProtocol=PLAINTEXT&errorHandling=" + value);
                CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, exchange -> { });
                try {
                    consumer.doStart();
                } finally {
                    consumer.doStop();
                }
            }
        }
    }

    private static Element errorHandlingAttribute() throws Exception {
        Assert.assertTrue("sender 1.4 metadata not found at " + SENDER_1_4.getAbsolutePath(), SENDER_1_4.isFile());
        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(SENDER_1_4);
        NodeList attributes = doc.getElementsByTagName("AttributeMetadata");
        for (int i = 0; i < attributes.getLength(); i++) {
            Element attribute = (Element) attributes.item(i);
            if ("errorHandling".equals(text(attribute, "Name"))) {
                return attribute;
            }
        }
        throw new AssertionError("no AttributeMetadata 'errorHandling' in " + SENDER_1_4.getName());
    }

    private static List<String> fixedValues(Element attribute) {
        List<String> values = new ArrayList<>();
        NodeList fixed = attribute.getElementsByTagName("FixedValue");
        for (int i = 0; i < fixed.getLength(); i++) {
            values.add(text((Element) fixed.item(i), "Value"));
        }
        return values;
    }

    private static String text(Element parent, String tag) {
        NodeList nodes = parent.getElementsByTagName(tag);
        return nodes.getLength() > 0 ? nodes.item(0).getTextContent().trim() : null;
    }
}
