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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * {@link UiLabels} against the labels the channel editor shows (#255): a deployment error that names a
 * field by a label the UI no longer has sends the user looking for something that is not there.
 */
public class UiLabelsTest {

    private static final File METADATA_DIR = new File("src/main/resources/metadata");

    @Test
    public void everyLabelIsTheOneTheUiShows() throws Exception {
        Map<String, String> sender = offeredLabels("sender");
        Map<String, String> receiver = offeredLabels("receiver");
        List<String> wrong = new ArrayList<>();
        for (Map.Entry<String, String> entry : UiLabels.all().entrySet()) {
            String param = entry.getKey();
            if (!sender.containsKey(param) && !receiver.containsKey(param)) {
                wrong.add(param + ": no channel offers it");
            }
            if (sender.containsKey(param) && !sender.get(param).equals(entry.getValue())) {
                wrong.add(param + ": '" + entry.getValue() + "', the sender shows '" + sender.get(param) + "'");
            }
            if (receiver.containsKey(param) && !receiver.get(param).equals(entry.getValue())) {
                wrong.add(param + ": '" + entry.getValue() + "', the receiver shows '" + receiver.get(param) + "'");
            }
        }
        Assert.assertTrue(String.join("\n", wrong), wrong.isEmpty());
    }

    @Test
    public void everyFieldAChannelOffersHasALabel() throws Exception {
        Set<String> offered = new HashSet<>(offeredLabels("sender").keySet());
        offered.addAll(offeredLabels("receiver").keySet());
        offered.removeAll(UiLabels.all().keySet());
        Assert.assertTrue("no label for " + offered, offered.isEmpty());
    }

    @Test
    public void aFieldIsNamedByItsLabelAndItsParameter() {
        Assert.assertEquals("'Max Poll Records' (maxPollRecords)", UiLabels.field("maxPollRecords"));
        Assert.assertEquals("noSuchParameter", UiLabels.label("noSuchParameter"));
    }

    @Test
    public void aQuotedValueShowsWhetherItIsEmptyOrHasSpacesAroundIt() {
        Assert.assertEquals("''", UiLabels.quote(null));
        Assert.assertEquals("''", UiLabels.quote(""));
        Assert.assertEquals("' orders '", UiLabels.quote(" orders "));
    }

    /**
     * The label of every field the 1.5.0 channel of {@code direction} ("sender" or "receiver") offers,
     * by parameter. A field defined in a metadata file but not referenced from a tab is not offered.
     */
    static Map<String, String> offeredLabels(String direction) throws Exception {
        File file = new File(METADATA_DIR, "metadata-" + direction + "-1.5.0.xml");
        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(file);
        Map<String, String> labels = new LinkedHashMap<>();
        NodeList definitions = doc.getElementsByTagName("AttributeMetadata");
        for (int i = 0; i < definitions.getLength(); i++) {
            Element definition = (Element) definitions.item(i);
            Element guiLabels = (Element) definition.getElementsByTagName("GuiLabels").item(0);
            labels.put(childText(definition, "Name"), childText(guiLabels, "Label"));
        }
        Map<String, String> offered = new LinkedHashMap<>();
        NodeList references = doc.getElementsByTagName("AttributeReference");
        for (int i = 0; i < references.getLength(); i++) {
            String param = childText((Element) references.item(i), "ReferenceName");
            offered.put(param, labels.get(param));
        }
        return offered;
    }

    private static String childText(Element parent, String tag) {
        for (Node child = parent.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child instanceof Element && ((Element) child).getTagName().equals(tag)) {
                return child.getTextContent().trim();
            }
        }
        return null;
    }
}
