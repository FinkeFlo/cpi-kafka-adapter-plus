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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.Test;

/**
 * Unit tests for the worker-node identity in {@code group.instance.id} (sender) and
 * {@code transactional.id} (receiver): which environment variable supplies the member suffix, and
 * the exact topic hash. The values are pinned because a different result gives every deployed
 * channel new ids. No Camel context and no broker, except for the test that checks how
 * {@link CpiKafkaPlusProducer} builds its {@code transactional.id} from them.
 */
public class NodeIdentityTest {

    /** An environment holding exactly the given name/value pairs. */
    private static UnaryOperator<String> env(String... namesAndValues) {
        Map<String, String> vars = new HashMap<>();
        for (int i = 0; i < namesAndValues.length; i += 2) {
            vars.put(namesAndValues[i], namesAndValues[i + 1]);
        }
        return vars::get;
    }

    @Test
    public void cfInstanceIndexWinsOverHostname() {
        assertEquals("3", NodeIdentity.resolveStaticMemberSuffix(
                env("CF_INSTANCE_INDEX", "3", "HOSTNAME", "worker-a")));
    }

    @Test
    public void instanceIndexZeroIsASuffix() {
        assertEquals("0", NodeIdentity.resolveStaticMemberSuffix(env("CF_INSTANCE_INDEX", "0")));
    }

    @Test
    public void hostnameIsUsedWithoutCfInstanceIndex() {
        assertEquals("worker-a", NodeIdentity.resolveStaticMemberSuffix(env("HOSTNAME", "worker-a")));
    }

    @Test
    public void emptyCfInstanceIndexFallsBackToHostname() {
        assertEquals("worker-a", NodeIdentity.resolveStaticMemberSuffix(
                env("CF_INSTANCE_INDEX", "", "HOSTNAME", "worker-a")));
    }

    @Test
    public void noSuffixWithoutEitherVariable() {
        assertNull(NodeIdentity.resolveStaticMemberSuffix(env()));
    }

    @Test
    public void emptyValuesCountAsMissing() {
        assertNull(NodeIdentity.resolveStaticMemberSuffix(env("CF_INSTANCE_INDEX", "", "HOSTNAME", "")));
    }

    @Test
    public void valuesAreUsedVerbatim() {
        // Only an empty value counts as missing: a blank one is neither trimmed nor skipped.
        assertEquals(" ", NodeIdentity.resolveStaticMemberSuffix(
                env("CF_INSTANCE_INDEX", " ", "HOSTNAME", "worker-a")));
        assertEquals("Worker-A.example", NodeIdentity.resolveStaticMemberSuffix(
                env("HOSTNAME", "Worker-A.example")));
    }

    @Test
    public void withoutArgumentTheProcessEnvironmentIsRead() {
        assertEquals(NodeIdentity.resolveStaticMemberSuffix(System::getenv),
                NodeIdentity.resolveStaticMemberSuffix());
    }

    @Test
    public void topicHashIsTheFirstFourBytesOfSha256InLowerCaseHex() {
        assertEquals("c6fb053c", NodeIdentity.computeTopicHash("test-topic"));
    }

    @Test
    public void topicHashKeepsLeadingZeros() {
        assertEquals("0c4102c8", NodeIdentity.computeTopicHash("topic-123"));
        assertEquals("0027707d", NodeIdentity.computeTopicHash("topic-518"));
    }

    @Test
    public void producerBuildsTransactionalIdFromTopicHashAndMemberSuffix() throws Exception {
        // A prefix this long fails the length check, whose message carries the example id.
        String prefix = new String(new char[240]).replace('\0', 'x');
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
            ctx.start();
            CpiKafkaPlusEndpoint endpoint = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                    "cpi-kafka-plus:test-topic?bootstrapServers=localhost:9999&securityProtocol=PLAINTEXT"
                    + "&producerBatchMode=JSON_ARRAY&enableTransactions=true&transactionalIdPrefix=" + prefix);
            CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
            try {
                producer.doStart();
                fail("Expected the length check to reject the transactional.id");
            } catch (IllegalArgumentException e) {
                String message = e.getMessage();
                int start = message.indexOf('\'') + 1;
                String id = message.substring(start, message.indexOf('\'', start));
                assertTrue(id, id.startsWith(prefix + "-c6fb053c-"));
                assertTrue(id, id.endsWith("-0"));
                String suffix = NodeIdentity.resolveStaticMemberSuffix();
                if (suffix != null) {
                    assertEquals(prefix + "-c6fb053c-" + suffix + "-0", id);
                }
            }
        }
    }
}
