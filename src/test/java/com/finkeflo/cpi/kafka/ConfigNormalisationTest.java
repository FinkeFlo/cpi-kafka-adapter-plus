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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.camel.ResolveEndpointFailedException;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.UriParam;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Values the adapter brings into the form Kafka and the CPI stores expect before it uses them (#261):
 * Kafka keywords in another case, and spaces around a number, a switch, an alias or a URL. An
 * externalized parameter bypasses the dropdown, and Camel binds a value with the spaces around it.
 */
public class ConfigNormalisationTest {

    private static final File RECEIVER_METADATA = new File("src/main/resources/metadata/metadata-receiver-1.4.0.xml");
    private static final Pattern REGEX_RESTRICTION = Pattern.compile("^Constraint\\.isValidRegex\\((.*)\\)$");
    private static final String EMPTY_PROTOCOL_REASON = "reason='empty; the Kafka client connects with PLAINTEXT, "
            + "without TLS and without authentication'";

    private DefaultCamelContext ctx;
    private int endpointCount;

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        ctx.close();
    }

    // ── Spaces ──────────────────────────────────────────────────────────────────

    @Test
    public void spacesAroundNumbersSwitchesAliasesAndUrlsAreRemoved() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint("maxPollRecords= 500&dlqEnabled=true &credentialAlias=kafka-cred "
                + "&schemaRegistryUrl= https://sr:8081 &schemaRegistryCredentialAlias= sr-cred"
                + "&dlqCredentialAlias= dlq-cred &bootstrapServers= b1:9092 , b2:9092 ,");
        Assert.assertEquals(500, e.getMaxPollRecords());
        Assert.assertTrue(e.isDlqEnabled());
        Assert.assertEquals("kafka-cred", e.getCredentialAlias());
        Assert.assertEquals("https://sr:8081", e.getSchemaRegistryUrl());
        Assert.assertEquals("sr-cred", e.getSchemaRegistryCredentialAlias());
        Assert.assertEquals("dlq-cred", e.getDlqCredentialAlias());
        Assert.assertEquals("b1:9092,b2:9092", e.getBootstrapServers());
    }

    @Test
    public void theConsumerGroupAndTheTransactionalIdPrefixKeepTheirSpaces() throws Exception {
        // Both are identities: the group owns the committed offsets, the prefix fences older producers.
        CpiKafkaPlusEndpoint e = endpoint("groupId= orders-group &transactionalIdPrefix= orders-tx ");
        Assert.assertEquals(" orders-group ", e.getGroupId());
        Assert.assertEquals(" orders-tx ", e.getTransactionalIdPrefix());
    }

    @Test
    public void everyNumberAndSwitchIsBoundWithSpacesAroundIt() throws Exception {
        // Camel failed to bind each of them, with a message that names no UI label.
        CpiKafkaPlusEndpoint defaults = new CpiKafkaPlusEndpoint();
        int checked = 0;
        for (Field field : CpiKafkaPlusEndpoint.class.getDeclaredFields()) {
            if (!field.isAnnotationPresent(UriParam.class) || !field.getType().isPrimitive()) {
                continue;
            }
            field.setAccessible(true);
            Object initial = field.get(defaults);
            String value = initial instanceof Boolean
                    ? String.valueOf(!(Boolean) initial) : String.valueOf(((Number) initial).longValue() + 1);
            CpiKafkaPlusEndpoint e = endpoint(field.getName() + "= " + value + " ");
            Assert.assertEquals(field.getName(), value, String.valueOf(field.get(e)));
            checked++;
        }
        Assert.assertTrue("found only " + checked + " numbers and switches", checked > 20);
    }

    // ── Kafka keywords ──────────────────────────────────────────────────────────

    @Test
    public void anAutoOffsetResetInAnotherCaseDeploysAndMakesAConsumerConfig() throws Exception {
        String[][] cases = {{"LATEST", "latest"}, {"Earliest", "earliest"}, {"NONE", "none"},
                {"by_duration:PT1H", "by_duration:PT1H"}};
        for (String[] c : cases) {
            CpiKafkaPlusEndpoint e = endpoint("bootstrapServers=localhost:9999&securityProtocol=PLAINTEXT"
                    + "&groupId=g&autoOffsetReset=" + c[0]);
            Assert.assertEquals(c[0], c[1], e.getAutoOffsetReset());
            Assert.assertNull(c[0], consumerRejection(e));
            ConsumerConfig config = new ConsumerConfig(consumerProperties(e));
            Assert.assertEquals(c[0], c[1], config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        }
    }

    @Test
    public void aCompressionTypeAndAcksInAnotherCaseDeployAndMakeAProducerConfig() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint("bootstrapServers=localhost:9999&securityProtocol=PLAINTEXT"
                + "&compressionType=GZIP&acks=ALL&enableIdempotence=false");
        Assert.assertEquals("gzip", e.getCompressionType());
        Assert.assertEquals("all", e.getAcks());
        Assert.assertNull(producerRejection(e));
        ProducerConfig config = new ProducerConfig(producerProperties(e));
        Assert.assertEquals("gzip", config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        Assert.assertEquals("-1", config.getString(ProducerConfig.ACKS_CONFIG));   // Kafka's form of all

        for (String codec : new String[] {"None", "SNAPPY", "Lz4", " zstd "}) {
            CpiKafkaPlusEndpoint other = endpoint("bootstrapServers=localhost:9999&securityProtocol=PLAINTEXT");
            other.setCompressionType(codec);
            new ProducerConfig(producerProperties(other));
        }
    }

    @Test
    public void anythingElseIsPassedOnUnchanged() throws Exception {
        CpiKafkaPlusEndpoint e = new CpiKafkaPlusEndpoint();
        e.setAutoOffsetReset("by_duration:PT1H");
        e.setCompressionType("brotli");
        e.setAcks("2");
        Assert.assertEquals("by_duration:PT1H", e.getAutoOffsetReset());
        Assert.assertEquals("brotli", e.getCompressionType());
        Assert.assertEquals("2", e.getAcks());
    }

    @Test
    public void anUnknownSaslMechanismIsRejectedAndAKnownOneDeploysInAnyCase() throws Exception {
        String sasl = "bootstrapServers=localhost:9999&securityProtocol=SASL_SSL&credentialAlias=kafka-credentials"
                + "&groupId=g";
        String expected = "'SASL Mechanism' (saslMechanism) must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512 while "
                + "'Security Protocol' (securityProtocol) is SASL_SSL, got: 'OAUTHBEARER'. The adapter can log in "
                + "with no other mechanism.";
        Assert.assertEquals(expected, consumerRejection(endpoint(sasl + "&saslMechanism=OAUTHBEARER")));
        Assert.assertEquals(expected, producerRejection(endpoint(sasl + "&saslMechanism=oauthbearer")));
        Assert.assertNull(consumerRejection(endpoint(sasl + "&saslMechanism=scram-sha-512")));
        // Without SASL the field is hidden and has no effect (#174).
        Assert.assertNull(consumerRejection(endpoint("bootstrapServers=localhost:9999&securityProtocol=SSL"
                + "&groupId=g&saslMechanism=OAUTHBEARER")));
    }

    @Test
    public void anEmptySecurityProtocolIsLoggedOnceAtStartAndDeploys() throws Exception {
        // It works against a PLAINTEXT broker, so it is not rejected.
        CpiKafkaPlusEndpoint sender = endpoint("bootstrapServers=localhost:9999&securityProtocol=&groupId=g");
        Assert.assertEquals("", sender.getSecurityProtocol());
        String senderLog = ownLines(capture(() -> Assert.assertNull(consumerRejection(sender))));
        assertLoggedOnce(senderLog, "ERROR " + CpiKafkaPlusConsumer.class.getName() + " - [CPI-KAFKA-PLUS-DIAG] "
                + "consumer.config.ineffective parameter=securityProtocol value='' " + EMPTY_PROTOCOL_REASON);

        CpiKafkaPlusEndpoint receiver = endpoint("bootstrapServers=localhost:9999&securityProtocol=");
        String receiverLog = ownLines(capture(() -> Assert.assertNull(producerRejection(receiver))));
        assertLoggedOnce(receiverLog, "ERROR " + CpiKafkaPlusProducer.class.getName() + " - [CPI-KAFKA-PLUS-DIAG] "
                + "producer.config.ineffective parameter=securityProtocol value='' " + EMPTY_PROTOCOL_REASON);

        CpiKafkaPlusEndpoint plaintext = endpoint("bootstrapServers=localhost:9999&securityProtocol=PLAINTEXT"
                + "&groupId=g");
        String plaintextLog = ownLines(capture(() -> Assert.assertNull(consumerRejection(plaintext))));
        Assert.assertFalse(plaintextLog, plaintextLog.contains("parameter=securityProtocol"));
    }

    @Test
    public void theSubjectNameStrategyIsCaseInsensitiveAtStartAndPerMessage() throws Exception {
        CpiKafkaPlusEndpoint e = endpoint("bootstrapServers=localhost:9999&securityProtocol=PLAINTEXT"
                + "&schemaRegistryEnabled=true&schemaRegistryUrl=http://localhost:1&avroValueSerialization=true"
                + "&subjectNameStrategy=topicnamestrategy");
        Assert.assertNull(producerRejection(e));
        try (AvroSerializerHelper helper = new AvroSerializerHelper(e)) {
            Method resolveSubject = AvroSerializerHelper.class.getDeclaredMethod("resolveSubject", String.class);
            resolveSubject.setAccessible(true);
            Assert.assertEquals("orders-value", resolveSubject.invoke(helper, "orders"));
        }
    }

    // ── The size Restrictions ───────────────────────────────────────────────────

    /**
     * A Restriction must not refuse a saved value that deploys (iron rule 2), so each of the three
     * accepts every value Camel binds and the start takes, an empty field and an externalized value.
     * Spaces around a number are not among them, nor is a plus sign, which Camel decodes to a space,
     * encoded or not: such a value failed at the binding up to now and deploys only since the spaces
     * around a number are trimmed, so no value that worked is refused.
     */
    @Test
    public void theSizeRestrictionsAcceptEveryValueThatDeploys() throws Exception {
        Assert.assertTrue("0 switches batching off, also as -0", deploys("producerBatchSizeKb", "-0"));
        String[] candidates = {"1", "0", "-0", "-00", "-1", "007", "5120", "2147483647", "2147483648",
                "9223372036854775807", "9223372036854775808", "1e3", "1.5", "0x10", "abc"};
        for (String field : new String[] {"maxRequestSizeKb", "producerBatchSizeKb", "bufferMemoryKb"}) {
            Pattern restriction = restriction(field);
            Assert.assertTrue(field + " refuses an empty field", restriction.matcher("").find());
            Assert.assertTrue(field + " refuses an externalized value", restriction.matcher("{{size}}").find());
            for (String value : candidates) {
                if (deploys(field, value)) {
                    Assert.assertTrue(field + "=" + value + " deploys, but the UI refuses it",
                            restriction.matcher(value).find());
                }
            }
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    /** A fresh endpoint per call: Camel caches endpoints by URI. */
    private CpiKafkaPlusEndpoint endpoint(String query) throws Exception {
        return (CpiKafkaPlusEndpoint) ctx.getEndpoint("cpi-kafka-plus:orders" + (++endpointCount) + "?" + query);
    }

    private boolean deploys(String field, String value) throws Exception {
        CpiKafkaPlusEndpoint e;
        try {
            e = endpoint("bootstrapServers=localhost:9999&securityProtocol=PLAINTEXT&" + field + "="
                    + URLEncoder.encode(value, "UTF-8"));
        } catch (ResolveEndpointFailedException bindingFailed) {
            return false;
        }
        return producerRejection(e) == null;
    }

    /** The message the receiver's start is rejected with, or {@code null} if it starts. */
    private static String producerRejection(CpiKafkaPlusEndpoint e) throws Exception {
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(e);
        try {
            producer.doStart();
            return null;
        } catch (IllegalArgumentException rejected) {
            return rejected.getMessage();
        } finally {
            producer.doStop();
        }
    }

    /** The message the sender's start is rejected with, or {@code null} if it starts. */
    private static String consumerRejection(CpiKafkaPlusEndpoint e) throws Exception {
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(e, exchange -> { });
        try {
            consumer.doStart();
            return null;
        } catch (IllegalArgumentException rejected) {
            return rejected.getMessage();
        } finally {
            consumer.doStop();
        }
    }

    private static Properties consumerProperties(CpiKafkaPlusEndpoint e) {
        Properties props = new CpiKafkaPlusConsumer(e, exchange -> { }).buildConsumerProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }

    private static Properties producerProperties(CpiKafkaPlusEndpoint e) {
        Properties props = ProducerConfigFactory.buildProducerProperties(e);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return props;
    }

    /** The regular expression of the field's Restriction on the receiver. */
    private static Pattern restriction(String field) throws Exception {
        NodeList references = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(RECEIVER_METADATA)
                .getElementsByTagName("AttributeReference");
        for (int i = 0; i < references.getLength(); i++) {
            Element reference = (Element) references.item(i);
            if (field.equals(reference.getElementsByTagName("ReferenceName").item(0).getTextContent().trim())) {
                NodeList restrictions = reference.getElementsByTagName("Restriction");
                Assert.assertEquals(field, 1, restrictions.getLength());
                Matcher m = REGEX_RESTRICTION.matcher(restrictions.item(0).getTextContent().trim());
                Assert.assertTrue(field, m.matches());
                return Pattern.compile(m.group(1));
            }
        }
        throw new AssertionError(field + " is not offered on the receiver");
    }

    private static void assertLoggedOnce(String logged, String line) {
        int first = logged.indexOf(line);
        Assert.assertTrue(logged, first >= 0);
        Assert.assertEquals(logged, -1, logged.indexOf(line, first + line.length()));
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Runs {@code action} and returns what was logged meanwhile; slf4j-simple writes to System.err. */
    private static String capture(ThrowingRunnable action) throws Exception {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, "UTF-8"));
        try {
            action.run();
        } finally {
            System.setErr(original);
        }
        return new String(captured.toByteArray(), StandardCharsets.UTF_8);
    }

    /** The lines of this thread: the Kafka clients and the topic check log from their own threads. */
    private static String ownLines(String logged) {
        String own = "[" + Thread.currentThread().getName() + "] ";
        StringBuilder out = new StringBuilder();
        for (String line : logged.split("\\R")) {
            if (line.startsWith(own)) {
                out.append(line).append('\n');
            }
        }
        return out.toString();
    }
}
