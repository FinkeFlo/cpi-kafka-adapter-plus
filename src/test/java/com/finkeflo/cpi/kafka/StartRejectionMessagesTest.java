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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The CPI UI shows labels only, so a deployment error names the field as the UI shows it, its
 * parameter, and the value it got: {@code '<Label>' (<param>) must ..., got: <value>} (#255). The
 * labels come from the metadata here, not from {@link UiLabels}.
 */
public class StartRejectionMessagesTest {

    private static final Path SOURCES = Paths.get("src/main/java/com/finkeflo/cpi/kafka");

    private DefaultCamelContext ctx;

    @Before
    public void setUp() throws Exception {
        ctx = new DefaultCamelContext();
        ctx.start();
    }

    @After
    public void tearDown() throws Exception {
        ctx.close();
    }

    /** One case per start rejection, each from a configuration that starts but for the one value. */
    @Test
    public void everyStartRejectionNamesLabelAndValue() throws Exception {
        List<Rejection> rejections = Arrays.asList(
                // CpiKafkaPlusEndpoint.validateConfiguration(), shared by both directions
                receiver("topic", "''", e -> e.setTopic(" ")),
                // The setter trims, so a blank value is reported as the empty one it becomes (#261).
                receiver("bootstrapServers", "''", e -> e.setBootstrapServers(" ")),
                receiver("bootstrapServers", "'b1:9092,broker2'", e -> e.setBootstrapServers("b1:9092, broker2")),
                sender("bootstrapServers", "'b1:99999'", e -> e.setBootstrapServers("b1:99999")),
                receiver("schemaRegistryUrl", "''", e -> {
                    e.setSchemaRegistryEnabled(true);
                    e.setSchemaRegistryUrl("");
                }),
                receiver("jsonSchema", "' '", e -> {
                    e.setJsonSchemaValidation(true);
                    e.setJsonSchema(" ");
                }),
                receiver("jsonSchema", "'{\"type\": \"object\",' (18 characters)", e -> {
                    e.setJsonSchemaValidation(true);
                    e.setJsonSchema("{\"type\": \"object\",");
                }),
                receiver("credentialAlias", "''", e -> {
                    e.setSecurityProtocol("SASL_SSL");
                    e.setCredentialAlias("");
                }),
                sender("saslMechanism", "'OAUTHBEARER'", e -> {
                    e.setSecurityProtocol("SASL_SSL");
                    e.setCredentialAlias("kafka-credentials");
                    e.setSaslMechanism("oauthbearer");
                }),
                receiver("diagnosticsLevel", "'VERBOSE'", e -> e.setDiagnosticsLevel("VERBOSE")),
                // ReceiverConfigValidator and TransactionalProducerPool
                receiver("producerBatchMode", "'CSV'", e -> e.setProducerBatchMode("CSV")),
                receiver("deliveryTimeoutSeconds", "0", e -> e.setDeliveryTimeoutSeconds(0)),
                receiver("maxRequestSizeKb", "0", e -> e.setMaxRequestSizeKb(0)),
                receiver("producerBatchSizeKb", "-1", e -> e.setProducerBatchSizeKb(-1)),
                receiver("bufferMemoryKb", "0", e -> e.setBufferMemoryKb(0)),
                receiver("schemaRegistryUrl", "'***@sr.example.com:8081'", e -> {
                    e.setSchemaRegistryEnabled(true);
                    e.setSchemaRegistryUrl("user:secret@sr.example.com:8081");
                    e.setAvroValueSerialization(true);
                }),
                receiver("subjectNameStrategy", "'RecordNameStrategy'", e -> {
                    e.setSchemaRegistryEnabled(true);
                    e.setSchemaRegistryUrl("http://localhost:8081");
                    e.setAvroValueSerialization(true);
                    e.setSubjectNameStrategy("RecordNameStrategy");
                }),
                receiver("transactionalIdPrefix", "''", e -> transactional(e, "")),
                receiver("enableIdempotence", "false", e -> {
                    transactional(e, "orders-tx");
                    e.setEnableIdempotence(false);
                }),
                receiver("deliveryTimeoutSeconds", "871", e -> {
                    transactional(e, "orders-tx");
                    e.setDeliveryTimeoutSeconds(871);
                }),
                receiver("maxConcurrentTransactions", "0", e -> {
                    transactional(e, "orders-tx");
                    e.setMaxConcurrentTransactions(0);
                }),
                receiver("transactionalIdPrefix", "'" + repeat('p', 300) + "' (300 characters)",
                        e -> transactional(e, repeat('p', 300))),
                receiver("producerRetryMaxAttempts", "6", e -> e.setProducerRetryMaxAttempts(6)),
                receiver("producerRetryDelaySeconds", "0", e -> {
                    e.setProducerRetryMaxAttempts(2);
                    e.setProducerRetryDelaySeconds(0);
                }),
                receiver("producerRetryTotalBudgetSeconds", "901", e -> {
                    e.setProducerRetryMaxAttempts(2);
                    e.setProducerRetryTotalBudgetSeconds(901);
                }),
                receiver("producerRetryTotalBudgetSeconds", "30", e -> e.setProducerRetryMaxAttempts(2)),
                // SenderConfigValidator
                sender("consumptionMode", "'BATCH'", e -> e.setConsumptionMode("BATCH")),
                sender("errorHandling", "'DROP'", e -> e.setErrorHandling("DROP")),
                sender("dlqTopic", "''", e -> dlq(e, "")),
                sender("groupId", "' '", e -> e.setGroupId(" ")),
                sender("commitStrategy", "'MANUAL'", e -> e.setCommitStrategy("MANUAL")),
                sender("maxPollRecords", "0", e -> e.setMaxPollRecords(0)),
                sender("fetchMinBytes", "-1", e -> e.setFetchMinBytes(-1)),
                sender("fetchMaxWaitMs", "-1", e -> e.setFetchMaxWaitMs(-1)),
                sender("batchTimeout", "-1", e -> e.setBatchTimeout(-1)),
                sender("batchSize", "0", e -> {
                    e.setBatchMode(true);
                    e.setBatchSize(0);
                }),
                sender("dlqMaxRetries", "-1", e -> {
                    dlq(e, "orders-dlq");
                    e.setDlqMaxRetries(-1);
                }),
                sender("dlqTopic", "' orders-dlq '", e -> dlq(e, " orders-dlq ")),
                sender("dlqTopic", "'orders dlq'", e -> dlq(e, "orders dlq")),
                sender("schemaRegistryUrl", "'ftp://sr.example.com'", e -> {
                    e.setSchemaRegistryEnabled(true);
                    e.setSchemaRegistryUrl("ftp://sr.example.com");
                    e.setAvroValueDeserialization(true);
                }),
                sender("dlqTopic", "'orders'", e -> dlq(e, "orders")),
                sender("topic", "','", e -> {
                    e.setTopic(",");
                    dlq(e, "orders-dlq");
                }),
                sender("commitStrategy", "'AUTO'", e -> {
                    e.setDrainEnabled(true);
                    e.setCommitStrategy("AUTO");
                }),
                sender("minBacklogToDrain", "-1", e -> {
                    e.setDrainEnabled(true);
                    e.setMinBacklogToDrain(-1);
                }),
                sender("minBacklogToDrain", "501", e -> {
                    e.setDrainEnabled(true);
                    e.setMinBacklogToDrain(501);
                }),
                sender("maxPartitionFetchSizeKb", "0", e -> e.setMaxPartitionFetchSizeKb(0)),
                sender("retryDelaySeconds", "301", e -> e.setRetryDelaySeconds(301)),
                sender("pollingIntervalSeconds", "0", e -> e.setPollingIntervalSeconds(0)),
                sender("pollingIntervalSeconds", "21601", e -> e.setPollingIntervalSeconds(21601)));

        Map<String, String> senderLabels = UiLabelsTest.offeredLabels("sender");
        Map<String, String> receiverLabels = UiLabelsTest.offeredLabels("receiver");
        List<String> wrong = new ArrayList<>();
        for (Rejection rejection : rejections) {
            String label = (rejection.sender ? senderLabels : receiverLabels).get(rejection.param);
            Assert.assertNotNull(rejection.param + " is not offered", label);
            String message = rejection.message();
            if (message == null) {
                wrong.add(rejection.param + "=" + rejection.value + ": the channel started");
            } else if (!message.startsWith("'" + label + "' (" + rejection.param + ") must ")
                    || !message.contains(", got: " + rejection.value)) {
                wrong.add(rejection.param + "=" + rejection.value + ": " + message);
            }
        }
        Assert.assertTrue(String.join("\n", wrong), wrong.isEmpty());
    }

    /**
     * Only ERROR reaches the tenant trace, so a line about a start-up setting at WARN or below is a
     * line nobody sees in production (#255).
     */
    @Test
    public void theValidatorsLogNothingBelowError() throws IOException {
        Pattern belowError = Pattern.compile("LOG\\.(warn|info|debug|trace)\\(");
        List<String> found = new ArrayList<>();
        for (String validator : new String[] {"SenderConfigValidator.java", "ReceiverConfigValidator.java"}) {
            String source = new String(Files.readAllBytes(SOURCES.resolve(validator)), StandardCharsets.UTF_8);
            Matcher m = belowError.matcher(source);
            while (m.find()) {
                found.add(validator + ": " + m.group());
            }
        }
        Assert.assertTrue(String.join("\n", found), found.isEmpty());
    }

    private Rejection sender(String param, String value, Consumer<CpiKafkaPlusEndpoint> change) {
        return new Rejection(true, param, value, change);
    }

    private Rejection receiver(String param, String value, Consumer<CpiKafkaPlusEndpoint> change) {
        return new Rejection(false, param, value, change);
    }

    private static void transactional(CpiKafkaPlusEndpoint e, String prefix) {
        e.setProducerBatchMode("JSON_ARRAY");
        e.setEnableTransactions(true);
        e.setTransactionalIdPrefix(prefix);
    }

    private static void dlq(CpiKafkaPlusEndpoint e, String topic) {
        e.setDlqEnabled(true);
        e.setDlqTopic(topic);
    }

    private static String repeat(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
    }

    /** A value one start check rejects. */
    private final class Rejection {
        final boolean sender;
        final String param;
        final String value;
        final Consumer<CpiKafkaPlusEndpoint> change;

        Rejection(boolean sender, String param, String value, Consumer<CpiKafkaPlusEndpoint> change) {
            this.sender = sender;
            this.param = param;
            this.value = value;
            this.change = change;
        }

        /** The message the start is rejected with, or {@code null} if the channel starts. */
        String message() throws Exception {
            CpiKafkaPlusEndpoint e = new CpiKafkaPlusEndpoint();
            e.setCamelContext(ctx);
            e.setTopic("orders");
            e.setBootstrapServers("localhost:9999");
            e.setSecurityProtocol("PLAINTEXT");
            if (sender) {
                e.setGroupId("orders-group");
            }
            change.accept(e);
            if (sender) {
                CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(e, exchange -> { });
                return startFailure(consumer::doStart, consumer::doStop);
            }
            CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(e);
            return startFailure(producer::doStart, producer::doStop);
        }
    }

    private interface Step {
        void run() throws Exception;
    }

    private static String startFailure(Step start, Step stop) throws Exception {
        try {
            start.run();
            return null;
        } catch (IllegalArgumentException expected) {
            return expected.getMessage();
        } finally {
            try {
                stop.run();
            } catch (Exception ignored) {
                // a failed start may also fail to stop; not what is under test
            }
        }
    }
}
