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

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultPollingEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

@UriEndpoint(scheme = "cpi-kafka-plus", syntax = "cpi-kafka-plus:topic", title = "CPI Kafka Plus")
public class CpiKafkaPlusEndpoint extends DefaultPollingEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusEndpoint.class);

    /** The mechanisms the adapter can log in with; it builds the login for these only. */
    private static final List<String> SASL_MECHANISMS = Arrays.asList("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512");

    private final CpiKafkaPlusComponent component;
    private String topicFromUri;

    /** Fixed idle heartbeat delay (ms) for STREAMING mode when the topic has no records. */
    private static final long STREAMING_IDLE_DELAY_MS = 1000L;

    /**
     * Delay (ms) before the very first scheduled poll, giving the CPI node time to finish route
     * startup. Identical for both consumption modes.
     */
    private static final long INITIAL_DELAY_MS = 5000L;

    // --- Connection ---
    @UriParam(label = "connection", description = "Kafka Bootstrap Servers (comma-separated)")
    private String bootstrapServers;

    @UriParam(label = "connection", description = "Kafka Topic(s) to consume from (comma-separated for multiple)")
    private String topic;

    @UriParam(label = "connection",
            description = "Consumer Group ID. One per integration flow and tenant: two channels with the same "
                    + "group ID on the same worker fence each other (static membership).")
    private String groupId;

    // --- Security ---
    @UriParam(label = "security", defaultValue = "SASL_SSL",
            description = "Security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL")
    private String securityProtocol = "SASL_SSL";

    @UriParam(label = "security", defaultValue = "PLAIN",
            description = "SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512")
    private String saslMechanism = "PLAIN";

    @UriParam(label = "security",
            description = "Credential alias for SASL username/password from CPI Secure Store")
    private String credentialAlias;

    @UriParam(label = "security",
            description = "Leave empty for brokers with a publicly trusted certificate (e.g. Confluent Cloud) — "
                    + "the JVM default truststore is used and TLS is still active. Setting any alias makes the "
                    + "adapter trust the certificates of the whole CPI tenant keystore instead of the JVM default "
                    + "truststore, so the broker's CA certificate (or its self-signed certificate) must then be "
                    + "in the tenant keystore. For client-certificate authentication (mTLS) the alias must name "
                    + "a key pair.")
    private String sslKeystoreAlias;

    // --- Processing ---
    @UriParam(label = "processing", defaultValue = "5",
            description = "Time in seconds between poll cycles. Range: 1 to 21600 (1 second to 6 hours). "
                    + "Whole-hour values (3600=1h, 7200=2h, ..., 21600=6h) align polls with predictable clock times.")
    private long pollingIntervalSeconds = 5;

    @UriParam(label = "processing", defaultValue = "latest",
            description = "Auto offset reset: earliest or latest")
    private String autoOffsetReset = "latest";

    @UriParam(label = "processing", defaultValue = "500",
            description = "Maximum records to poll per request")
    private int maxPollRecords = 500;

    @UriParam(label = "processing", defaultValue = "1",
            description = "Kafka fetch.min.bytes: minimum data (in bytes) the broker accumulates "
                    + "before responding to a fetch request. Higher values allow larger, more "
                    + "efficient batches at the cost of added latency (bounded by fetchMaxWaitMs). "
                    + "Default (1) matches the Kafka client default, preserving existing behavior.")
    private int fetchMinBytes = 1;

    @UriParam(label = "processing", defaultValue = "500",
            description = "Kafka fetch.max.wait.ms: maximum time the broker waits to satisfy "
                    + "fetchMinBytes before returning whatever records are currently available. "
                    + "Default (500) matches the Kafka client default, preserving existing behavior.")
    private int fetchMaxWaitMs = 500;

    @UriParam(label = "processing", defaultValue = "SCHEDULED",
            description = "Consumption mode. SCHEDULED (default): poll every 'pollingIntervalSeconds'. "
                    + "STREAMING: greedy scheduling — as long as a poll returns records the next poll "
                    + "fires immediately (no delay), so records are consumed continuously with minimal "
                    + "latency; when the topic is idle the consumer only waits out the poll timeout "
                    + "('batchTimeout') plus a fixed 1s heartbeat delay before retrying. "
                    + "In STREAMING mode 'pollingIntervalSeconds' and 'drainEnabled' are ignored (greedy "
                    + "replaces the drain/interval mechanism).")
    private String consumptionMode = "SCHEDULED";

    @UriParam(label = "processing", defaultValue = "BATCH_COMPLETE",
            description = "Offset commit strategy: AUTO, BATCH_COMPLETE")
    private String commitStrategy = "BATCH_COMPLETE";

    @UriParam(label = "processing", defaultValue = "false",
            description = "Enable drain mode: poll repeatedly until topic is empty")
    private boolean drainEnabled = false;

    @UriParam(label = "processing", defaultValue = "0",
            description = "Minimum records returned by a drain poll to continue draining. "
                    + "When an EXTRA drain poll (cycle > 1) returns fewer records than this threshold, "
                    + "those records are seeked back (un-polled) and the drain stops, avoiding tiny tail "
                    + "MPLs. The first poll of a scheduled run always processes its records, regardless "
                    + "of count. Default 0 means drain until topic is completely empty.")
    private int minBacklogToDrain = 0;

    @UriParam(label = "consumer",
            description = "Maximum data the broker returns per partition per poll, in kilobytes. "
                    + "Default: 1024 KB (1 MB). Increase when 'Drain Backlog' is enabled and polls return "
                    + "far fewer records than 'Max Poll Records'.",
            defaultValue = "1024")
    private int maxPartitionFetchSizeKb = 1024;

    // --- Batch ---
    @UriParam(label = "batch", defaultValue = "true",
            description = "Enable batch mode (multiple records per exchange)")
    private boolean batchMode = true;

    @UriParam(label = "batch", defaultValue = "100",
            description = "Maximum records per batch")
    private int batchSize = 100;

    @UriParam(label = "batch", defaultValue = "5000",
            description = "Poll timeout in ms: how long one poll waits for records when the topic is "
                    + "empty. It does not wait to fill a batch; a poll returns as soon as records arrive.")
    private long batchTimeout = 5000;

    @UriParam(label = "batch", defaultValue = "JSON_ARRAY",
            description = "Batch output format: JSON_ARRAY, XML_LIST. "
                    + "SPLIT_EXCHANGES is accepted at runtime for backward compatibility with "
                    + "existing channel configs but is no longer shown in the UI as of 1.2.0.")
    private String batchOutputFormat = "JSON_ARRAY";

    // --- Producer Settings ---
    @UriParam(label = "producer", defaultValue = "NONE",
            description = "Batch send mode: NONE (one message per exchange), "
                    + "JSON_ARRAY (split JSON array into individual messages), "
                    + "XML_LIST (split XML kafkaRecords into individual messages)")
    private String producerBatchMode = "NONE";

    @UriParam(label = "producer", defaultValue = "all",
            description = "Producer acknowledgments: all, 1, 0")
    private String acks = "all";

    @UriParam(label = "producer", defaultValue = "none",
            description = "Compression type: none, gzip, lz4, zstd")
    private String compressionType = "none";

    @UriParam(label = "producer", defaultValue = "5120",
            description = "Maximum size of a request in kilobytes. Default: 5120 KB (5 MB).")
    private int maxRequestSizeKb = 5120;

    @UriParam(label = "producer", defaultValue = "1024",
            description = "Producer batch size in kilobytes. Default: 1024 KB (1 MB).")
    private int producerBatchSizeKb = 1024;

    @UriParam(label = "producer", defaultValue = "32768",
            description = "Total memory for producer buffering in kilobytes. Default: 32768 KB (32 MB).")
    private long bufferMemoryKb = 32768;

    @UriParam(label = "producer", defaultValue = "true",
            description = "Enable the idempotent producer: the broker discards the duplicates of the producer's "
                    + "own internal retries. Not end-to-end exactly-once: a message the caller sends twice "
                    + "is written twice.")
    private boolean enableIdempotence = true;

    @UriParam(label = "producer", defaultValue = "false",
            description = "Enable transactional batching (creates a new transactional producer per batch)")
    private boolean enableTransactions = false;

    @UriParam(label = "producer",
            description = "Prefix for transactional.id (e.g. my-app-txn). Required with transactional batching "
                    + "(enableTransactions with producerBatchMode JSON_ARRAY or XML_LIST). Unique per integration "
                    + "flow and tenant for the same topic and cluster: equal prefixes fence each other.")
    private String transactionalIdPrefix;

    @UriParam(label = "producer", defaultValue = "5",
            description = "Maximum number of concurrent transactional producers per worker node")
    private int maxConcurrentTransactions = 5;

    /**
     * @deprecated Retained only so existing iFlow URIs that still carry this parameter continue to
     *     resolve. It has no effect. The option never controlled Transaction Protocol V2 (KIP-890),
     *     which the client derives from the broker's finalized {@code transaction.version} feature.
     *     It mapped to {@code transaction.two.phase.commit.enable} (KIP-939), which
     *     {@link ProducerConfigFactory} now pins to {@code false} for every producer. Removed from
     *     the channel UI in 1.2.6. Kept for good: CPI has no major-version migration, so saved iFlows
     *     can carry the parameter indefinitely, and an unknown parameter would fail their deployment.
     */
    @Deprecated
    @UriParam(label = "producer", defaultValue = "true",
            description = "Deprecated and ignored. This option never controlled Transaction Protocol "
            + "V2 (KIP-890), which is negotiated from the broker's transaction.version feature. "
            + "Two-phase commit (KIP-939) is now disabled for every producer regardless of this value.")
    private boolean transactionV2Enabled = true;

    @UriParam(label = "producer", defaultValue = "*",
            description = "Pipe-separated list of exchange headers to send to Kafka (e.g. SAP_*|MyHeader). "
            + "* sends all but Authorization, Proxy-Authorization, Cookie and Set-Cookie, which are sent only "
            + "when named exactly. Empty sends none.")
    private String allowedHeaders = "*";

    // No "retries" option: deliveryTimeoutSeconds is the effective limit, and with idempotence the
    // Kafka client retries internally until it expires.

    @UriParam(label = "producer", defaultValue = "120",
            description = "Maximum time for message delivery in seconds, including retries. "
                    + "Covers transient leader elections / broker restarts (e.g. NotLeaderOrFollowerException), "
                    + "which the client retries internally within this window. Default: 120 seconds.")
    private int deliveryTimeoutSeconds = 120;

    // --- JSON Schema Validation ---
    @UriParam(label = "processing", defaultValue = "false",
            description = "Enable JSON Schema validation of incoming messages")
    private boolean jsonSchemaValidation = false;

    @UriParam(label = "processing",
            description = "JSON Schema (inline JSON string) for message validation")
    private String jsonSchema;

    @UriParam(label = "processing", defaultValue = "false",
            description = "Sender: report a record that fails JSON Schema validation as a failed message "
                    + "in CPI Monitoring (MPL); when disabled it is dropped with a WARN log only. "
                    + "Receiver: an invalid message always fails the exchange; this option only adds "
                    + "the payload to the trace.")
    private boolean jsonSchemaReportError = false;

    // --- Error Handling / DLQ ---
    @UriParam(label = "errorHandling", defaultValue = "RETRY", enums = "RETRY,SKIP",
            description = "Sender (consumer) direction only. What happens to a message whose processing "
                    + "fails when no Dead Letter Queue is configured: RETRY keeps retrying the same offset "
                    + "(the partition waits, with a backoff that starts at retryDelaySeconds, at least 1 s, "
                    + "and doubles up to 300 s); SKIP continues with the next offset (at-most-once). "
                    + "Mirrors the 'Error Handling' option of the SAP Kafka sender adapter.")
    private String errorHandling = "RETRY";

    @UriParam(label = "errorHandling", defaultValue = "false",
            description = "Enable Dead Letter Queue for failed messages")
    private boolean dlqEnabled = false;

    @UriParam(label = "errorHandling",
            description = "Topic name for the Dead Letter Queue")
    private String dlqTopic;

    @UriParam(label = "errorHandling", defaultValue = "3",
            description = "Maximum processing retries before routing to DLQ")
    private int dlqMaxRetries = 3;

    @UriParam(label = "errorHandling",
            description = "SASL credential alias for writing to the DLQ topic, if it needs other "
                    + "credentials than the main connection. The DLQ topic is on the same cluster "
                    + "(bootstrapServers).")
    private String dlqCredentialAlias;

    // --- Smart Retry (Sender/consumer direction only — see producerRetry* below) ---
    @UriParam(label = "errorHandling", defaultValue = "true",
            description = "Sender (consumer) direction only. Only retry transient errors "
                    + "(e.g. timeouts, connection failures). "
                    + "Permanent errors like mapping failures or NullPointerExceptions are "
                    + "sent directly to the DLQ without retries. "
                    + "The receiver (producer) direction uses producerRetryOnlyTransientErrors.")
    private boolean retryOnlyTransientErrors = true;

    @UriParam(label = "errorHandling", defaultValue = "0",
            description = "Sender (consumer) direction only. Initial delay between retries in seconds. "
                    + "The delay doubles after each retry (exponential backoff), "
                    + "capped at 300 seconds. "
                    + "Example: with 2s delay and 3 retries the waits are 2s, 4s, 8s. "
                    + "Set to 0 to retry immediately without delay. "
                    + "The receiver (producer) direction uses producerRetryDelaySeconds, which is "
                    + "constant rather than exponential.")
    private int retryDelaySeconds = 0;

    // --- Producer Outer Retry (Receiver direction) ---
    // Deliberately separate parameters from the consumer-side smart retry above: the consumer
    // parameters carry exponential-backoff semantics, and one option meaning two different things
    // depending on the channel direction is a support trap.
    @UriParam(label = "producer", defaultValue = "1",
            description = "Total number of send attempts for the receiver direction, not additional "
                    + "attempts. 1 (default) keeps today's behaviour and switches the feature off. "
                    + "Only failures that provably wrote nothing are retried: a transactional batch "
                    + "that failed before commitTransaction(), or a single non-transactional message "
                    + "that failed before it reached the producer's buffer (idempotence required). "
                    + "A failure inside commitTransaction() and a "
                    + "partially sent non-transactional batch are never retried, because a retry "
                    + "could duplicate records. Range: 1-5.")
    private int producerRetryMaxAttempts = 1;

    @UriParam(label = "producer", defaultValue = "2",
            description = "Constant wait between producer send attempts in seconds (no exponential "
                    + "backoff: the Kafka client has already exhausted its own). Values below 1 "
                    + "second are pointless against a disconnected broker node and only add load. "
                    + "Range: 1-30.")
    private int producerRetryDelaySeconds = 2;

    @UriParam(label = "producer", defaultValue = "true",
            description = "Retry only transient (RETRIABLE) send failures. Set to false to also "
                    + "retry a fenced or otherwise unusable transactional producer — only safe when "
                    + "this transactionalIdPrefix is provably used by no other iFlow, since a "
                    + "ProducerFencedException usually means another instance took over the "
                    + "transactional.id. Data errors and unclassified errors are never retried.")
    private boolean producerRetryOnlyTransientErrors = true;

    @UriParam(label = "producer", defaultValue = "30",
            description = "Hard upper bound in seconds for all producer send attempts of one "
                    + "message together. Must stay below the timeout of the calling HTTP client: "
                    + "if the caller times out while the adapter is still retrying, it may resend "
                    + "the request and duplicate the message outside the adapter's control. "
                    + "Range: 5-900.")
    private int producerRetryTotalBudgetSeconds = 30;

    // --- Auto-Pause (Circuit Breaker) ---
    @UriParam(label = "errorHandling", defaultValue = "false",
            description = "Automatically pause the consumer after consecutive processing errors")
    private boolean autoPauseEnabled = false;

    @UriParam(label = "errorHandling", defaultValue = "5",
            description = "Number of consecutive failed records (or failed batches without a DLQ) before "
                    + "auto-pause activates. A record counts once after its last attempt, also when it is then "
                    + "dead-lettered; JSON Schema violations do not count.")
    private int autoPauseErrorThreshold = 5;

    @UriParam(label = "errorHandling", defaultValue = "60",
            description = "Initial pause duration in seconds. Doubles after each subsequent failure (max 900s / 15min)")
    private int autoPauseCooldownSeconds = 60;

    // --- Avro / Schema Registry ---
    @UriParam(label = "avro", defaultValue = "false",
            description = "Enable Confluent Schema Registry integration")
    private boolean schemaRegistryEnabled = false;

    @UriParam(label = "avro", defaultValue = "false",
            description = "Automatically register schemas with Schema Registry")
    private boolean autoRegisterSchemas = false;

    @UriParam(label = "avro", defaultValue = "TopicNameStrategy",
            description = "Subject naming strategy. Only TopicNameStrategy is supported: with RecordNameStrategy or "
                    + "TopicRecordNameStrategy and Avro serialization the endpoint does not start.")
    private String subjectNameStrategy = "TopicNameStrategy";

    @UriParam(label = "avro",
            description = "Confluent Schema Registry URL")
    private String schemaRegistryUrl;

    @UriParam(label = "avro",
            description = "Credential alias for Schema Registry authentication")
    private String schemaRegistryCredentialAlias;

    @UriParam(label = "avro", defaultValue = "JSON",
            description = "Avro output format: JSON, XML")
    private String avroOutputFormat = "JSON";

    @UriParam(label = "avro", defaultValue = "true",
            description = "Deserialize message values using Avro (requires Schema Registry)")
    private boolean avroValueDeserialization = true;

    @UriParam(label = "avro", defaultValue = "true",
            description = "Serialize message values using Avro (requires Schema Registry)")
    private boolean avroValueSerialization = true;

    // --- Diagnostics ---
    @UriParam(label = "processing", defaultValue = "STANDARD",
            description = "Diagnostic output level. STANDARD (default): every failure gets one structured ERROR line "
                    + "with the full serialised cause chain — sufficient to root-cause most production incidents. "
                    + "FULL adds one thing on the receiver: a bounded thread dump attached to the node-fault "
                    + "escalation, which always carries the JVM state. It costs nothing on a normal send. "
                    + "Use FULL only while investigating such a fault.")
    private String diagnosticsLevel = "STANDARD";

    @UriParam(label = "errorHandling", defaultValue = "true",
            description = "Write the full Kafka adapter error diagnostic as an MPL attachment "
                    + "('KafkaAdapterError'). Disable to keep only searchable MPL headers/attributes. "
                    + "Sender only: on the receiver it currently has no effect, because a failing receiver "
                    + "channel fails the exchange and CPI writes the MPL entry without the attachment.")
    private boolean writeMplErrorAttachment = true;

    public CpiKafkaPlusEndpoint() {
        this.component = null;
    }

    public CpiKafkaPlusEndpoint(String endpointUri, CpiKafkaPlusComponent component) throws URISyntaxException {
        super(endpointUri, component);
        this.component = component;
    }

    public CpiKafkaPlusEndpoint(String uri, String remaining, CpiKafkaPlusComponent component) throws URISyntaxException {
        this(uri, component);
        this.topicFromUri = remaining != null ? remaining.trim() : null;
    }

    /**
     * Validates configuration settings that are shared between Consumer and Producer.
     * Called from doStart() of both sides for fail-fast behaviour.
     */
    public void validateConfiguration() {
        validateConfiguration(true);
    }

    /**
     * As {@link #validateConfiguration()}, checking the JSON Schema only if {@code jsonSchemaApplied}.
     * The receiver applies it to single messages only, so with a batch send mode a stale schema must
     * not block the start (#263).
     */
    void validateConfiguration(boolean jsonSchemaApplied) {
        String effectiveTopic = getEffectiveTopic();
        if (effectiveTopic == null || effectiveTopic.trim().isEmpty()) {
            throw new IllegalArgumentException(UiLabels.field("topic") + " must not be empty, got: "
                    + UiLabels.quote(effectiveTopic) + ". Please set it in the adapter Connection tab.");
        }
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new IllegalArgumentException(UiLabels.field("bootstrapServers") + " must not be empty, got: "
                    + UiLabels.quote(bootstrapServers) + ". Please set the Kafka broker address in the adapter "
                    + "Connection tab.");
        }
        for (String entry : bootstrapServers.split(",")) {
            // An empty entry is skipped, as Kafka's ClientUtils does.
            String address = entry.trim();
            String problem = address.isEmpty() ? null : unusableAddress(address);
            if (problem != null) {
                throw new IllegalArgumentException(UiLabels.field("bootstrapServers")
                        + " must be a comma-separated list of host:port addresses, got: "
                        + UiLabels.quote(bootstrapServers) + ". " + problem);
            }
        }
        if (schemaRegistryEnabled
                && (schemaRegistryUrl == null || schemaRegistryUrl.trim().isEmpty())) {
            throw new IllegalArgumentException(UiLabels.field("schemaRegistryUrl") + " must not be empty while "
                    + UiLabels.field("schemaRegistryEnabled") + " is on, got: " + UiLabels.quote(schemaRegistryUrl));
        }
        if (jsonSchemaValidation && jsonSchemaApplied) {
            if (jsonSchema == null || jsonSchema.trim().isEmpty()) {
                throw new IllegalArgumentException(UiLabels.field("jsonSchema") + " must not be empty while "
                        + UiLabels.field("jsonSchemaValidation") + " is on, got: " + UiLabels.quote(jsonSchema)
                        + ". Please provide a JSON Schema (draft-07) or switch the validation off.");
            }
            try {
                JsonSchemaValidator.parseSchemaDocument(new ObjectMapper(), jsonSchema);
            } catch (IllegalArgumentException e) {
                // The schema can be 50,000 characters long: its beginning, its length and the parser's
                // account of where it fails say more than the whole text would.
                throw new IllegalArgumentException(UiLabels.field("jsonSchema") + " must be one valid JSON document "
                        + "while " + UiLabels.field("jsonSchemaValidation") + " is on, got: "
                        + UiLabels.quote(abbreviate(jsonSchema)) + " (" + jsonSchema.length() + " characters). "
                        + e.getMessage(), e);
            }
        }
        boolean sasl = securityProtocol != null && securityProtocol.toUpperCase().contains("SASL");
        if (sasl && (credentialAlias == null || credentialAlias.trim().isEmpty())) {
            throw new IllegalArgumentException(UiLabels.field("credentialAlias") + " must not be empty while "
                    + UiLabels.field("securityProtocol") + " is " + securityProtocol + ", got: "
                    + UiLabels.quote(credentialAlias));
        }
        if (sasl && !SASL_MECHANISMS.contains(saslMechanism)) {
            // Any other mechanism used to get the PLAIN login and failed at the first connect.
            throw new IllegalArgumentException(UiLabels.field("saslMechanism")
                    + " must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512 while " + UiLabels.field("securityProtocol")
                    + " is " + securityProtocol + ", got: " + UiLabels.quote(saslMechanism)
                    + ". The adapter can log in with no other mechanism.");
        }
        if (diagnosticsLevel != null
                && !diagnosticsLevel.isEmpty()
                && !"STANDARD".equalsIgnoreCase(diagnosticsLevel)
                && !"FULL".equalsIgnoreCase(diagnosticsLevel)) {
            throw new IllegalArgumentException(UiLabels.field("diagnosticsLevel")
                    + " must be STANDARD (default) or FULL, got: " + UiLabels.quote(diagnosticsLevel));
        }
    }

    /**
     * Why Kafka cannot use {@code address} as a bootstrap server, or {@code null} if it can. Read as
     * Kafka's ClientUtils reads it: host and port as {@link Utils#getHost} and {@link Utils#getPort}
     * find them, and a port that an {@code InetSocketAddress} takes.
     */
    private static String unusableAddress(String address) {
        Integer port;
        try {
            port = Utils.getPort(address);
        } catch (NumberFormatException e) {
            port = Integer.MAX_VALUE; // more digits than an int holds
        }
        if (Utils.getHost(address) == null || port == null) {
            return "Kafka cannot read '" + address + "' as host:port.";
        }
        if (port > 65535) {
            return "The port of '" + address + "' is above 65535.";
        }
        return null;
    }

    /** The first 40 characters of {@code text}, and "..." if there are more. */
    private static String abbreviate(String text) {
        return text.length() <= 40 ? text : text.substring(0, 40) + "...";
    }

    @Override
    public Producer createProducer() throws Exception {
        return new CpiKafkaPlusProducer(this);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        LOG.error("[CPI-KAFKA-PLUS-DIAG] createConsumer called for topic='{}' group='{}'",
                getEffectiveTopic(), getGroupId());
        CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(this, processor);
        configureConsumer(consumer);
        // Override after configureConsumer() so pollingIntervalSeconds always wins
        // over any scheduler values injected by CPI's default scheduler tab.
        // initialDelay is fixed at 5s (independent of pollingIntervalSeconds) so the
        // first poll() — and therefore the consumer-group join — happens shortly after
        // deployment instead of waiting a full polling interval. Without this, long
        // intervals (e.g. 600s) leave the consumer group EMPTY for that full duration
        // after IFlow start.
        // Scheduler-Takt = min(pollingIntervalSeconds, Keep-Alive-Takt). Bei langen
        // Intervallen tickt poll() alle 60 s und treibt zwischen den Emit-Zyklen das
        // Kafka-Group-Protokoll (Keep-Alive-Poll). pollingIntervalSeconds bleibt das
        // Emit-Intervall — die Drain-/Batch-Logik laeuft weiterhin nur alle X s.
        if (isStreamingMode()) {
            // STREAMING: greedy scheduling. As long as a poll() returns records the scheduler
            // fires the next poll() IMMEDIATELY (no delay) -> continuous consumption with minimal
            // latency. When idle it falls back to the 1s fixed delay, so the idle cadence is
            // "poll timeout (batchTimeout) + 1s" — a pure heartbeat, since a poll() that has data
            // returns as soon as the records arrive.
            // pollingIntervalSeconds/drainEnabled have no effect here because every tick emits
            // and greedy replaces the drain purpose.
            // The 5s initial delay is kept identical to SCHEDULED: it only shifts the very first
            // poll (giving the CPI node time to finish route startup and acquire the cluster
            // lock) and has no influence on steady-state streaming latency.
            consumer.setGreedy(true);
            consumer.setDelay(STREAMING_IDLE_DELAY_MS);
            consumer.setInitialDelay(INITIAL_DELAY_MS);
            consumer.setUseFixedDelay(true);
        } else {
            consumer.setGreedy(false);
            consumer.setDelay(
                    CpiKafkaPlusConsumer.computeSchedulerDelaySeconds(pollingIntervalSeconds) * 1000);
            consumer.setInitialDelay(INITIAL_DELAY_MS);
            consumer.setUseFixedDelay(true);
        }
        return consumer;
    }

    public String getEffectiveTopic() {
        return (topic != null && !topic.isEmpty()) ? topic : topicFromUri;
    }

    @Override
    public String getEndpointUri() {
        String effectiveTopic = getEffectiveTopic();
        if (effectiveTopic != null && !effectiveTopic.isEmpty()) {
            return "cpi-kafka-plus://" + effectiveTopic;
        }
        return super.getEndpointUri();
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    // --- Getters and Setters ---

    public String getBootstrapServers() { return bootstrapServers; }
    /**
     * Trimmed, entry by entry, and without empty entries: kafka-clients 4.3.1 rejects an empty entry,
     * so a trailing comma failed every poll and send (#261).
     */
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = addressList(bootstrapServers); }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic != null ? topic.trim() : null; }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public String getSecurityProtocol() { return securityProtocol; }
    /**
     * Normalised to upper case: Kafka accepts {@code sasl_ssl}, but every check in the adapter compares
     * against the upper-case names, so a lower-case value from an externalized parameter configured
     * neither SASL nor the keystore alias (#175).
     */
    public void setSecurityProtocol(String securityProtocol) { this.securityProtocol = normaliseName(securityProtocol); }

    public String getSaslMechanism() { return saslMechanism; }
    /** Normalised to upper case, like {@link #setSecurityProtocol(String)}. */
    public void setSaslMechanism(String saslMechanism) { this.saslMechanism = normaliseName(saslMechanism); }

    private static String normaliseName(String value) {
        return value == null ? null : value.trim().toUpperCase(java.util.Locale.ROOT);
    }

    /** For a value where a space at either end can never be meant, such as an alias or a URL (#261). */
    private static String trim(String value) {
        return value == null ? null : value.trim();
    }

    /**
     * Kafka's spelling of {@code value} if it is one of {@code keywords} in any case, with or without
     * spaces around it, and anything else unchanged, for Kafka to take or reject. Kafka compares these
     * keywords case-sensitively, and an externalized value bypasses the dropdown (#261).
     */
    private static String kafkaKeyword(String value, String... keywords) {
        if (value != null) {
            for (String keyword : keywords) {
                if (keyword.equalsIgnoreCase(value.trim())) {
                    return keyword;
                }
            }
        }
        return value;
    }

    /** The trimmed, non-empty entries of a comma-separated list, joined by commas. */
    private static String addressList(String value) {
        if (value == null) {
            return null;
        }
        StringBuilder list = new StringBuilder();
        for (String entry : value.split(",")) {
            String address = entry.trim();
            if (!address.isEmpty()) {
                if (list.length() > 0) {
                    list.append(',');
                }
                list.append(address);
            }
        }
        return list.toString();
    }

    public String getCredentialAlias() { return credentialAlias; }
    /** Trimmed: the Secure Store lookup takes the alias as it is (#261). */
    public void setCredentialAlias(String credentialAlias) { this.credentialAlias = trim(credentialAlias); }

    public String getSslKeystoreAlias() { return sslKeystoreAlias; }
    public void setSslKeystoreAlias(String sslKeystoreAlias) { this.sslKeystoreAlias = sslKeystoreAlias; }

    public long getPollingIntervalSeconds() { return pollingIntervalSeconds; }
    public void setPollingIntervalSeconds(long pollingIntervalSeconds) { this.pollingIntervalSeconds = pollingIntervalSeconds; }

    public String getAutoOffsetReset() { return autoOffsetReset; }
    /** {@code latest}, {@code earliest} and {@code none} in any case are passed in Kafka's spelling (#261). */
    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = kafkaKeyword(autoOffsetReset, "latest", "earliest", "none");
    }

    public int getMaxPollRecords() { return maxPollRecords; }
    public void setMaxPollRecords(int maxPollRecords) { this.maxPollRecords = maxPollRecords; }

    public int getFetchMinBytes() { return fetchMinBytes; }
    public void setFetchMinBytes(int fetchMinBytes) { this.fetchMinBytes = fetchMinBytes; }

    public int getFetchMaxWaitMs() { return fetchMaxWaitMs; }
    public void setFetchMaxWaitMs(int fetchMaxWaitMs) { this.fetchMaxWaitMs = fetchMaxWaitMs; }

    public String getCommitStrategy() { return commitStrategy; }
    public void setCommitStrategy(String commitStrategy) { this.commitStrategy = commitStrategy; }

    public String getConsumptionMode() { return consumptionMode; }
    public void setConsumptionMode(String consumptionMode) { this.consumptionMode = consumptionMode; }

    /** True when the consumer runs in greedy STREAMING mode instead of scheduled polling. */
    public boolean isStreamingMode() { return "STREAMING".equalsIgnoreCase(consumptionMode); }

    /**
     * Transactions apply to batches only. With {@code producerBatchMode=NONE} every message goes
     * through the shared, non-transactional producer, whatever {@code enableTransactions} says.
     */
    public boolean isTransactionalBatching() {
        return enableTransactions && !"NONE".equalsIgnoreCase(producerBatchMode);
    }

    public boolean isDrainEnabled() { return drainEnabled; }
    public void setDrainEnabled(boolean drainEnabled) { this.drainEnabled = drainEnabled; }

    public int getMinBacklogToDrain() { return minBacklogToDrain; }
    public void setMinBacklogToDrain(int minBacklogToDrain) { this.minBacklogToDrain = minBacklogToDrain; }

    public int getMaxPartitionFetchSizeKb() { return maxPartitionFetchSizeKb; }
    public void setMaxPartitionFetchSizeKb(int maxPartitionFetchSizeKb) { this.maxPartitionFetchSizeKb = maxPartitionFetchSizeKb; }

    public boolean isBatchMode() { return batchMode; }
    public void setBatchMode(boolean batchMode) { this.batchMode = batchMode; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public long getBatchTimeout() { return batchTimeout; }
    public void setBatchTimeout(long batchTimeout) { this.batchTimeout = batchTimeout; }

    public String getBatchOutputFormat() { return batchOutputFormat; }
    public void setBatchOutputFormat(String batchOutputFormat) { this.batchOutputFormat = batchOutputFormat; }

    public boolean isJsonSchemaValidation() { return jsonSchemaValidation; }
    public void setJsonSchemaValidation(boolean jsonSchemaValidation) { this.jsonSchemaValidation = jsonSchemaValidation; }

    public String getJsonSchema() { return jsonSchema; }
    public void setJsonSchema(String jsonSchema) { this.jsonSchema = jsonSchema; }

    public boolean isJsonSchemaReportError() { return jsonSchemaReportError; }
    public void setJsonSchemaReportError(boolean jsonSchemaReportError) { this.jsonSchemaReportError = jsonSchemaReportError; }

    public String getSchemaRegistryUrl() { return schemaRegistryUrl; }
    /** Trimmed, like the URL every registry request is built from (#261). */
    public void setSchemaRegistryUrl(String schemaRegistryUrl) { this.schemaRegistryUrl = trim(schemaRegistryUrl); }

    public String getSchemaRegistryCredentialAlias() { return schemaRegistryCredentialAlias; }
    /** Trimmed, like {@link #setCredentialAlias(String)}. */
    public void setSchemaRegistryCredentialAlias(String schemaRegistryCredentialAlias) {
        this.schemaRegistryCredentialAlias = trim(schemaRegistryCredentialAlias);
    }

    public String getAvroOutputFormat() { return avroOutputFormat; }
    public void setAvroOutputFormat(String avroOutputFormat) { this.avroOutputFormat = avroOutputFormat; }

    public boolean isAvroValueDeserialization() { return avroValueDeserialization; }
    public void setAvroValueDeserialization(boolean avroValueDeserialization) { this.avroValueDeserialization = avroValueDeserialization; }

    public boolean isAvroValueSerialization() { return avroValueSerialization; }
    public void setAvroValueSerialization(boolean avroValueSerialization) { this.avroValueSerialization = avroValueSerialization; }

    // --- DLQ Getters/Setters ---

    public boolean isDlqEnabled() { return dlqEnabled; }
    public void setDlqEnabled(boolean dlqEnabled) { this.dlqEnabled = dlqEnabled; }

    public String getDlqTopic() { return dlqTopic; }
    public void setDlqTopic(String dlqTopic) { this.dlqTopic = dlqTopic; }

    public int getDlqMaxRetries() { return dlqMaxRetries; }
    public void setDlqMaxRetries(int dlqMaxRetries) { this.dlqMaxRetries = dlqMaxRetries; }

    public String getDlqCredentialAlias() { return dlqCredentialAlias; }
    /** Trimmed, like {@link #setCredentialAlias(String)}. */
    public void setDlqCredentialAlias(String dlqCredentialAlias) { this.dlqCredentialAlias = trim(dlqCredentialAlias); }

    // --- Smart Retry Getters/Setters ---

    public boolean isRetryOnlyTransientErrors() { return retryOnlyTransientErrors; }
    public void setRetryOnlyTransientErrors(boolean retryOnlyTransientErrors) { this.retryOnlyTransientErrors = retryOnlyTransientErrors; }

    public String getErrorHandling() { return errorHandling; }
    public void setErrorHandling(String errorHandling) { this.errorHandling = errorHandling; }
    /** @return true if failed messages without a DLQ are skipped rather than retried */
    public boolean isSkipFailedMessages() { return "SKIP".equalsIgnoreCase(errorHandling); }

    public int getRetryDelaySeconds() { return retryDelaySeconds; }
    public void setRetryDelaySeconds(int retryDelaySeconds) { this.retryDelaySeconds = retryDelaySeconds; }

    // --- Producer Outer Retry Getters/Setters ---

    public int getProducerRetryMaxAttempts() { return producerRetryMaxAttempts; }
    public void setProducerRetryMaxAttempts(int producerRetryMaxAttempts) { this.producerRetryMaxAttempts = producerRetryMaxAttempts; }

    public int getProducerRetryDelaySeconds() { return producerRetryDelaySeconds; }
    public void setProducerRetryDelaySeconds(int producerRetryDelaySeconds) { this.producerRetryDelaySeconds = producerRetryDelaySeconds; }

    public boolean isProducerRetryOnlyTransientErrors() { return producerRetryOnlyTransientErrors; }
    public void setProducerRetryOnlyTransientErrors(boolean producerRetryOnlyTransientErrors) { this.producerRetryOnlyTransientErrors = producerRetryOnlyTransientErrors; }

    public int getProducerRetryTotalBudgetSeconds() { return producerRetryTotalBudgetSeconds; }
    public void setProducerRetryTotalBudgetSeconds(int producerRetryTotalBudgetSeconds) { this.producerRetryTotalBudgetSeconds = producerRetryTotalBudgetSeconds; }

    // --- Auto-Pause Getters/Setters ---

    public boolean isAutoPauseEnabled() { return autoPauseEnabled; }
    public void setAutoPauseEnabled(boolean autoPauseEnabled) { this.autoPauseEnabled = autoPauseEnabled; }

    public int getAutoPauseErrorThreshold() { return autoPauseErrorThreshold; }
    public void setAutoPauseErrorThreshold(int autoPauseErrorThreshold) { this.autoPauseErrorThreshold = autoPauseErrorThreshold; }

    public int getAutoPauseCooldownSeconds() { return autoPauseCooldownSeconds; }
    public void setAutoPauseCooldownSeconds(int autoPauseCooldownSeconds) { this.autoPauseCooldownSeconds = autoPauseCooldownSeconds; }

    // --- Producer Getters/Setters ---

    public String getProducerBatchMode() { return producerBatchMode; }
    public void setProducerBatchMode(String producerBatchMode) { this.producerBatchMode = producerBatchMode; }

    public String getAcks() { return acks; }
    /** {@code all} in any case is passed in Kafka's spelling (#261). */
    public void setAcks(String acks) { this.acks = kafkaKeyword(acks, "all", "-1", "0", "1"); }

    public String getCompressionType() { return compressionType; }
    /** The codec names in any case are passed in Kafka's spelling (#261). */
    public void setCompressionType(String compressionType) {
        this.compressionType = kafkaKeyword(compressionType, "none", "gzip", "snappy", "lz4", "zstd");
    }

    public int getMaxRequestSizeKb() { return maxRequestSizeKb; }
    public void setMaxRequestSizeKb(int maxRequestSizeKb) { this.maxRequestSizeKb = maxRequestSizeKb; }

    public int getProducerBatchSizeKb() { return producerBatchSizeKb; }
    public void setProducerBatchSizeKb(int producerBatchSizeKb) { this.producerBatchSizeKb = producerBatchSizeKb; }

    public long getBufferMemoryKb() { return bufferMemoryKb; }
    public void setBufferMemoryKb(long bufferMemoryKb) { this.bufferMemoryKb = bufferMemoryKb; }

    public boolean isEnableIdempotence() { return enableIdempotence; }
    public void setEnableIdempotence(boolean enableIdempotence) { this.enableIdempotence = enableIdempotence; }

    public boolean isEnableTransactions() { return enableTransactions; }
    public void setEnableTransactions(boolean enableTransactions) { this.enableTransactions = enableTransactions; }

    public String getTransactionalIdPrefix() { return transactionalIdPrefix; }
    public void setTransactionalIdPrefix(String transactionalIdPrefix) { this.transactionalIdPrefix = transactionalIdPrefix; }

    public int getMaxConcurrentTransactions() { return maxConcurrentTransactions; }
    public void setMaxConcurrentTransactions(int maxConcurrentTransactions) { this.maxConcurrentTransactions = maxConcurrentTransactions; }

    /** @deprecated ignored; see {@link #transactionV2Enabled}. */
    @Deprecated
    public boolean isTransactionV2Enabled() { return transactionV2Enabled; }

    /** @deprecated ignored; see {@link #transactionV2Enabled}. */
    @Deprecated
    public void setTransactionV2Enabled(boolean transactionV2Enabled) { this.transactionV2Enabled = transactionV2Enabled; }

    public String getAllowedHeaders() { return allowedHeaders; }
    public void setAllowedHeaders(String allowedHeaders) { this.allowedHeaders = allowedHeaders; }

    public int getDeliveryTimeoutSeconds() { return deliveryTimeoutSeconds; }
    public void setDeliveryTimeoutSeconds(int deliveryTimeoutSeconds) { this.deliveryTimeoutSeconds = deliveryTimeoutSeconds; }


    public boolean isSchemaRegistryEnabled() { return schemaRegistryEnabled; }
    public void setSchemaRegistryEnabled(boolean schemaRegistryEnabled) { this.schemaRegistryEnabled = schemaRegistryEnabled; }

    public boolean isAutoRegisterSchemas() { return autoRegisterSchemas; }
    public void setAutoRegisterSchemas(boolean autoRegisterSchemas) { this.autoRegisterSchemas = autoRegisterSchemas; }

    public String getSubjectNameStrategy() { return subjectNameStrategy; }
    public void setSubjectNameStrategy(String subjectNameStrategy) { this.subjectNameStrategy = subjectNameStrategy; }

    // --- Diagnostics Getter/Setter ---

    public String getDiagnosticsLevel() { return diagnosticsLevel; }
    public void setDiagnosticsLevel(String diagnosticsLevel) { this.diagnosticsLevel = diagnosticsLevel; }

    /** True at diagnostics level FULL: the receiver's node-fault escalation then adds a thread dump. */
    public boolean isDiagnosticsLevelFull() { return "FULL".equalsIgnoreCase(diagnosticsLevel); }

    public boolean isWriteMplErrorAttachment() { return writeMplErrorAttachment; }
    public void setWriteMplErrorAttachment(boolean writeMplErrorAttachment) {
        this.writeMplErrorAttachment = writeMplErrorAttachment;
    }
}
