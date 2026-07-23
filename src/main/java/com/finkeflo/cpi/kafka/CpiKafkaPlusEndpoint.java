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

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultPollingEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@UriEndpoint(scheme = "cpi-kafka-plus", syntax = "cpi-kafka-plus:topic", title = "CPI Kafka Plus")
public class CpiKafkaPlusEndpoint extends DefaultPollingEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusEndpoint.class);

    private final CpiKafkaPlusComponent component;
    private String topicFromUri;

    // --- Connection ---
    @UriParam(label = "connection", description = "Kafka Bootstrap Servers (comma-separated)")
    private String bootstrapServers;

    @UriParam(label = "connection", description = "Kafka Topic(s) to consume from (comma-separated for multiple)")
    private String topic;

    @UriParam(label = "connection", description = "Consumer Group ID")
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
            description = "Optional CPI Keystore alias for custom broker CAs and/or client certificates (mTLS). "
                    + "Leave empty to use the JVM default truststore only.")
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
            description = "Maximum wait time in ms to fill a batch")
    private long batchTimeout = 5000;

    @UriParam(label = "batch", defaultValue = "JSON_ARRAY",
            description = "Batch output format: JSON_ARRAY, XML_LIST, SPLIT_EXCHANGES")
    private String batchOutputFormat = "JSON_ARRAY";

    @UriParam(label = "batch", defaultValue = "false",
            description = "When enabled, XML values in XML_LIST batch output are embedded "
            + "as child elements instead of text. Non-XML values use CDATA wrapping. "
            + "Only applies when batchOutputFormat is XML_LIST.")
    private boolean embedXmlValues = false;

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
            description = "Enable idempotent producer (exactly-once semantics)")
    private boolean enableIdempotence = true;

    @UriParam(label = "producer", defaultValue = "false",
            description = "Enable transactional batching (creates a new transactional producer per batch)")
    private boolean enableTransactions = false;

    @UriParam(label = "producer",
            description = "Prefix for transactional.id (e.g. my-app-txn). Required if enableTransactions is true.")
    private String transactionalIdPrefix;

    @UriParam(label = "producer", defaultValue = "5",
            description = "Maximum number of concurrent transactional producers per worker node")
    private int maxConcurrentTransactions = 5;

    @UriParam(label = "producer", defaultValue = "*",
            description = "Pipe-separated list of headers to send to Kafka (e.g. SAP_*|MyHeader|*). Use * for all.")
    private String allowedHeaders = "*";

    // retries: commented out — deliveryTimeoutSeconds is the effective limit.
    // Kafka uses Integer.MAX_VALUE retries internally when idempotence is enabled.
    // Re-enable if a use case requires explicit retry control.
    // @UriParam(label = "producer", defaultValue = "3",
    //         description = "Number of retries for failed sends")
    // private int retries = 3;

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
            description = "Report JSON Schema validation failures as ERROR in CPI Monitoring (MPL). "
                    + "When disabled, invalid messages are silently dropped.")
    private boolean jsonSchemaReportError = false;

    // --- Error Handling / DLQ ---
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
            description = "SASL credential alias for DLQ Kafka cluster (if different from main connection)")
    private String dlqCredentialAlias;

    // --- Smart Retry ---
    @UriParam(label = "errorHandling", defaultValue = "true",
            description = "Only retry transient errors (e.g. timeouts, connection failures). "
                    + "Permanent errors like mapping failures or NullPointerExceptions are "
                    + "sent directly to the DLQ without retries.")
    private boolean retryOnlyTransientErrors = true;

    @UriParam(label = "errorHandling", defaultValue = "0",
            description = "Initial delay between retries in seconds. "
                    + "The delay doubles after each retry (exponential backoff), "
                    + "capped at 300 seconds. "
                    + "Example: with 2s delay and 3 retries the waits are 2s, 4s, 8s. "
                    + "Set to 0 to retry immediately without delay.")
    private int retryDelaySeconds = 0;

    // --- Auto-Pause (Circuit Breaker) ---
    @UriParam(label = "errorHandling", defaultValue = "false",
            description = "Automatically pause the consumer after consecutive processing errors")
    private boolean autoPauseEnabled = false;

    @UriParam(label = "errorHandling", defaultValue = "5",
            description = "Number of consecutive processing errors before auto-pause activates")
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
            description = "Subject naming strategy: TopicNameStrategy, RecordNameStrategy, TopicRecordNameStrategy")
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

    public CpiKafkaPlusEndpoint() {
        this.component = null;
    }

    public CpiKafkaPlusEndpoint(String endpointUri, CpiKafkaPlusComponent component) throws URISyntaxException {
        super(endpointUri, component);
        this.component = component;
    }

    public CpiKafkaPlusEndpoint(String uri, String remaining, CpiKafkaPlusComponent component) throws URISyntaxException {
        this(uri, component);
        this.topicFromUri = remaining;
    }

    /**
     * Validates configuration settings that are shared between Consumer and Producer.
     * Called from doStart() of both sides for fail-fast behaviour.
     */
    public void validateConfiguration() {
        if (schemaRegistryEnabled
                && (schemaRegistryUrl == null || schemaRegistryUrl.isEmpty())) {
            throw new IllegalArgumentException(
                    "Schema Registry is enabled but schemaRegistryUrl is not configured.");
        }
        if (jsonSchemaValidation) {
            if (jsonSchema == null || jsonSchema.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "JSON Schema validation is enabled but no JSON Schema is configured. "
                        + "Please provide a valid JSON Schema (draft-07) or disable JSON Schema validation.");
            }
        }
        if (securityProtocol != null
                && securityProtocol.toUpperCase().contains("SASL")
                && (credentialAlias == null || credentialAlias.trim().isEmpty())) {
            throw new IllegalArgumentException(
                    "Security protocol " + securityProtocol
                    + " requires SASL authentication but no credentialAlias is configured.");
        }
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
        consumer.setDelay(
                CpiKafkaPlusConsumer.computeSchedulerDelaySeconds(pollingIntervalSeconds) * 1000);
        consumer.setInitialDelay(5000);
        consumer.setUseFixedDelay(true);
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
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public String getSecurityProtocol() { return securityProtocol; }
    public void setSecurityProtocol(String securityProtocol) { this.securityProtocol = securityProtocol; }

    public String getSaslMechanism() { return saslMechanism; }
    public void setSaslMechanism(String saslMechanism) { this.saslMechanism = saslMechanism; }

    public String getCredentialAlias() { return credentialAlias; }
    public void setCredentialAlias(String credentialAlias) { this.credentialAlias = credentialAlias; }

    public String getSslKeystoreAlias() { return sslKeystoreAlias; }
    public void setSslKeystoreAlias(String sslKeystoreAlias) { this.sslKeystoreAlias = sslKeystoreAlias; }

    public long getPollingIntervalSeconds() { return pollingIntervalSeconds; }
    public void setPollingIntervalSeconds(long pollingIntervalSeconds) { this.pollingIntervalSeconds = pollingIntervalSeconds; }

    public String getAutoOffsetReset() { return autoOffsetReset; }
    public void setAutoOffsetReset(String autoOffsetReset) { this.autoOffsetReset = autoOffsetReset; }

    public int getMaxPollRecords() { return maxPollRecords; }
    public void setMaxPollRecords(int maxPollRecords) { this.maxPollRecords = maxPollRecords; }

    public String getCommitStrategy() { return commitStrategy; }
    public void setCommitStrategy(String commitStrategy) { this.commitStrategy = commitStrategy; }

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

    public boolean isEmbedXmlValues() { return embedXmlValues; }
    public void setEmbedXmlValues(boolean embedXmlValues) { this.embedXmlValues = embedXmlValues; }

    public boolean isJsonSchemaValidation() { return jsonSchemaValidation; }
    public void setJsonSchemaValidation(boolean jsonSchemaValidation) { this.jsonSchemaValidation = jsonSchemaValidation; }

    public String getJsonSchema() { return jsonSchema; }
    public void setJsonSchema(String jsonSchema) { this.jsonSchema = jsonSchema; }

    public boolean isJsonSchemaReportError() { return jsonSchemaReportError; }
    public void setJsonSchemaReportError(boolean jsonSchemaReportError) { this.jsonSchemaReportError = jsonSchemaReportError; }

    public String getSchemaRegistryUrl() { return schemaRegistryUrl; }
    public void setSchemaRegistryUrl(String schemaRegistryUrl) { this.schemaRegistryUrl = schemaRegistryUrl; }

    public String getSchemaRegistryCredentialAlias() { return schemaRegistryCredentialAlias; }
    public void setSchemaRegistryCredentialAlias(String schemaRegistryCredentialAlias) { this.schemaRegistryCredentialAlias = schemaRegistryCredentialAlias; }

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
    public void setDlqCredentialAlias(String dlqCredentialAlias) { this.dlqCredentialAlias = dlqCredentialAlias; }

    // --- Smart Retry Getters/Setters ---

    public boolean isRetryOnlyTransientErrors() { return retryOnlyTransientErrors; }
    public void setRetryOnlyTransientErrors(boolean retryOnlyTransientErrors) { this.retryOnlyTransientErrors = retryOnlyTransientErrors; }

    public int getRetryDelaySeconds() { return retryDelaySeconds; }
    public void setRetryDelaySeconds(int retryDelaySeconds) { this.retryDelaySeconds = retryDelaySeconds; }

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
    public void setAcks(String acks) { this.acks = acks; }

    public String getCompressionType() { return compressionType; }
    public void setCompressionType(String compressionType) { this.compressionType = compressionType; }

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

    public String getAllowedHeaders() { return allowedHeaders; }
    public void setAllowedHeaders(String allowedHeaders) { this.allowedHeaders = allowedHeaders; }

    // public int getRetries() { return retries; }
    // public void setRetries(int retries) { this.retries = retries; }

    public int getDeliveryTimeoutSeconds() { return deliveryTimeoutSeconds; }
    public void setDeliveryTimeoutSeconds(int deliveryTimeoutSeconds) { this.deliveryTimeoutSeconds = deliveryTimeoutSeconds; }


    public boolean isSchemaRegistryEnabled() { return schemaRegistryEnabled; }
    public void setSchemaRegistryEnabled(boolean schemaRegistryEnabled) { this.schemaRegistryEnabled = schemaRegistryEnabled; }

    public boolean isAutoRegisterSchemas() { return autoRegisterSchemas; }
    public void setAutoRegisterSchemas(boolean autoRegisterSchemas) { this.autoRegisterSchemas = autoRegisterSchemas; }

    public String getSubjectNameStrategy() { return subjectNameStrategy; }
    public void setSubjectNameStrategy(String subjectNameStrategy) { this.subjectNameStrategy = subjectNameStrategy; }
}
