# Configuration Reference

All adapter parameters are configured on the `CpiKafkaPlusEndpoint`. The sections below follow the UI tab layout in CPI — **Sender** parameters are for the _Kafka → CPI_ consumer channel, **Receiver** parameters for the _CPI → Kafka_ producer channel.

---

## Sender (Consumer)

### Connection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bootstrapServers` | _(required)_ | Kafka bootstrap servers, comma-separated. |
| `topic` | _(required)_ | Kafka topic or comma-separated topics to consume from. |
| `groupId` | — | Consumer group ID. |

**Security**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `securityProtocol` | `SASL_SSL` | Security protocol, covering transport and authentication in one value: `SASL_SSL` (UI: "SASL_SSL (SASL over TLS)"), `SSL` (UI: "SSL (TLS, client certificate optional via Keystore Alias)") — TLS with a client certificate (mTLS) only when `sslKeystoreAlias` holds one, `SASL_PLAINTEXT` (UI: "SASL_PLAINTEXT (no TLS)"), `PLAINTEXT` (UI: "PLAINTEXT (no TLS, no authentication)"). Managed brokers such as Confluent Cloud accept TLS only. |
| `saslMechanism` | `PLAIN` | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. |
| `credentialAlias` | — | Credential alias for SASL username/password from CPI Secure Store. |
| `sslKeystoreAlias` | — | Leave empty for brokers with a publicly trusted certificate (e.g. Confluent Cloud) — the JVM default truststore is used and TLS is still active. Set a CPI Keystore alias only for a private/company CA, a self-signed broker certificate, or client-certificate authentication (mTLS). |

For detailed security setup, see [Authentication](security/authentication.md).

### Consumption

**Consumption Mode**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `consumptionMode` | `SCHEDULED` | `SCHEDULED` polls every `pollingIntervalSeconds`. `STREAMING` uses greedy scheduling: while a poll returns records the next poll fires immediately (continuous, minimal latency), falling back to a heartbeat cadence of `batchTimeout` + 1 s when idle. In `STREAMING`, `pollingIntervalSeconds` and `drainEnabled` are ignored. |

**Offsets & Commit**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `autoOffsetReset` | `latest` | Auto offset reset: `earliest` or `latest`. |
| `commitStrategy` | `BATCH_COMPLETE` | Offset commit strategy: `AUTO`, `BATCH_COMPLETE`. |

**Polling**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pollingIntervalSeconds` | `5` | Time in seconds between poll cycles. Range: 1–21600. Ignored when `consumptionMode=STREAMING`. |
| `maxPollRecords` | `500` | Maximum records fetched per `kafkaConsumer.poll()` call. |
| `batchTimeout` | `5000` | Maximum time in milliseconds `kafkaConsumer.poll()` blocks waiting for the broker to return records. Only affects idle behaviour (empty topic); when records are available `poll()` returns immediately. |

### Advanced

**Fetch Tuning**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `maxPartitionFetchSizeKb` | `1024` | Maximum data returned by the broker per partition per poll, in KB. |
| `fetchMinBytes` | `1` | Kafka `fetch.min.bytes`: minimum data (in bytes) the broker accumulates before responding to a fetch request. Raise this to encourage larger, more efficient batches under low load. |
| `fetchMaxWaitMs` | `500` | Kafka `fetch.max.wait.ms`: maximum time the broker waits to satisfy `fetchMinBytes` before returning whatever records are currently available. |

**Backlog Drain**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `drainEnabled` | `false` | Poll repeatedly until the topic is empty. Ignored when `consumptionMode=STREAMING`. |
| `minBacklogToDrain` | `0` | Minimum records in an extra drain poll required to continue draining; `0` drains until empty. |

### Message Handling

**JSON Schema Validation**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `jsonSchemaValidation` | `false` | Enable JSON Schema validation of incoming messages. A message that fails validation never reaches the IFlow: it goes to the DLQ if one is enabled, otherwise it is discarded and its offset committed. `errorHandling` does not apply to it. |
| `jsonSchema` | — | Inline JSON Schema for message validation. |
| `jsonSchemaReportError` | `false` | Also write a failed MPL entry, with the payload, for every message that fails validation. When `false`, a discarded message leaves only a WARN log line, which does not reach the tenant trace in production. |

For more details, see [JSON Schema Validation](features/json-schema-validation.md).

**Delivery to IFlow (Batching)**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batchMode` | `true` | Enable batch mode: multiple records per iFlow execution (one MPL entry per batch). |
| `batchSize` | `100` | Maximum records per batch (UI label: "Max Records per IFlow Run (MPL)"). |
| `batchOutputFormat` | `JSON_ARRAY` | Batch output format: `JSON_ARRAY`, `XML_LIST`. (`SPLIT_EXCHANGES` accepted at runtime for backward compatibility but removed from the UI as of 1.2.0.) |

In `XML_LIST` mode, each `<value>` element carries a `format` attribute (`"xml"` for directly embedded XML, `"text"` for text/CDATA content). Values that look like XML are always auto-detected and embedded; there is no configuration option for this. See [Batch Processing](features/batch-processing.md) for details.

### Avro / Schema Registry

| Parameter | Default | Description |
|-----------|---------|-------------|
| `schemaRegistryEnabled` | `false` | Enable Confluent Schema Registry integration. |
| `schemaRegistryUrl` | — | Confluent Schema Registry URL. |
| `schemaRegistryCredentialAlias` | — | Credential alias for Schema Registry authentication. |
| `avroOutputFormat` | `JSON` | Avro output format: `JSON`, `XML`. |
| `avroValueDeserialization` | `true` | Deserialize message values using Avro. Requires Schema Registry. |

For details on Avro integration, see [Avro / Schema Registry](features/avro-schema-registry.md).

### Error Handling

**Failed Messages**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `errorHandling` | `RETRY` | **Sender (consumer) direction only.** What happens to a message whose processing fails when no DLQ is enabled. `RETRY` (*Retry Failed Message*) retries the same offset until it succeeds; the partition waits meanwhile. `SKIP` (*Skip Failed Message*) continues with the next offset; the failed message is lost (at-most-once). Has no effect while the DLQ is enabled. Available from metadata version 1.4; iFlows on an older version (1.0 to 1.3) have no such field and run with `RETRY` — move them to 1.4 with *Update Version* to select `SKIP`. With **Auto-Pause** enabled every retry counts as a failure, so a message that can never succeed pauses the whole consumer; use a DLQ for such messages. |
| `retryDelaySeconds` | `0` | **Sender (consumer) direction only.** Initial retry delay in seconds with exponential backoff, capped at 300 seconds. Applies to the retries before a message is dead-lettered and to *Retry Failed Message*, where the first wait is at least 1 second. The receiver direction uses `producerRetryDelaySeconds`, which is constant rather than exponential. |

**Dead Letter Queue**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dlqEnabled` | `false` | Enable Dead Letter Queue routing for failed messages. |
| `dlqTopic` | — | Topic name for the Dead Letter Queue. |
| `dlqMaxRetries` | `3` | Maximum processing retries before routing to the DLQ. |
| `dlqCredentialAlias` | — | SASL credential alias for writing to the DLQ topic, if that needs other credentials than the main connection. The DLQ topic is always on the same cluster (`bootstrapServers`); leave empty to reuse `credentialAlias`. |
| `retryOnlyTransientErrors` | `true` | **Sender (consumer) direction only.** Retry only transient errors; send permanent errors directly to the DLQ. The receiver direction uses `producerRetryOnlyTransientErrors`. |
| `writeMplErrorAttachment` | `true` | Write the full error diagnostic (including full stack trace) as MPL attachment `KafkaAdapterError`. Disable to keep only searchable MPL headers/attributes. |

**Auto-Pause on Errors**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `autoPauseEnabled` | `false` | Automatically pause the consumer after consecutive processing errors. |
| `autoPauseErrorThreshold` | `5` | Consecutive processing errors required to activate auto-pause. Every failed IFlow call counts, including records that end up in the DLQ. |
| `autoPauseCooldownSeconds` | `60` | Initial auto-pause duration in seconds; doubles after subsequent failures, capped at 900 seconds. |

For details on DLQ and retry behavior, see [Dead Letter Queue](features/dead-letter-queue.md).

---

## Receiver (Producer)

### Connection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bootstrapServers` | _(required)_ | Kafka bootstrap servers, comma-separated. |
| `topic` | _(required)_ | Kafka topic to produce messages to. Can be a Camel Simple expression resolved per message, e.g. `${header.targetTopic}` or `${property.targetTopic}`; a message whose expression does not resolve to a topic name fails. A `CamelKafkaTopic` header on the message overrides the configured topic. |

**Security**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `securityProtocol` | `SASL_SSL` | Security protocol, covering transport and authentication in one value: `SASL_SSL` (UI: "SASL_SSL (SASL over TLS)"), `SSL` (UI: "SSL (TLS, client certificate optional via Keystore Alias)") — TLS with a client certificate (mTLS) only when `sslKeystoreAlias` holds one, `SASL_PLAINTEXT` (UI: "SASL_PLAINTEXT (no TLS)"), `PLAINTEXT` (UI: "PLAINTEXT (no TLS, no authentication)"). Managed brokers such as Confluent Cloud accept TLS only. |
| `saslMechanism` | `PLAIN` | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. |
| `credentialAlias` | — | Credential alias for SASL username/password from CPI Secure Store. |
| `sslKeystoreAlias` | — | Leave empty for brokers with a publicly trusted certificate (e.g. Confluent Cloud) — the JVM default truststore is used and TLS is still active. Set a CPI Keystore alias only for a private/company CA, a self-signed broker certificate, or client-certificate authentication (mTLS). |

For detailed security setup, see [Authentication](security/authentication.md).

### Producing

**Send Mode**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `producerBatchMode` | `NONE` | Batch send mode: `NONE`, `JSON_ARRAY`, `XML_LIST`. |

**Delivery Semantics**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `acks` | `all` | Producer acknowledgments: `all`, `1`, `0`. Only used with `enableIdempotence=false`; otherwise the adapter always sends with `all`. |
| `enableIdempotence` | `true` | Enable idempotent producer: the broker discards duplicates from the producer's own internal retries. Forces `acks=all`. Not end-to-end exactly-once: a message the caller sends twice is written twice. |
| `deliveryTimeoutSeconds` | `120` | Maximum delivery time in seconds, including retries. With `enableTransactions` the adapter derives `transaction.timeout.ms` from this value (plus up to 30 s commit headroom, never below 60 s), so the maximum is 870 s — above that a broker rejects the producer. |

**Header Mapping**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `allowedHeaders` | `*` | Pipe-separated list of exchange headers to forward as Kafka record headers. Use `*` for all. Note: headers explicitly mapped in a batch payload (JSON/XML) bypass this filter and overwrite exchange headers of the same name. |

### Advanced

**Transactions**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enableTransactions` | `false` | Enable transactional batching (creates a new transactional producer per batch). Applies only to `producerBatchMode` `JSON_ARRAY` or `XML_LIST`; with `NONE` messages are sent without a transaction. |
| `transactionalIdPrefix` | — | Prefix for `transactional.id` (e.g. `my-app-txn`). Required if `enableTransactions` is `true`. |
| `maxConcurrentTransactions` | `5` | Maximum number of concurrent transactional producers per worker node. |

**Performance Tuning**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `compressionType` | `none` | Compression type: `none`, `gzip`, `lz4`, `zstd`. |
| `maxRequestSizeKb` | `5120` | Maximum request size in KB. |
| `producerBatchSizeKb` | `1024` | Kafka producer internal batch size in KB (UI label: "Producer Batch Size (KB)"). Controls how many records the client buffers before sending to the broker. |
| `bufferMemoryKb` | `32768` | Total memory for producer buffering in KB. |

### Message Handling

| Parameter | Default | Description |
|-----------|---------|-------------|
| `jsonSchemaValidation` | `false` | Enable JSON Schema validation of outgoing messages. An invalid message fails the exchange and is not sent. Skipped, with a warning, when `producerBatchMode` is `JSON_ARRAY` or `XML_LIST`. |
| `jsonSchema` | — | Inline JSON Schema for message validation. |
| `jsonSchemaReportError` | `false` | An invalid message always fails the exchange, whatever this setting says. `true` also writes the rejected payload to the MPL trace, if trace is active for the integration flow. |

For more details, see [JSON Schema Validation](features/json-schema-validation.md).

### Avro / Schema Registry

| Parameter | Default | Description |
|-----------|---------|-------------|
| `schemaRegistryEnabled` | `false` | Enable Confluent Schema Registry integration. |
| `schemaRegistryUrl` | — | Confluent Schema Registry URL. |
| `schemaRegistryCredentialAlias` | — | Credential alias for Schema Registry authentication. |
| `autoRegisterSchemas` | `false` | Automatically register schemas with Schema Registry. |
| `subjectNameStrategy` | `TopicNameStrategy` | Subject naming strategy. Only `TopicNameStrategy` (subject `<topic>-value`) is supported: with `RecordNameStrategy` or `TopicRecordNameStrategy` every message fails, because the record name cannot be derived from the JSON input. |
| `avroValueSerialization` | `true` | Serialize message values using Avro. Requires Schema Registry. |

For details on Avro integration, see [Avro / Schema Registry](features/avro-schema-registry.md).

### Error Handling

**Retry**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `producerRetryMaxAttempts` | `1` | Total number of send attempts, not additional attempts. `1` keeps the previous behaviour and switches the retry off. Range 1–5. |
| `producerRetryDelaySeconds` | `2` | Constant wait between attempts in seconds. Range 1–30. |
| `producerRetryOnlyTransientErrors` | `true` | Retry only transient (`RETRIABLE`) failures. `false` additionally retries an unusable transactional producer. |
| `producerRetryTotalBudgetSeconds` | `30` | Hard upper bound for all attempts of one message together. Must stay below the calling system's timeout; a Kafka-to-Kafka or scheduled channel has no such caller and can use a larger budget. Range 5–900. |

Only failures that provably wrote nothing are repeated. See [Producer Retry](features/producer-retry.md)
for the decision tree, the duplicate guarantees and the interaction with `deliveryTimeoutSeconds`,
which the adapter validates at start-up.

**Diagnostics**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `diagnosticsLevel` | `STANDARD` | Diagnostic output level. `STANDARD` (default) is fully diagnostic on its own: every failure produces one structured ERROR line with the complete serialised cause chain. `FULL` adds exactly one thing, a bounded thread dump (at most 20 threads, 10 frames each, with lock owners) attached to the node-fault escalation that fires when the same fault recurs 5 times in 20 minutes. Leave this on `STANDARD` unless you are actively investigating such a fault. |
| `writeMplErrorAttachment` | `true` | Currently without effect on the receiver: a failing receiver channel fails the exchange, and CPI writes the MPL entry without a `KafkaAdapterError` attachment. The option works on the sender. |
