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
| `securityProtocol` | `SASL_SSL` | Security protocol, covering transport and authentication in one value: `SASL_SSL` (UI: "SASL_SSL (SASL over TLS)"), `SSL` (UI: "SSL (TLS, certificate authentication)"), `SASL_PLAINTEXT` (UI: "SASL_PLAINTEXT (no TLS)"), `PLAINTEXT` (UI: "PLAINTEXT (no TLS, no authentication)"). Managed brokers such as Confluent Cloud accept TLS only. |
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
| `jsonSchemaValidation` | `false` | Enable JSON Schema validation of incoming messages. |
| `jsonSchema` | — | Inline JSON Schema for message validation. |
| `jsonSchemaReportError` | `false` | Report JSON Schema validation failures as errors in CPI monitoring; otherwise invalid messages are dropped. |

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
| `autoRegisterSchemas` | `false` | Automatically register schemas with Schema Registry. |
| `subjectNameStrategy` | `TopicNameStrategy` | Subject naming strategy. `TopicNameStrategy` is the supported strategy for deserialization. |
| `avroOutputFormat` | `JSON` | Avro output format: `JSON`, `XML`. |
| `avroValueDeserialization` | `true` | Deserialize message values using Avro. Requires Schema Registry. |

For details on Avro integration, see [Avro / Schema Registry](features/avro-schema-registry.md).

### Error Handling

**Dead Letter Queue**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dlqEnabled` | `false` | Enable Dead Letter Queue routing for failed messages. |
| `dlqTopic` | — | Topic name for the Dead Letter Queue. |
| `dlqMaxRetries` | `3` | Maximum processing retries before routing to the DLQ. |
| `dlqCredentialAlias` | — | SASL credential alias for the DLQ Kafka cluster, if different from the main connection. |
| `retryOnlyTransientErrors` | `true` | Retry only transient errors; send permanent errors directly to the DLQ. |
| `retryDelaySeconds` | `0` | Initial retry delay in seconds with exponential backoff capped at 300 seconds. |
| `writeMplErrorAttachment` | `true` | Write the full error diagnostic (including full stack trace) as MPL attachment `KafkaAdapterError`. Disable to keep only searchable MPL headers/attributes. |

**Auto-Pause on Errors**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `autoPauseEnabled` | `false` | Automatically pause the consumer after consecutive processing errors. |
| `autoPauseErrorThreshold` | `5` | Consecutive processing errors required to activate auto-pause. |
| `autoPauseCooldownSeconds` | `60` | Initial auto-pause duration in seconds; doubles after subsequent failures, capped at 900 seconds. |

For details on DLQ and retry behavior, see [Dead Letter Queue](features/dead-letter-queue.md).

---

## Receiver (Producer)

### Connection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bootstrapServers` | _(required)_ | Kafka bootstrap servers, comma-separated. |
| `topic` | _(required)_ | Kafka topic to produce messages to. |

**Security**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `securityProtocol` | `SASL_SSL` | Security protocol, covering transport and authentication in one value: `SASL_SSL` (UI: "SASL_SSL (SASL over TLS)"), `SSL` (UI: "SSL (TLS, certificate authentication)"), `SASL_PLAINTEXT` (UI: "SASL_PLAINTEXT (no TLS)"), `PLAINTEXT` (UI: "PLAINTEXT (no TLS, no authentication)"). Managed brokers such as Confluent Cloud accept TLS only. |
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
| `acks` | `all` | Producer acknowledgments: `all`, `1`, `0`. |
| `enableIdempotence` | `true` | Enable idempotent producer. |
| `deliveryTimeoutSeconds` | `120` | Maximum delivery time in seconds, including retries. |

**Header Mapping**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `allowedHeaders` | `*` | Pipe-separated list of exchange headers to forward as Kafka record headers. Use `*` for all. Note: headers explicitly mapped in a batch payload (JSON/XML) bypass this filter and overwrite exchange headers of the same name. |

### Advanced

**Transactions**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enableTransactions` | `false` | Enable transactional batching (creates a new transactional producer per batch). |
| `transactionalIdPrefix` | — | Prefix for `transactional.id` (e.g. `my-app-txn`). Required if `enableTransactions` is `true`. |
| `maxConcurrentTransactions` | `5` | Maximum number of concurrent transactional producers per worker node. |

**Performance Tuning**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `compressionType` | `none` | Compression type: `none`, `gzip`, `lz4`, `zstd`. |
| `maxRequestSizeKb` | `5120` | Maximum request size in KB. |
| `producerBatchSizeKb` | `1024` | Kafka producer internal batch size in KB (UI label: "Producer Batch Size (KB)"). Controls how many records the client buffers before sending to the broker. |
| `bufferMemoryKb` | `32768` | Total memory for producer buffering in KB. |

**Diagnostics**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `diagnosticsLevel` | `STANDARD` | Diagnostic output level. `STANDARD` (default) is fully diagnostic on its own: every failure produces one structured ERROR line with the complete serialised cause chain. `FULL` adds exactly one thing, a bounded thread dump (at most 20 threads, 10 frames each, with lock owners) attached to the node-fault escalation that fires when the same fault recurs 5 times in 20 minutes. Leave this on `STANDARD` unless you are actively investigating such a fault. |
| `writeMplErrorAttachment` | `true` | Write the full error diagnostic (including full stack trace) as MPL attachment `KafkaAdapterError`. Disable to keep only searchable MPL headers/attributes. |

### Message Handling

| Parameter | Default | Description |
|-----------|---------|-------------|
| `jsonSchemaValidation` | `false` | Enable JSON Schema validation of outgoing messages. |
| `jsonSchema` | — | Inline JSON Schema for message validation. |
| `jsonSchemaReportError` | `false` | Report JSON Schema validation failures as errors in CPI monitoring; otherwise invalid messages are dropped. |

For more details, see [JSON Schema Validation](features/json-schema-validation.md).

### Avro / Schema Registry

| Parameter | Default | Description |
|-----------|---------|-------------|
| `schemaRegistryEnabled` | `false` | Enable Confluent Schema Registry integration. |
| `schemaRegistryUrl` | — | Confluent Schema Registry URL. |
| `schemaRegistryCredentialAlias` | — | Credential alias for Schema Registry authentication. |
| `autoRegisterSchemas` | `false` | Automatically register schemas with Schema Registry. |
| `subjectNameStrategy` | `TopicNameStrategy` | Subject naming strategy. `TopicNameStrategy` is the supported strategy for serialization. |
| `avroValueSerialization` | `true` | Serialize message values using Avro. Requires Schema Registry. |

For details on Avro integration, see [Avro / Schema Registry](features/avro-schema-registry.md).
