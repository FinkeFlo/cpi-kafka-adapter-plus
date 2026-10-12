# Configuration Reference

All adapter parameters are configured on the `CpiKafkaPlusEndpoint`. The sections below follow the UI tab layout in CPI — **Sender** parameters are for the _Kafka → CPI_ consumer channel, **Receiver** parameters for the _CPI → Kafka_ producer channel.

A value that can never work fails the deployment with a message that names the field as the UI shows it, its parameter and the value it got: `'<UI label>' (<parameter>) must …, got: <value>`, for example `'Max Poll Records' (maxPollRecords) must be at least 1, got: 0`. A text value is quoted, so an empty value or spaces around it show. A value the adapter replaces with a working one, or a setting without effect, is logged once at deployment as ERROR (`consumer.config.*`, `producer.config.ineffective`); see [Troubleshooting](troubleshooting.md).

An externalized parameter bypasses the dropdowns, and its value reaches the adapter as it is. So the adapter removes the spaces around a number, a switch, a credential alias, the bootstrap servers and the Schema Registry URL, and takes Kafka's keywords for `autoOffsetReset`, `compressionType` and `acks` in any letter case (`LATEST`, `GZIP`, `ALL`). The consumer group ID and the transactional ID prefix are used exactly as entered: they identify the channel to Kafka.

---

## Sender (Consumer)

### Connection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bootstrapServers` | _(required)_ | Kafka bootstrap servers, comma-separated `host:port` addresses, each optionally with a listener prefix such as `SASL_SSL://`. Spaces around the addresses and empty entries, such as after a trailing comma, are removed. An address Kafka cannot read as `host:port`, or a port above 65535, fails the deployment. |
| `topic` | _(required)_ | Kafka topic or comma-separated topics to consume from. |
| `groupId` | _(required)_ | Consumer group ID. |

**Security**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `securityProtocol` | `SASL_SSL` | Security protocol, covering transport and authentication in one value: `SASL_SSL` (UI: "SASL_SSL (SASL over TLS)"), `SSL` (UI: "SSL (TLS, client certificate optional via Keystore Alias)") — TLS with a client certificate (mTLS) only when `sslKeystoreAlias` names a key pair, `SASL_PLAINTEXT` (UI: "SASL_PLAINTEXT (no TLS)"), `PLAINTEXT` (UI: "PLAINTEXT (no TLS, no authentication)"). Managed brokers such as Confluent Cloud accept TLS only. An empty externalized value connects with `PLAINTEXT`, without TLS and without authentication; the deployment logs this once as ERROR (`consumer.config.ineffective`). |
| `saslMechanism` | `PLAIN` | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, in any letter case. With a SASL protocol any other value fails the deployment. |
| `credentialAlias` | _(required)_ | Credential alias for SASL username/password from CPI Secure Store. Required when `securityProtocol` is `SASL_SSL` or `SASL_PLAINTEXT`. Neither the user name nor the password may contain a line break or another control character; the adapter refuses such a credential at the first connect, naming the alias (see [Authentication](security/authentication.md#cpi-secure-store)). |
| `sslKeystoreAlias` | — | Leave empty for brokers with a publicly trusted certificate (e.g. Confluent Cloud) — the JVM default truststore is used and TLS is still active. Setting any alias makes the adapter trust the certificates of the whole CPI tenant keystore instead of the JVM default truststore, so the broker's CA certificate (or its self-signed certificate) must then be in the tenant keystore. For client-certificate authentication (mTLS) the alias must name a key pair. |

For detailed security setup, see [Authentication](security/authentication.md).

### Consumption

**Consumption Mode**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `consumptionMode` | `SCHEDULED` | `SCHEDULED` polls every `pollingIntervalSeconds`. `STREAMING` uses greedy scheduling: while a poll returns records the next poll fires immediately (continuous, minimal latency), falling back to a heartbeat cadence of `batchTimeout` + 1 s when idle. In `STREAMING`, `pollingIntervalSeconds` and `drainEnabled` are ignored. |

**Offsets & Commit**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `autoOffsetReset` | `latest` | Where to start when the consumer group has no valid committed offset: `earliest` reads all available messages, `latest` only new ones. Both are accepted in any letter case; an externalized value may also be Kafka's `none` or `by_duration:<duration>`, and anything else is passed to Kafka as it is. This applies for example on the first deployment with a new `groupId`, after the broker deleted the group's offsets (by default once the group has had no active consumer for 7 days, broker setting `offsets.retention.minutes`) and when the committed offset no longer exists because the topic's retention deleted those messages. With `latest`, messages written before that point are never read. |
| `commitStrategy` | `BATCH_COMPLETE` | Offset commit strategy. `BATCH_COMPLETE` (UI: "After Successful Processing (At-Least-Once)") commits only records that were processed, dead-lettered or deliberately skipped. `AUTO` (UI: "Auto Commit (Periodic, may lose messages)") lets the Kafka client commit the consumer position periodically instead: records that were polled but not processed yet can be committed and lost, for example when the integration flow is stopped while a long-running IFlow call is still in progress. With `consumptionMode=SCHEDULED`, `AUTO` cannot be combined with `drainEnabled`: the channel does not start. |

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
| `drainEnabled` | `false` | Poll repeatedly until the topic is empty. Ignored when `consumptionMode=STREAMING`. Requires `commitStrategy=BATCH_COMPLETE`; with `AUTO` the channel does not start. |
| `minBacklogToDrain` | `0` | Minimum records in an extra drain poll required to continue draining; `0` drains until empty. |

### Message Handling

**JSON Schema Validation**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `jsonSchemaValidation` | `false` | Enable JSON Schema validation of incoming messages. A message that fails validation never reaches the IFlow: it goes to the DLQ if one is enabled, otherwise it is discarded and its offset committed. `errorHandling` does not apply to it. |
| `jsonSchema` | _(required)_ | Inline JSON Schema for message validation. Required when `jsonSchemaValidation` is `true`. |
| `jsonSchemaReportError` | `false` | Also write a failed MPL entry with the validation error for every message that fails validation (the payload only in its trace, if trace is active). When `false`, a discarded message leaves only a WARN log line, which does not reach the tenant trace in production. |

For more details, see [JSON Schema Validation](features/json-schema-validation.md).

**Delivery to IFlow (Batching)**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batchMode` | `true` | When `true`, the iFlow receives an envelope with up to `batchSize` records of one partition, `{"kafkaRecords":{"record":[…]}}` or the XML equivalent (see `batchOutputFormat`), not the original message body; one MPL entry per batch. When `false`, every Kafka message is its own iFlow execution with the message value as the body. |
| `batchSize` | `100` | Maximum number of records of one partition from one poll per iFlow execution (UI label: "Max Records per IFlow Run (MPL)"). Batches never span partitions or polls, so a value above `maxPollRecords` has no effect. |
| `batchOutputFormat` | `JSON_ARRAY` | Batch output format: `JSON_ARRAY` (`{"kafkaRecords":{"record":[{"key":…,"value":…,"topic":…,"partition":…,"offset":…,"timestamp":…}]}}`), `XML_LIST` (`<kafkaRecords count="n"><record>…</record></kafkaRecords>`). (`SPLIT_EXCHANGES` accepted at runtime for backward compatibility but removed from the UI as of 1.2.0.) |

In `XML_LIST` mode, each `<value>` element carries a `format` attribute (`"xml"` for directly embedded XML, `"text"` for text/CDATA content). Values that look like XML are always auto-detected and embedded; there is no configuration option for this. See [Batch Processing](features/batch-processing.md) for details.

### Avro / Schema Registry

| Parameter | Default | Description |
|-----------|---------|-------------|
| `schemaRegistryEnabled` | `false` | Enable Confluent Schema Registry integration. |
| `schemaRegistryUrl` | _(required)_ | Confluent Schema Registry URL. Required when `schemaRegistryEnabled` is `true`. It must start with `http://` or `https://`, in any letter case: while `schemaRegistryEnabled` and `avroValueDeserialization` are on, any other URL fails the deployment. |
| `schemaRegistryCredentialAlias` | — | Credential alias for Schema Registry authentication. |
| `avroOutputFormat` | `JSON` | Avro output format: `JSON`, `XML`. |
| `avroValueDeserialization` | `true` | Deserialize message values using Avro. Requires Schema Registry. |

For details on Avro integration, see [Avro / Schema Registry](features/avro-schema-registry.md).

### Error Handling

**Failed Messages**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `errorHandling` | `RETRY` | **Sender (consumer) direction only.** What happens to a message whose processing fails when no DLQ is enabled. `RETRY` (*Retry Failed Message*) retries the same offset until it succeeds; the partition waits meanwhile. `SKIP` (*Skip Failed Message*) continues with the next offset; the failed message is lost (at-most-once). Has no effect while the DLQ is enabled, and is then not checked either. Without a DLQ, any other value (for example from an externalized parameter) fails the deployment. Available from metadata version 1.4; iFlows on an older version (1.0 to 1.3) have no such field and run with `RETRY` — move them to 1.4 with *Update Version* to select `SKIP`. With **Auto-Pause** enabled every retry counts as a failure, so a message that can never succeed pauses the whole consumer; use a DLQ for such messages. |
| `retryDelaySeconds` | `0` | **Sender (consumer) direction only.** Initial retry delay in seconds with exponential backoff, capped at 300 seconds. Applies to the retries before a message is dead-lettered and to *Retry Failed Message*, where the first wait is at least 1 second. The receiver direction uses `producerRetryDelaySeconds`, which is constant rather than exponential. |

**Dead Letter Queue**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dlqEnabled` | `false` | Enable Dead Letter Queue routing for failed messages. |
| `dlqTopic` | _(required)_ | Topic name for the Dead Letter Queue. Required when `dlqEnabled` is `true`. It takes 1 to 249 characters from `a-z`, `A-Z`, `0-9`, `.`, `_` and `-`, but not `.` or `..` alone, and must not be a topic this channel consumes; anything else fails the deployment. |
| `dlqMaxRetries` | `3` | Retries after the first failed attempt before a record is routed to the DLQ (UI: "Retries before DLQ (additional attempts)"): `3` means up to 4 attempts in total, `0` routes it after the first failure. |
| `dlqCredentialAlias` | — | SASL credential alias for writing to the DLQ topic, if that needs other credentials than the main connection. The DLQ topic is always on the same cluster (`bootstrapServers`); leave empty to reuse `credentialAlias`. As there, a user name or password with a line break or another control character is refused. |
| `retryOnlyTransientErrors` | `true` | **Sender (consumer) direction only.** Retry only transient errors; send permanent errors directly to the DLQ (UI: "Retry Transient Errors Only"). The receiver direction uses `producerRetryOnlyTransientErrors`. |
| `writeMplErrorAttachment` | `true` | Write the full error diagnostic (including full stack trace) as MPL attachment `KafkaAdapterError`. Disable to keep only searchable MPL headers/attributes. |

**Auto-Pause on Errors**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `autoPauseEnabled` | `false` | Automatically pause the consumer after consecutive processing errors. |
| `autoPauseErrorThreshold` | `5` | Consecutive failed records required to activate auto-pause. A record, including one that cannot be deserialized, counts once after its last attempt, also when it is then written to the DLQ; the retries before it do not count separately. Without a DLQ a failed batch counts once; with a DLQ it is retried record by record and its failing records count. With `errorHandling` `RETRY` every new attempt on a later poll counts again. Records rejected by JSON Schema validation do not count, nor does a failure caused by stopping the integration flow. A successful record or batch resets the count. |
| `autoPauseCooldownSeconds` | `60` | Initial auto-pause duration in seconds; doubles after subsequent failures, capped at 900 seconds. A larger value is used as 900 and logged as `consumer.config.clamped` at deployment. |

For details on DLQ and retry behavior, see [Dead Letter Queue](features/dead-letter-queue.md).

---

## Receiver (Producer)

### Connection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bootstrapServers` | _(required)_ | Kafka bootstrap servers, comma-separated `host:port` addresses, each optionally with a listener prefix such as `SASL_SSL://`. Spaces around the addresses and empty entries, such as after a trailing comma, are removed. An address Kafka cannot read as `host:port`, or a port above 65535, fails the deployment. |
| `topic` | _(required)_ | Kafka topic to produce messages to. Can be a Camel Simple expression resolved per message, e.g. `${header.targetTopic}` or `${property.targetTopic}`; a message whose expression does not resolve to a topic name fails. A `CamelKafkaTopic` header on the message overrides the configured topic. |

**Security**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `securityProtocol` | `SASL_SSL` | Security protocol, covering transport and authentication in one value: `SASL_SSL` (UI: "SASL_SSL (SASL over TLS)"), `SSL` (UI: "SSL (TLS, client certificate optional via Keystore Alias)") — TLS with a client certificate (mTLS) only when `sslKeystoreAlias` names a key pair, `SASL_PLAINTEXT` (UI: "SASL_PLAINTEXT (no TLS)"), `PLAINTEXT` (UI: "PLAINTEXT (no TLS, no authentication)"). Managed brokers such as Confluent Cloud accept TLS only. An empty externalized value connects with `PLAINTEXT`, without TLS and without authentication; the deployment logs this once as ERROR (`producer.config.ineffective`). |
| `saslMechanism` | `PLAIN` | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, in any letter case. With a SASL protocol any other value fails the deployment. |
| `credentialAlias` | _(required)_ | Credential alias for SASL username/password from CPI Secure Store. Required when `securityProtocol` is `SASL_SSL` or `SASL_PLAINTEXT`. Neither the user name nor the password may contain a line break or another control character; the adapter refuses such a credential at the first connect, naming the alias (see [Authentication](security/authentication.md#cpi-secure-store)). |
| `sslKeystoreAlias` | — | Leave empty for brokers with a publicly trusted certificate (e.g. Confluent Cloud) — the JVM default truststore is used and TLS is still active. Setting any alias makes the adapter trust the certificates of the whole CPI tenant keystore instead of the JVM default truststore, so the broker's CA certificate (or its self-signed certificate) must then be in the tenant keystore. For client-certificate authentication (mTLS) the alias must name a key pair. |

For detailed security setup, see [Authentication](security/authentication.md).

### Producing

**Send Mode**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `producerBatchMode` | `NONE` | Batch send mode: `NONE`, `JSON_ARRAY`, `XML_LIST`. |

**Delivery Semantics**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `acks` | `all` | Producer acknowledgments: `all` (in any letter case), `1`, `0`. Only used with `enableIdempotence=false`; otherwise the adapter always sends with `all`. |
| `enableIdempotence` | `true` | Enable idempotent producer: the broker discards duplicates from the producer's own internal retries. Forces `acks=all`. Not end-to-end exactly-once: a message the caller sends twice is written twice. |
| `deliveryTimeoutSeconds` | `120` | Maximum delivery time in seconds, including retries. With transactional batching (`enableTransactions` with `producerBatchMode` `JSON_ARRAY` or `XML_LIST`) the adapter derives `transaction.timeout.ms` from this value (plus up to 30 s commit headroom, never below 60 s), so the maximum is then 870 s — above that a broker rejects the producer. |

**Header Mapping**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `allowedHeaders` | `*` | Pipe-separated list of exchange headers to forward as Kafka record headers. Use `*` for all. Note: headers explicitly mapped in a batch payload (JSON/XML) bypass this filter and overwrite exchange headers of the same name. |

### Advanced

**Transactions**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enableTransactions` | `false` | Enable transactional batching (creates a new transactional producer per batch). Applies only to `producerBatchMode` `JSON_ARRAY` or `XML_LIST`; with `NONE` messages are sent without a transaction. |
| `transactionalIdPrefix` | _(required)_ | Prefix for `transactional.id` (e.g. `my-app-txn`). Required with transactional batching: `enableTransactions` with `producerBatchMode` `JSON_ARRAY` or `XML_LIST`. Each transaction slot has its own id, `<prefix>-<topic hash>-<node>-<slot>`, and the adapter keeps every one of them within 249 characters: a prefix that makes the id of the last slot (`maxConcurrentTransactions` − 1) longer fails the deployment, and the message says by how many characters to shorten it. |
| `maxConcurrentTransactions` | `5` | Maximum number of concurrent transactional producers per worker node. |

**Performance Tuning**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `compressionType` | `none` | Compression type: `none`, `gzip`, `lz4`, `zstd`, in any letter case; an externalized value may also be `snappy`. |
| `maxRequestSizeKb` | `5120` | Maximum request size in KB, at least 1: with less, no message fits into a request, so the deployment fails. |
| `producerBatchSizeKb` | `1024` | Kafka producer internal batch size in KB (UI label: "Producer Batch Size (KB)"). Controls how many records the client buffers before sending to the broker. 0 switches batching off; a negative value fails the deployment. |
| `bufferMemoryKb` | `32768` | Total memory for producer buffering in KB, at least 1: with less, no message fits into the buffer, so the deployment fails. |

### Message Handling

| Parameter | Default | Description |
|-----------|---------|-------------|
| `jsonSchemaValidation` | `false` | Enable JSON Schema validation of outgoing messages. An invalid message fails the exchange and is not sent. Applies to `producerBatchMode` `NONE` only: with `JSON_ARRAY` or `XML_LIST` a batch is sent without validation, the schema is neither checked at deployment nor compiled, and the deployment logs `producer.config.ineffective` once. |
| `jsonSchema` | _(required)_ | Inline JSON Schema for message validation. Required when `jsonSchemaValidation` is `true` and `producerBatchMode` is `NONE`. |
| `jsonSchemaReportError` | `false` | An invalid message always fails the exchange, whatever this setting says. `true` also writes the rejected payload to the MPL trace, if trace is active for the integration flow. |

For more details, see [JSON Schema Validation](features/json-schema-validation.md).

### Avro / Schema Registry

| Parameter | Default | Description |
|-----------|---------|-------------|
| `schemaRegistryEnabled` | `false` | Enable Confluent Schema Registry integration. |
| `schemaRegistryUrl` | _(required)_ | Confluent Schema Registry URL. Required when `schemaRegistryEnabled` is `true`. It must start with `http://` or `https://`, in any letter case: while `schemaRegistryEnabled` and `avroValueSerialization` are on, any other URL fails the deployment. |
| `schemaRegistryCredentialAlias` | — | Credential alias for Schema Registry authentication. |
| `autoRegisterSchemas` | `false` | Automatically register schemas with Schema Registry. |
| `subjectNameStrategy` | `TopicNameStrategy` | Subject naming strategy. Only `TopicNameStrategy` (subject `<topic>-value`) is supported, its name in any letter case: with `RecordNameStrategy` or `TopicRecordNameStrategy` (UI: "RecordNameStrategy (not supported – deployment fails)", "TopicRecordNameStrategy (not supported – deployment fails)") while `schemaRegistryEnabled` and `avroValueSerialization` are on, the channel does not start, because the record name cannot be derived from the JSON input. Both values stay selectable only so that saved configurations remain valid. |
| `avroValueSerialization` | `true` | Serialize message values using Avro. Requires Schema Registry. |

For details on Avro integration, see [Avro / Schema Registry](features/avro-schema-registry.md).

### Error Handling

**Retry**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `producerRetryMaxAttempts` | `1` | Total number of send attempts, not additional attempts (UI: "Send Attempts (total, 1 = off)"). `1` keeps the previous behaviour and switches the retry off. Range 1–5. |
| `producerRetryDelaySeconds` | `2` | Constant wait between attempts in seconds. Range 1–30. Shown only when `producerRetryMaxAttempts` is not `1`. |
| `producerRetryOnlyTransientErrors` | `true` | Retry only transient (`RETRIABLE`) failures. `false` additionally retries an unusable transactional producer. |
| `producerRetryTotalBudgetSeconds` | `30` | Hard upper bound for all attempts of one message together. Must stay below the calling system's timeout; a Kafka-to-Kafka or scheduled channel has no such caller and can use a larger budget. Range 5–900. Shown only when `producerRetryMaxAttempts` is not `1`. |

Only failures that provably wrote nothing are repeated. See [Producer Retry](features/producer-retry.md)
for the decision tree, the duplicate guarantees and the interaction with `deliveryTimeoutSeconds`,
which the adapter validates at start-up.

**Diagnostics**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `diagnosticsLevel` | `STANDARD` | Diagnostic output level. `STANDARD` (default) is fully diagnostic on its own: every failure produces one structured ERROR line with the complete serialised cause chain. `FULL` adds exactly one thing, a bounded thread dump (at most 20 threads, 10 frames each, with lock owners) attached to the node-fault escalation that fires when the same fault recurs 5 times in 20 minutes. Leave this on `STANDARD` unless you are actively investigating such a fault. |
| `writeMplErrorAttachment` | `true` | Currently without effect on the receiver: a failing receiver channel fails the exchange, and CPI writes the MPL entry without a `KafkaAdapterError` attachment. The option works on the sender. |
