# Configuration Reference

All adapter parameters are configured on the `CpiKafkaPlusEndpoint`. Parameters are organized by category.

## Connection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bootstrapServers` | _(required)_ | Kafka bootstrap servers, comma-separated. |
| `topic` | _(required)_ | Kafka topic or comma-separated topics to consume from or produce to. |
| `groupId` | — | Consumer group ID. |

## Security

| Parameter | Default | Description |
|-----------|---------|-------------|
| `securityProtocol` | `SASL_SSL` | Security protocol: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`. |
| `saslMechanism` | `PLAIN` | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. |
| `credentialAlias` | — | Credential alias for SASL username/password from CPI Secure Store. |

For detailed security setup, see [Authentication](security/authentication.md).

## Processing

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pollingIntervalSeconds` | `5` | Time in seconds between poll cycles. Range: 1 to 21600. |
| `autoOffsetReset` | `latest` | Auto offset reset: `earliest` or `latest`. |
| `maxPollRecords` | `500` | Maximum records to poll per request. |
| `commitStrategy` | `BATCH_COMPLETE` | Offset commit strategy: `AUTO`, `BATCH_COMPLETE`. |
| `drainEnabled` | `false` | Poll repeatedly until the topic is empty. |
| `minBacklogToDrain` | `0` | Minimum records in an extra drain poll required to continue draining; `0` drains until empty. |
| `jsonSchemaValidation` | `false` | Enable JSON Schema validation of incoming messages. |
| `jsonSchema` | — | Inline JSON Schema for message validation. |
| `jsonSchemaReportError` | `false` | Report JSON Schema validation failures as errors in CPI monitoring; otherwise invalid messages are dropped. |

## Consumer

| Parameter | Default | Description |
|-----------|---------|-------------|
| `maxPartitionFetchSizeKb` | `1024` | Maximum data returned by the broker per partition per poll, in KB. |

## Batch

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batchMode` | `true` | Enable batch mode with multiple records per exchange. |
| `batchSize` | `100` | Maximum records per batch. |
| `batchTimeout` | `5000` | Maximum wait time in milliseconds to fill a batch. |
| `batchOutputFormat` | `JSON_ARRAY` | Batch output format: `JSON_ARRAY`, `XML_LIST`, `SPLIT_EXCHANGES`. |
| `embedXmlValues` | `false` | In `XML_LIST` output, embed XML values as child elements instead of text. |

In `XML_LIST` mode, each `<value>` element carries a `format` attribute (`"xml"` for directly embedded XML, `"text"` for text/CDATA content). See [Batch Processing](features/batch-processing.md) for details.

## Producer

| Parameter | Default | Description |
|-----------|---------|-------------|
| `producerBatchMode` | `NONE` | Batch send mode: `NONE`, `JSON_ARRAY`, `XML_LIST`. |
| `acks` | `all` | Producer acknowledgments: `all`, `1`, `0`. |
| `compressionType` | `none` | Compression type: `none`, `gzip`, `lz4`, `zstd`. |
| `maxRequestSizeKb` | `1024` | Maximum request size in KB. |
| `producerBatchSizeKb` | `249` | Producer batch size in KB. |
| `bufferMemoryKb` | `32768` | Total memory for producer buffering in KB. |
| `enableIdempotence` | `true` | Enable idempotent producer. |
| `deliveryTimeoutSeconds` | `120` | Maximum delivery time in seconds, including retries. |

## Error Handling

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dlqEnabled` | `false` | Enable Dead Letter Queue routing for failed messages. |
| `dlqTopic` | — | Topic name for the Dead Letter Queue. |
| `dlqMaxRetries` | `3` | Maximum processing retries before routing to the DLQ. |
| `dlqCredentialAlias` | — | SASL credential alias for the DLQ Kafka cluster, if different from the main connection. |
| `retryOnlyTransientErrors` | `true` | Retry only transient errors; send permanent errors directly to the DLQ. |
| `retryDelaySeconds` | `0` | Initial retry delay in seconds with exponential backoff capped at 300 seconds. |
| `autoPauseEnabled` | `false` | Automatically pause the consumer after consecutive processing errors. |
| `autoPauseErrorThreshold` | `5` | Consecutive processing errors required to activate auto-pause. |
| `autoPauseCooldownSeconds` | `60` | Initial auto-pause duration in seconds; doubles after subsequent failures, capped at 900 seconds. |

For details on DLQ and retry behavior, see [Dead Letter Queue](features/dead-letter-queue.md).

## Avro / Schema Registry

| Parameter | Default | Description |
|-----------|---------|-------------|
| `schemaRegistryEnabled` | `false` | Enable Confluent Schema Registry integration. |
| `autoRegisterSchemas` | `false` | Automatically register schemas with Schema Registry. |
| `subjectNameStrategy` | `TopicNameStrategy` | Subject naming strategy. `TopicNameStrategy` is the supported strategy for serialization. |
| `schemaRegistryUrl` | — | Confluent Schema Registry URL. |
| `schemaRegistryCredentialAlias` | — | Credential alias for Schema Registry authentication. |
| `avroOutputFormat` | `JSON` | Avro output format: `JSON`, `XML`. |
| `avroValueDeserialization` | `true` | Deserialize message values using Avro. Requires Schema Registry. |
| `avroValueSerialization` | `true` | Serialize message values using Avro. Requires Schema Registry. |

For details on Avro integration, see [Avro / Schema Registry](features/avro-schema-registry.md).
