# Kafka Headers

This page documents all message headers set or consumed by the adapter.

---

## Consumer Headers (set by the adapter on incoming messages)

When the adapter delivers a Kafka record to an iFlow, it sets the following Camel/CPI message headers.

### Single-Record Mode

| Header | Type | Description |
|---|---|---|
| `SAP_Sender` | `String` | Topic name of the consumed record |
| `CpiKafkaPlusTopic` | `String` | Topic name |
| `CpiKafkaPlusPartition` | `Integer` | Partition the record was read from |
| `CpiKafkaPlusOffset` | `Long` | Offset of the record |
| `CpiKafkaPlusTimestamp` | `Long` | Record timestamp (epoch ms) |
| `CpiKafkaPlusKey` | `String` | Record key (decoded as UTF-8, `null` if no key) |
| `CpiKafkaPlusConsumerGroup` | `String` | Consumer group ID |
| `CpiKafkaPlusCommitStrategy` | `String` | Commit strategy configured on the endpoint |
| `CpiKafkaPlusPayloadSize` | `Integer` | Serialized payload size in bytes |
| `kafka.header.<name>` | `String` | Every Kafka record header is forwarded as a Camel header with this prefix, e.g. `kafka.header.traceparent` |

### Batch Mode

With **Batch Mode** (`batchMode=true`) the adapter delivers up to `batchSize` records of one partition from one poll as one message, in the envelope of `batchOutputFormat`, and sets these headers on it:

| Header | Type | Description |
|---|---|---|
| `SAP_Sender` | `String` | Topic name |
| `CpiKafkaPlusTopic` | `String` | Topic the batch was read from — a batch never spans topics, also on a channel subscribed to several |
| `CpiKafkaPlusRecordCount` | `Integer` | Number of records in the batch |
| `CpiKafkaPlusPayloadSize` | `Integer` | Total payload size in bytes |
| `CpiKafkaPlusBatchOutputFormat` | `String` | Batch output format as configured: `JSON_ARRAY` or `XML_LIST` |
| `CpiKafkaPlusFirstOffset` | `Long` | Offset of the first record in the batch |
| `CpiKafkaPlusLastOffset` | `Long` | Offset of the last record in the batch |
| `CpiKafkaPlusPartition` | `Integer` | Partition (from the first record) |
| `CpiKafkaPlusConsumerGroup` | `String` | Consumer group ID |
| `CpiKafkaPlusCommitStrategy` | `String` | Commit strategy |
| `CpiKafkaPlusDlqCount` | `Integer` | Number of records routed to the DLQ *(only when DLQ is enabled)* |
| `CpiKafkaPlusSchemaValidationFailures` | `Integer` | Number of schema validation failures *(only when JSON Schema validation is enabled)* |

With a DLQ enabled, a batch that fails is processed again record by record (see
[Dead Letter Queue](dead-letter-queue.md#batch-mode-behavior)). Each of these messages is a batch of
one record in the same envelope, with the headers above (`CpiKafkaPlusRecordCount` = 1,
`CpiKafkaPlusDlqCount` and `CpiKafkaPlusSchemaValidationFailures` = 0) and two more:

| Header | Type | Description |
|---|---|---|
| `CpiKafkaPlusFallbackMode` | `Boolean` | `true`: the record belongs to a failed batch and is processed on its own |
| `CpiKafkaPlusKey` | `String` | Record key (decoded as UTF-8, `null` if no key) |

---

## Producer Headers (read by the adapter from outgoing messages)

Set these headers on the exchange **before** sending to control the producer behaviour.

| Header | Type | Description |
|---|---|---|
| `kafka.KEY` | `String` | Record key to use. If absent, the record is sent without a key. |
| `kafka.PARTITION_KEY` | `String` | Target partition (numeric string). If absent, Kafka's partitioner decides. |
| `kafka.OVERRIDE_TIMESTAMP` | `Long` | Record timestamp to use (epoch ms). If absent, the Kafka client uses the current time when the record is sent (`CreateTime`); only a topic configured with `LogAppendTime` gets the broker's time instead. |
| `CamelKafkaTopic` | `String` | Overrides the topic configured on the endpoint for this single message. A batch receiver's own `CamelKafkaTopic` response header is not treated as an override by a later receiver that has its own topic (see [Producer Batch](producer-batch.md#headers)). |

Any additional headers whose names match the `allowedHeaders` pattern configured on the endpoint are forwarded as **Kafka record headers** on the outgoing message.

### Producer Response Headers (set by the adapter after a successful send)

| Header | Type | Description |
|---|---|---|
| `SAP_Receiver` | `String` | Topic the record was written to |
| `CpiKafkaPlusTopic` | `String` | Topic the record was written to |
| `CpiKafkaPlusPartition` | `Integer` | Partition the record was assigned to |
| `CpiKafkaPlusOffset` | `Long` | Offset assigned to the record |
| `CpiKafkaPlusTimestamp` | `Long` | Timestamp of the written record (epoch ms): `kafka.OVERRIDE_TIMESTAMP` or the send time, or the broker's append time on a topic with `LogAppendTime` |
| `CpiKafkaPlusStatus` | `String` | `OK` on success |

---

## Dead Letter Queue (DLQ) Record Headers

When a record is routed to the DLQ topic the adapter writes these **Kafka record headers** onto the DLQ message (not Camel headers):

| Kafka Header | Description |
|---|---|
| `CpiKafkaPlusDlqError` | Error message, or the exception class name if there is no message |
| `CpiKafkaPlusDlqErrorClass` | Exception class name *(deserialization errors only)* |
| `CpiKafkaPlusDlqCauseClass` | Root-cause exception class *(deserialization errors only)* |
| `CpiKafkaPlusDlqCauseMessage` | Root-cause message *(deserialization errors only)* |
| `CpiKafkaPlusDlqErrorType` | `DESERIALIZATION` for poison-pill records. For a record that failed in the IFlow, `PERMANENT` (the error was permanent, so the remaining retries were skipped) or `TRANSIENT` (all retries failed), but only with **Retry Transient Errors Only** (`retryOnlyTransientErrors`) switched on; absent otherwise and for records that failed JSON Schema validation |
| `CpiKafkaPlusDlqOriginalTopic` | Topic the failed record was originally consumed from |
| `CpiKafkaPlusDlqOriginalPartition` | Partition of the original record |
| `CpiKafkaPlusDlqOriginalOffset` | Offset of the original record |
| `CpiKafkaPlusDlqTimestamp` | ISO-8601 timestamp when the record was sent to the DLQ |
| `CpiKafkaPlusDlqRetryCount` | Number of retries before routing to the DLQ (attempts − 1), including a retry that failed with a permanent error; `0` for records that failed JSON Schema validation; absent for deserialization errors |

> See [Dead Letter Queue](dead-letter-queue.md) for the full DLQ configuration.
