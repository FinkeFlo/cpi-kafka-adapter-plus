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

When `batchOutputFormat` is configured the adapter accumulates multiple records and sets these headers on the batch message:

| Header | Type | Description |
|---|---|---|
| `SAP_Sender` | `String` | Topic name |
| `CpiKafkaPlusTopic` | `String` | Topic name |
| `CpiKafkaPlusRecordCount` | `Integer` | Number of records in the batch |
| `CpiKafkaPlusPayloadSize` | `Integer` | Total payload size in bytes |
| `CpiKafkaPlusBatchOutputFormat` | `String` | Batch output format (`JSON_ARRAY`, `XML`, …) |
| `CpiKafkaPlusFirstOffset` | `Long` | Offset of the first record in the batch |
| `CpiKafkaPlusLastOffset` | `Long` | Offset of the last record in the batch |
| `CpiKafkaPlusPartition` | `Integer` | Partition (from the first record) |
| `CpiKafkaPlusConsumerGroup` | `String` | Consumer group ID |
| `CpiKafkaPlusCommitStrategy` | `String` | Commit strategy |
| `CpiKafkaPlusDlqCount` | `Integer` | Number of records routed to the DLQ *(only when DLQ is enabled)* |
| `CpiKafkaPlusSchemaValidationFailures` | `Integer` | Number of schema validation failures *(only when JSON Schema validation is enabled)* |

---

## Producer Headers (read by the adapter from outgoing messages)

Set these headers on the exchange **before** sending to control the producer behaviour.

| Header | Type | Description |
|---|---|---|
| `kafka.KEY` | `String` | Record key to use. If absent, the record is sent without a key. |
| `kafka.PARTITION_KEY` | `String` | Target partition (numeric string). If absent, Kafka's partitioner decides. |
| `kafka.OVERRIDE_TIMESTAMP` | `Long` | Record timestamp to use (epoch ms). If absent, the broker assigns the timestamp. |
| `CamelKafkaTopic` | `String` | Overrides the topic configured on the endpoint for this single message. |

Any additional headers whose names match the `allowedHeaders` pattern configured on the endpoint are forwarded as **Kafka record headers** on the outgoing message.

### Producer Response Headers (set by the adapter after a successful send)

| Header | Type | Description |
|---|---|---|
| `SAP_Receiver` | `String` | Topic the record was written to |
| `CpiKafkaPlusTopic` | `String` | Topic the record was written to |
| `CpiKafkaPlusPartition` | `Integer` | Partition the record was assigned to |
| `CpiKafkaPlusOffset` | `Long` | Offset assigned to the record |
| `CpiKafkaPlusTimestamp` | `Long` | Timestamp assigned by the broker (epoch ms) |
| `CpiKafkaPlusStatus` | `String` | `OK` on success |

---

## Dead Letter Queue (DLQ) Record Headers

When a record is routed to the DLQ topic the adapter writes these **Kafka record headers** onto the DLQ message (not Camel headers):

| Kafka Header | Description |
|---|---|
| `CpiKafkaPlusDlqError` | Error message |
| `CpiKafkaPlusDlqErrorClass` | Exception class name |
| `CpiKafkaPlusDlqCauseClass` | Root-cause exception class (deserialization errors) |
| `CpiKafkaPlusDlqCauseMessage` | Root-cause message (deserialization errors) |
| `CpiKafkaPlusDlqErrorType` | `DESERIALIZATION` for poison-pill records; absent for processing errors |
| `CpiKafkaPlusDlqOriginalTopic` | Topic the failed record was originally consumed from |
| `CpiKafkaPlusDlqOriginalPartition` | Partition of the original record |
| `CpiKafkaPlusDlqOriginalOffset` | Offset of the original record |
| `CpiKafkaPlusDlqTimestamp` | ISO-8601 timestamp when the record was sent to the DLQ |
| `CpiKafkaPlusDlqRetryCount` | Number of processing attempts before routing to the DLQ |

> See [Dead Letter Queue](dead-letter-queue.md) for the full DLQ configuration.
