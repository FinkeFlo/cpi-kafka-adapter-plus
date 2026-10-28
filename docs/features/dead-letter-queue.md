# Dead Letter Queue (DLQ)

The adapter supports routing failed messages to a Dead Letter Queue topic, preventing poison pills from blocking the consumer while preserving failed records for later analysis or reprocessing.

## Overview

When **Enable Dead Letter Queue** (`dlqEnabled`) is turned on, records that fail processing in the CPI IFlow are retried a configurable number of times. If all retries configured via **Retries before DLQ (additional attempts)** (`dlqMaxRetries`) are exhausted, the record is forwarded to the configured **DLQ Topic** (`dlqTopic`) instead of blocking the consumer — unless the write to the DLQ itself fails, see [When the DLQ Write Fails](#when-the-dlq-write-fails). The original record key, value, and headers are preserved, and error metadata is added as Kafka headers.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dlqEnabled` | `false` | Enable Dead Letter Queue routing for failed records |
| `dlqTopic` | _(required)_ | Kafka topic to send failed records to. Kafka takes 1 to 249 characters from `a-z`, `A-Z`, `0-9`, `.`, `_` and `-`, but not `.` or `..` alone; any other name fails the deployment. |
| `dlqMaxRetries` | `3` | Number of retries after the first failed attempt before a record is sent to the DLQ (UI: "Retries before DLQ (additional attempts)") |
| `dlqCredentialAlias` | _(empty)_ | SASL credential alias for writing to the DLQ topic, if that needs other credentials than the main connection. The DLQ topic is always on the same cluster (`bootstrapServers`). Only visible when DLQ is enabled. Leave empty to reuse the main consumer credentials. |
| `retryOnlyTransientErrors` | `true` | Only retry transient errors (timeouts, connection failures). Permanent errors are sent directly to DLQ. (UI: "Retry Transient Errors Only") |
| `retryDelaySeconds` | `0` | Initial delay between retries in seconds. Doubles after each retry (exponential backoff), capped at 300s. |

!!! note "Not to be confused with the producer retry"
    `retryOnlyTransientErrors` and `retryDelaySeconds` on this page are **sender (consumer)
    parameters**: they govern how often a record is reprocessed before it is dead-lettered, with
    exponential backoff. The receiver (producer) direction has its own, separate set —
    `producerRetryOnlyTransientErrors`, `producerRetryDelaySeconds`, `producerRetryMaxAttempts`,
    `producerRetryTotalBudgetSeconds` — with a constant delay and completely different rules. See
    [Producer Retry](producer-retry.md).

The DLQ topic must differ from every topic the channel consumes — the channel does not start
otherwise. The DLQ producer is sized for *Max Fetch Size per Partition (KB)* plus 1 MB for the error
headers, and waits at most 15 s for a missing DLQ topic. The fetch size is a soft limit on compressed
bytes, so a single record larger than that (after decompression) can still be rejected. The broker or topic must allow messages
of that size (`message.max.bytes` / `max.message.bytes`).

## How Retries Work

Retries happen **synchronously in memory** during the same poll cycle — the record is not re-read from Kafka. The consumer holds the record and passes it to the IFlow pipeline up to **Retries before DLQ (additional attempts)** (`dlqMaxRetries`) + 1 times (1 initial attempt + N retries).

```
Record consumed from Kafka
  |
  +-- Attempt 1 -> IFlow fails
  |     |
  |     +-- Is error permanent? (retryOnlyTransientErrors=true)
  |     |     Yes -> Send to DLQ immediately
  |     |     No  -> Wait (retryDelaySeconds with backoff)
  |     |
  +-- Attempt 2 -> IFlow fails -> Wait...
  +-- Attempt 3 -> IFlow fails -> Wait...
  +-- Attempt 4 -> IFlow fails
  |
  +-- All retries exhausted -> Send to DLQ topic, commit offset
```

If any attempt succeeds, the offset is committed immediately and processing continues with the next record.

A record that ends up in the DLQ still counts as one failure for **Auto-Pause on Errors**
(`autoPauseEnabled`): once, after its last attempt, while the retries before it do not count
separately. With the default **Consecutive Error Threshold** of 5, five dead-lettered records in a row
pause the consumer, although the DLQ took each of them. A successful record resets the count, and
records rejected by JSON Schema validation do not count.

If the integration flow is stopped or undeployed while an attempt runs or while the consumer waits for the next
one, the record is neither sent to the DLQ nor skipped: it stays uncommitted and is delivered again after the
restart. An attempt that fails during the stop usually fails because of the stop, not because of the record.

## Smart Retry: Error Classification

When **Retry Transient Errors Only** (`retryOnlyTransientErrors`) is enabled (default), the adapter classifies exceptions before retrying. Permanent errors are sent directly to the DLQ without further retry attempts.

### Transient Errors (retried)

These errors indicate temporary conditions that may resolve on their own:

- `ConnectException` — target system unreachable
- `SocketException` — connection reset, socket failure
- `SocketTimeoutException` — read/connect timeout
- `UnknownHostException` — DNS resolution failure
- `TimeoutException` — processing timeout
- Kafka `RetriableException` — broker-side transient errors
- A failed Schema Registry request of the adapter — no answer (connection, TLS or timeout), or the status `401`, `408`, `429` or `5xx`

### Permanent Errors (sent directly to DLQ)

Everything else, including:

- `NullPointerException` — mapping error, missing data
- `ClassCastException` — type mismatch in mapping
- `IllegalArgumentException` — invalid input
- `NumberFormatException` — data conversion failure
- `FileNotFoundException` — resource not found

The adapter walks the full exception cause chain. If a `RuntimeException` wraps a `ConnectException`, it is still classified as transient.

### DLQ Error Type Header

With **Retry Transient Errors Only** (`retryOnlyTransientErrors`) switched on, a record that failed in the IFlow reaches the DLQ with the header `CpiKafkaPlusDlqErrorType`: `PERMANENT` if the error was permanent and the remaining retries were skipped, `TRANSIENT` if all retries failed. With the switch off, the header is absent, because the adapter does not classify the error then. Records that cannot be deserialized (poison pills) always carry `DESERIALIZATION`; records that failed JSON Schema validation carry no error type. A record that the Schema Registry cannot serve a schema for, because it does not answer or answers `401`, `408`, `429` or `5xx`, is no poison pill and never reaches the DLQ: its partition waits until the registry answers again (see [Avro / Schema Registry](avro-schema-registry.md#when-the-schema-registry-cannot-serve-a-schema)). Up to 1.4.1 such records were dead-lettered as `DESERIALIZATION`.

## Retry Delay with Exponential Backoff

When `retryDelaySeconds` is set to a value greater than 0, the adapter waits between retry attempts. The delay doubles after each retry (exponential backoff), capped at 300 seconds.

**Formula:** `delay = min(retryDelaySeconds * 2^attempt, 300)`

**Example** with `retryDelaySeconds=2` and **Retries before DLQ (additional attempts)** (`dlqMaxRetries`) = 3:

| Attempt | Result | Wait |
|---------|--------|------|
| 1 (initial) | Fails | 2s |
| 2 (retry 1) | Fails | 4s |
| 3 (retry 2) | Fails | 8s |
| 4 (retry 3) | Fails | -> DLQ |
| **Total** | | **14s** |

### Retries and max.poll.interval.ms

Retries run on the consumer's poll thread. If the consumer does not call `poll()` again within `max.poll.interval.ms`, Kafka hands its partitions to another member of the group, which processes the same records again. That limit is 10 minutes with **Consumption Mode** (`consumptionMode`) *Streaming (Continuous)*, and **Polling Interval (Seconds)** (`pollingIntervalSeconds`) plus 10 minutes with *Scheduled Polling*, capped at 6 h 10 min.

So that long retry delays, a slow integration flow or many failing records cannot exceed it, the adapter starts no record, batch or retry later than halfway through that limit after a poll, for example 5 minutes in *Streaming (Continuous)*. What is left of the poll comes with the next poll. A record whose next retry would start later does not wait for it on the poll thread: its partition is rewound to the record and paused until the retry is due, while the other partitions keep flowing. The next poll of the partition runs that retry instead of starting the record over, so the record still reaches the DLQ after **Retries before DLQ (additional attempts)** (`dlqMaxRetries`) retries, only later. In batch mode that poll first sends the batch that starts with the record; if the batch fails again, the record continues with that retry.

The 14 seconds of waiting in the example above fit easily. With **Retry Delay (Seconds)** (`retryDelaySeconds`) = 60 in *Streaming (Continuous)*, the waits are 60, 120 and 240 seconds: for a record at the start of its poll whose flow fails fast, the first two retries start within the 5 minutes, and retry 3, due 7 minutes after the first attempt, runs in a later poll.

The trace shows `consumer.retry.deferred` for such a record and `consumer.poll.timeLimit` when records of a poll are left to the next one (see [Troubleshooting](../troubleshooting.md#consumerpolltimelimit-and-consumerretrydeferred)). A single integration flow call that takes longer than half of `max.poll.interval.ms` can still exceed the limit. **Retry Transient Errors Only** (`retryOnlyTransientErrors`) keeps records with permanent errors out of the retries altogether.

## Batch Mode Behavior

When using batch processing (`batchMode=true` with **JSON Array** (`JSON_ARRAY`) or **XML List** (`XML_LIST`)), the DLQ integrates with a **two-stage fallback**:

1. The batch is first processed as a whole (e.g., 5 records as one JSON array)
2. If the batch fails, the adapter **falls back to individual record processing**
3. Each record is then retried individually with the full retry logic
4. Only records that fail all individual retries are sent to the DLQ

Each record of this fallback reaches the IFlow as a batch of one record in the same envelope, with
the header `CpiKafkaPlusFallbackMode=true` (see [Kafka Headers](kafka-headers.md#batch-mode)).

This means a single bad record does not drag the entire batch into the DLQ.

A batch of **one** record is not run a second time: its failed batch run counts as the record's
first attempt. A permanent error sends it straight to the DLQ, a transient one continues with the
record's retries, which carry `CpiKafkaPlusFallbackMode=true`. Up to 1.4.2 the record ran through
the IFlow once more as a fallback before its retries started.

### Example: 5-Record Batch with One Bad Record

**Setup:** **Max Records per IFlow Run (MPL)** (`batchSize`) = 5, **Retries before DLQ (additional attempts)** (`dlqMaxRetries`) = 2, Record #3 contains invalid data.

| Step | Action | Result |
|------|--------|--------|
| 1 | Batch of 5 records sent to IFlow as JSON array | IFlow fails (Record #3 causes error) |
| 2 | Record 1 processed individually | Attempt 1 succeeds, offset committed |
| 3 | Record 2 processed individually | Attempt 1 succeeds, offset committed |
| 4 | Record 3 processed individually | Attempt 1 fails, Attempt 2 fails, Attempt 3 fails → **sent to DLQ** |
| 5 | Record 4 processed individually | Attempt 1 succeeds, offset committed |
| 6 | Record 5 processed individually | Attempt 1 succeeds, offset committed |

**Result:** Only Record 3 ends up in the DLQ. Records 1, 2, 4, and 5 are processed successfully — albeit as individual exchanges rather than as a batch.

!!! note
    If the batch failure is caused by a general error (e.g., CPI runtime unavailable) rather than a single bad record, all records will fail individually and all will be routed to the DLQ.

## JSON Schema Validation and DLQ

Records that fail **JSON Schema Validation** (`jsonSchemaValidation`) are sent to the DLQ **immediately without retries** (`CpiKafkaPlusDlqRetryCount` = `0`, no `CpiKafkaPlusDlqErrorType`), since schema validation errors are deterministic and retrying would produce the same result.

**Without DLQ:** Records that fail **JSON Schema Validation** (`jsonSchemaValidation`) are **silently discarded** — the offset is committed so the record is not reprocessed, but the record is not forwarded to the IFlow. A WARN-level log entry is written for each discarded record; WARN does not reach the tenant trace in production, so enable **Report Validation Failures in CPI Monitoring** (`jsonSchemaReportError`) to see these records as failed messages.

## When the DLQ Write Fails

A record is only committed once it has been processed or written to the DLQ. If the DLQ write
itself fails — the DLQ topic does not exist, the credentials lack `WRITE` on it, the broker is
unreachable — the record is **not** committed and nothing after it in the same partition is
processed. The partition is rewound to the record and retried after a delay: 1 second, doubling
with every further failure at the same offset up to 5 minutes (or **Retry Delay** if that is
longer than 1 second). Other partitions keep flowing.

The trace shows `dlq.send.failed … consequence='offset not committed, record will be retried'`
followed by `consumer.partition.retry` with the `offset` and `retryInMs`. If an earlier record of
the same poll is still open as well, `offset` is that record, where the partition resumes, and
`blockedAt` is the record whose DLQ write failed. Fix the cause and the partition continues on its
own with the next retry.

This applies to every way a record reaches the DLQ: failed IFlow processing, failed
deserialization and failed JSON Schema validation.

The failed write is reported in the CPI monitor too, in one MPL entry per failure with the code
`KP-DLQ-001`. The status text leads with the failed write and its root cause, followed by the
original failure, for example:

```
KP-DLQ-001: dead-letter write to orders-dlq failed: InvalidRecordException: Compacted topic cannot accept message without key in topic partition orders-dlq-0. (original: KP-FLOW-001: <flow error>)
```

The `KafkaAdapterError` attachment holds both stack traces (the original failure and, under
`--- DLQ Error ---`, the failed write) and, for well-known causes, a `hint`:

| Root cause | Hint |
|------------|------|
| `InvalidRecordException` on a compacted topic | The DLQ topic is compacted and the record has no key: use `cleanup.policy=delete` |
| `InvalidRecordException`, other | The broker rejected the record: check the DLQ topic configuration |
| `AuthenticationException` | Authentication of the DLQ producer failed |
| `TopicAuthorizationException` / `AuthorizationException` | The credentials lack `WRITE` (or `DESCRIBE`) on the DLQ topic |
| `UnknownTopicOrPartitionException`, metadata timeout | The DLQ topic does not exist or is not visible to these credentials |
| `RecordTooLargeException` | The record exceeds `max.message.bytes` of the DLQ topic or broker |

A record the broker rejects (`InvalidRecordException`) is a data error: the DLQ producer is not
rebuilt for it, since a fresh producer would be rejected the same way.

The custom header properties `KafkaAdapterOriginalErrorCode`, `KafkaAdapterDlqOutcome`
(`MOVED`/`SEND_FAILED`) and `KafkaAdapterDlqTopic` make these messages searchable, and every MPL
entry with a DLQ outcome names the source topic as sender (`SAP_Sender`) and the DLQ topic as
receiver (`SAP_Receiver`).

## Error Handling Without DLQ

When DLQ is **not** enabled, **Error Handling** (`errorHandling`) decides what happens to a record
whose processing fails. The two options match the *Error Handling* setting of SAP's Kafka sender
adapter:

| Option | Behavior |
|--------|----------|
| **Retry Failed Message** (`RETRY`, default) | The record is retried at the same offset until it succeeds. Its partition waits meanwhile: nothing after the record is processed or committed. The retries back off — **Retry Delay** (at least 1 second), doubling up to 5 minutes — and every attempt writes a failed message processing log. Other partitions keep flowing, unless Auto-Pause is enabled (see below). Nothing is lost. |
| **Skip Failed Message** (`SKIP`) | The failed message processing log is written, the record (or batch) is skipped and its offset is committed. The record is lost (at-most-once). |

In batch mode the unit that fails is the batch: with *Retry Failed Message* the whole batch is
retried, including the records in it that would have succeeded on their own, and a single bad
record holds back its batch and everything after it in the partition. Only a DLQ isolates the bad
record — with a DLQ a failed batch is retried record by record and only the failing records are
dead-lettered.

This applies to failed IFlow processing (batch and non-batch), to batches that cannot be formatted
and to records that cannot be deserialized (Avro). It does not apply to:

| Error Type | Behavior |
|------------|----------|
| JSON Schema validation failure | Record is **discarded**, offset committed — a record that violates the schema never becomes valid by retrying. A WARN log is written, which does not reach the tenant trace in production, and the record is lost; `jsonSchemaReportError` adds a failed MPL entry for it. |
| A failure inside the adapter itself (not in the IFlow) | The record is always retried, whatever the setting. |
| A Schema Registry that cannot serve the record's schema (no answer, `401`, `408`, `429`, `5xx`) | The record is always retried, whatever the setting and also with a DLQ: the record is not at fault. See [Avro / Schema Registry](avro-schema-registry.md#when-the-schema-registry-cannot-serve-a-schema). |

> **Retry Failed Message and poison pills:** a record that can never be processed blocks its
> partition until you fix the IFlow, the backend or the data. Use a Dead Letter Queue for any flow
> where a single bad record must not hold up the others.
>
> **Retry Failed Message and Auto-Pause:** every retry counts as a failure for **Auto-Pause on
> Errors**. That is what you want during a backend outage: the consumer stops hammering the backend.
> But a single record that can never succeed then pauses the **whole** consumer, all partitions,
> again and again with a growing cooldown. Auto-Pause is the tool for backend outages, a DLQ the
> tool for poison pills.

## DLQ Record Headers

Each record sent to the DLQ includes the original headers plus DLQ metadata. The exact metadata depends on how the record reached the DLQ.

### Normal Processing Failures

Records that fail during IFlow processing, and records that fail JSON Schema validation, include these headers:

| Header | Description |
|--------|-------------|
| `CpiKafkaPlusDlqError` | Error message or exception class name |
| `CpiKafkaPlusDlqOriginalTopic` | Source topic the record was consumed from |
| `CpiKafkaPlusDlqOriginalPartition` | Original partition number |
| `CpiKafkaPlusDlqOriginalOffset` | Original offset within the partition |
| `CpiKafkaPlusDlqTimestamp` | ISO 8601 timestamp of when the record was sent to DLQ |
| `CpiKafkaPlusDlqRetryCount` | Number of retries before DLQ routing (attempts − 1), including a retry that failed with a permanent error; `0` for JSON Schema violations |
| `CpiKafkaPlusDlqErrorType` | `PERMANENT` or `TRANSIENT`, only with **Retry Transient Errors Only** (`retryOnlyTransientErrors`) switched on; absent otherwise and for JSON Schema violations (see [DLQ Error Type Header](#dlq-error-type-header)) |

### Deserialization / Poison-Pill Failures

Records that cannot be deserialized include these headers:

| Header | Description |
|--------|-------------|
| `CpiKafkaPlusDlqError` | Error message or exception class name |
| `CpiKafkaPlusDlqErrorClass` | Exception class name |
| `CpiKafkaPlusDlqCauseClass` | Cause exception class name |
| `CpiKafkaPlusDlqCauseMessage` | Cause exception message |
| `CpiKafkaPlusDlqOriginalTopic` | Source topic the record was consumed from |
| `CpiKafkaPlusDlqOriginalPartition` | Original partition number |
| `CpiKafkaPlusDlqOriginalOffset` | Original offset within the partition |
| `CpiKafkaPlusDlqTimestamp` | ISO 8601 timestamp of when the record was sent to DLQ |
| `CpiKafkaPlusDlqErrorType` | `DESERIALIZATION` |

Such a record is not retried, so it carries no `CpiKafkaPlusDlqRetryCount`.

These headers allow consumers of the DLQ topic to trace the origin of each failed record and understand the failure context.

## Recommendations

- **Start with the default** of **Retries before DLQ (additional attempts)** (`dlqMaxRetries`) = 3 — this handles transient errors well without adding too much latency
- **Monitor the DLQ topic** to detect recurring failures and fix root causes
- **Use the DLQ headers** (`CpiKafkaPlusDlqError`, `CpiKafkaPlusDlqOriginalTopic`) to build automated alerting or reprocessing pipelines
- **Combine with Offset Commit Strategy = After Successful Processing (At-Least-Once)** (`BATCH_COMPLETE`) to ensure at-least-once delivery — records are only committed after successful processing or DLQ routing
- **Enable Retry Transient Errors Only** (`retryOnlyTransientErrors`) (default) to avoid wasting retries on mapping errors, NPEs, or other permanent failures
- **Set `retryDelaySeconds=2`** when using DLQ with transient-error-prone backends to give downstream systems time to recover
