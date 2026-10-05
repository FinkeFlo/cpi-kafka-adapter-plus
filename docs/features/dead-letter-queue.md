# Dead Letter Queue (DLQ)

The adapter supports routing failed messages to a Dead Letter Queue topic, preventing poison pills from blocking the consumer while preserving failed records for later analysis or reprocessing.

## Overview

When **Enable Dead Letter Queue** (`dlqEnabled`) is turned on, records that fail processing in the CPI IFlow are retried a configurable number of times. If all retries configured via **Max Retries before DLQ** (`dlqMaxRetries`) are exhausted, the record is forwarded to the configured **DLQ Topic** (`dlqTopic`) instead of blocking the consumer — unless the write to the DLQ itself fails, see [When the DLQ Write Fails](#when-the-dlq-write-fails). The original record key, value, and headers are preserved, and error metadata is added as Kafka headers.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dlqEnabled` | `false` | Enable Dead Letter Queue routing for failed records |
| `dlqTopic` | _(required)_ | Kafka topic to send failed records to |
| `dlqMaxRetries` | `3` | Number of retry attempts before a record is sent to the DLQ |
| `dlqCredentialAlias` | _(empty)_ | SASL credential alias for the DLQ Kafka cluster. Only visible when DLQ is enabled. Leave empty to reuse the main consumer credentials. |
| `retryOnlyTransientErrors` | `true` | Only retry transient errors (timeouts, connection failures). Permanent errors are sent directly to DLQ. |
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

Retries happen **synchronously in memory** during the same poll cycle — the record is not re-read from Kafka. The consumer holds the record and passes it to the IFlow pipeline up to **Max Retries before DLQ** (`dlqMaxRetries`) + 1 times (1 initial attempt + N retries).

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

## Smart Retry: Error Classification

When **Only Retry Transient Errors** (`retryOnlyTransientErrors`) is enabled (default), the adapter classifies exceptions before retrying. Permanent errors are sent directly to the DLQ without further retry attempts.

### Transient Errors (retried)

These errors indicate temporary conditions that may resolve on their own:

- `ConnectException` — target system unreachable
- `SocketException` — connection reset, socket failure
- `SocketTimeoutException` — read/connect timeout
- `UnknownHostException` — DNS resolution failure
- `TimeoutException` — processing timeout
- Kafka `RetriableException` — broker-side transient errors

### Permanent Errors (sent directly to DLQ)

Everything else, including:

- `NullPointerException` — mapping error, missing data
- `ClassCastException` — type mismatch in mapping
- `IllegalArgumentException` — invalid input
- `NumberFormatException` — data conversion failure
- `FileNotFoundException` — resource not found

The adapter walks the full exception cause chain. If a `RuntimeException` wraps a `ConnectException`, it is still classified as transient.

### DLQ Error Type Header

DLQ records include a `CpiKafkaPlusDlqErrorType` header. Normal processing failures use `PERMANENT` or `TRANSIENT`; deserialization poison pills use `DESERIALIZATION`.

## Retry Delay with Exponential Backoff

When `retryDelaySeconds` is set to a value greater than 0, the adapter waits between retry attempts. The delay doubles after each retry (exponential backoff), capped at 300 seconds.

**Formula:** `delay = min(retryDelaySeconds * 2^attempt, 300)`

**Example** with `retryDelaySeconds=2` and **Max Retries before DLQ** (`dlqMaxRetries`) = 3:

| Attempt | Result | Wait |
|---------|--------|------|
| 1 (initial) | Fails | 2s |
| 2 (retry 1) | Fails | 4s |
| 3 (retry 2) | Fails | 8s |
| 4 (retry 3) | Fails | -> DLQ |
| **Total** | | **14s** |

!!! warning "max.poll.interval.ms"
    The consumer's `max.poll.interval.ms` scales with **Polling Interval (Seconds)** (`pollingIntervalSeconds`) — it is that interval plus a 10-minute processing buffer (about 10 minutes at the default 5-second interval), capped at 6 h 10 min. If the total backoff time across all records in a single poll exceeds this limit, Kafka triggers a rebalance. Keep **Max Retries before DLQ** (`dlqMaxRetries`) low (1-3) and **Max Poll Records** (`maxPollRecords`) moderate when using retry delays. Combining with **Only Retry Transient Errors** (`retryOnlyTransientErrors`) minimizes the number of records entering the retry loop.

## Batch Mode Behavior

When using batch processing (`batchMode=true` with **JSON Array** (`JSON_ARRAY`) or **XML List** (`XML_LIST`)), the DLQ integrates with a **two-stage fallback**:

1. The batch is first processed as a whole (e.g., 5 records as one JSON array)
2. If the batch fails, the adapter **falls back to individual record processing**
3. Each record is then retried individually with the full retry logic
4. Only records that fail all individual retries are sent to the DLQ

This means a single bad record does not drag the entire batch into the DLQ.

### Example: 5-Record Batch with One Bad Record

**Setup:** **Max Records per IFlow Run (MPL)** (`batchSize`) = 5, **Max Retries before DLQ** (`dlqMaxRetries`) = 2, Record #3 contains invalid data.

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

Records that fail **JSON Schema Validation** (`jsonSchemaValidation`) are sent to the DLQ **immediately without retries** (`retryCount=0`), since schema validation errors are deterministic and retrying would produce the same result.

**Without DLQ:** Records that fail **JSON Schema Validation** (`jsonSchemaValidation`) are **silently discarded** — the offset is committed so the record is not reprocessed, but the record is not forwarded to the IFlow. A WARN-level log entry is written for each discarded record; WARN does not reach the tenant trace in production, so enable **Report Validation Failures in CPI Monitoring** (`jsonSchemaReportError`) to see these records as failed messages.

## When the DLQ Write Fails

A record is only committed once it has been processed or written to the DLQ. If the DLQ write
itself fails — the DLQ topic does not exist, the credentials lack `WRITE` on it, the broker is
unreachable — the record is **not** committed and nothing after it in the same partition is
processed. The partition is rewound to the record and retried after a delay: 1 second, doubling
with every further failure at the same offset up to 5 minutes (or **Retry Delay** if that is
longer than 1 second). Other partitions keep flowing.

The trace shows `dlq.send.failed … consequence='offset not committed, record will be retried'`
followed by `consumer.partition.retry` with the `offset` and `retryInMs`. Fix the cause and the
partition continues on its own with the next retry.

This applies to every way a record reaches the DLQ: failed IFlow processing, failed
deserialization and failed JSON Schema validation.

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

Records that fail during IFlow processing include these headers:

| Header | Description |
|--------|-------------|
| `CpiKafkaPlusDlqError` | Error message or exception class name |
| `CpiKafkaPlusDlqOriginalTopic` | Source topic the record was consumed from |
| `CpiKafkaPlusDlqOriginalPartition` | Original partition number |
| `CpiKafkaPlusDlqOriginalOffset` | Original offset within the partition |
| `CpiKafkaPlusDlqTimestamp` | ISO 8601 timestamp of when the record was sent to DLQ |
| `CpiKafkaPlusDlqRetryCount` | Number of retries attempted before DLQ routing |
| `CpiKafkaPlusDlqErrorType` | `PERMANENT` or `TRANSIENT` — error classification |

### Deserialization / Poison-Pill Failures

Records that cannot be deserialized include these headers:

| Header | Description |
|--------|-------------|
| `CpiKafkaPlusDlqError` | Error message |
| `CpiKafkaPlusDlqErrorClass` | Exception class name |
| `CpiKafkaPlusDlqCauseClass` | Cause exception class name |
| `CpiKafkaPlusDlqCauseMessage` | Cause exception message |
| `CpiKafkaPlusDlqOriginalTopic` | Source topic the record was consumed from |
| `CpiKafkaPlusDlqOriginalPartition` | Original partition number |
| `CpiKafkaPlusDlqOriginalOffset` | Original offset within the partition |
| `CpiKafkaPlusDlqTimestamp` | ISO 8601 timestamp of when the record was sent to DLQ |
| `CpiKafkaPlusDlqErrorType` | `DESERIALIZATION` |

These headers allow consumers of the DLQ topic to trace the origin of each failed record and understand the failure context.

## Recommendations

- **Start with the default** of **Max Retries before DLQ** (`dlqMaxRetries`) = 3 — this handles transient errors well without adding too much latency
- **Monitor the DLQ topic** to detect recurring failures and fix root causes
- **Use the DLQ headers** (`CpiKafkaPlusDlqError`, `CpiKafkaPlusDlqOriginalTopic`) to build automated alerting or reprocessing pipelines
- **Combine with Offset Commit Strategy = After Successful Processing (At-Least-Once)** (`BATCH_COMPLETE`) to ensure at-least-once delivery — records are only committed after successful processing or DLQ routing
- **Enable Only Retry Transient Errors** (`retryOnlyTransientErrors`) (default) to avoid wasting retries on mapping errors, NPEs, or other permanent failures
- **Set `retryDelaySeconds=2`** when using DLQ with transient-error-prone backends to give downstream systems time to recover
