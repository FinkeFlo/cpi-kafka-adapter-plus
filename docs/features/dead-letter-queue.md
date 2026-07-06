# Dead Letter Queue (DLQ)

The adapter supports routing failed messages to a Dead Letter Queue topic, preventing poison pills from blocking the consumer while preserving failed records for later analysis or reprocessing.

## Overview

When DLQ is enabled, records that fail processing in the CPI IFlow are retried a configurable number of times. If all retries are exhausted, the record is forwarded to a dedicated DLQ topic instead of blocking the consumer. The original record key, value, and headers are preserved, and error metadata is added as Kafka headers.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dlqEnabled` | `false` | Enable Dead Letter Queue routing for failed records |
| `dlqTopic` | _(required)_ | Kafka topic to send failed records to |
| `dlqMaxRetries` | `3` | Number of retry attempts before a record is sent to the DLQ |
| `dlqCredentialAlias` | _(empty)_ | SASL credential alias for the DLQ Kafka cluster. Only visible when DLQ is enabled. Leave empty to reuse the main consumer credentials. |
| `retryOnlyTransientErrors` | `true` | Only retry transient errors (timeouts, connection failures). Permanent errors are sent directly to DLQ. |
| `retryDelaySeconds` | `0` | Initial delay between retries in seconds. Doubles after each retry (exponential backoff), capped at 300s. |

## How Retries Work

Retries happen **synchronously in memory** during the same poll cycle — the record is not re-read from Kafka. The consumer holds the record and passes it to the IFlow pipeline up to `dlqMaxRetries + 1` times (1 initial attempt + N retries).

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

When `retryOnlyTransientErrors` is enabled (default), the adapter classifies exceptions before retrying. Permanent errors are sent directly to the DLQ without further retry attempts.

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

**Example** with `retryDelaySeconds=2` and `dlqMaxRetries=3`:

| Attempt | Result | Wait |
|---------|--------|------|
| 1 (initial) | Fails | 2s |
| 2 (retry 1) | Fails | 4s |
| 3 (retry 2) | Fails | 8s |
| 4 (retry 3) | Fails | -> DLQ |
| **Total** | | **14s** |

!!! warning "max.poll.interval.ms"
    The consumer's `max.poll.interval.ms` scales with the polling interval — it is `pollingIntervalSeconds` plus a 10-minute processing buffer (about 10 minutes at the default 5-second interval), capped at 6 h 10 min. If the total backoff time across all records in a single poll exceeds this limit, Kafka triggers a rebalance. Keep `dlqMaxRetries` low (1-3) and `maxPollRecords` moderate when using retry delays. Combining with `retryOnlyTransientErrors=true` minimizes the number of records entering the retry loop.

## Batch Mode Behavior

When using batch processing (`batchMode=true` with `JSON_ARRAY` or `XML_LIST`), the DLQ integrates with a **two-stage fallback**:

1. The batch is first processed as a whole (e.g., 5 records as one JSON array)
2. If the batch fails, the adapter **falls back to individual record processing**
3. Each record is then retried individually with the full retry logic
4. Only records that fail all individual retries are sent to the DLQ

This means a single bad record does not drag the entire batch into the DLQ.

### Example: 5-Record Batch with One Bad Record

**Setup:** `batchSize=5`, `dlqMaxRetries=2`, Record #3 contains invalid data.

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

Records that fail JSON Schema validation are sent to the DLQ **immediately without retries** (`retryCount=0`), since schema validation errors are deterministic and retrying would produce the same result.

**Without DLQ:** Records that fail JSON Schema validation are **silently discarded** — the offset is committed so the record is not reprocessed, but the record is not forwarded to the IFlow. A WARN-level log entry is written for each discarded record.

## Error Handling Without DLQ

When DLQ is **not** enabled, the adapter handles failures differently depending on the error type:

| Error Type | Behavior |
|------------|----------|
| JSON Schema validation failure | Record is **discarded**, offset committed. A WARN log is written but the record is lost. |
| IFlow processing failure (batch) | Offsets are **not** committed. The records will be re-delivered on the next poll cycle (at-least-once). |
| IFlow processing failure (non-batch) | Offsets are **not** committed. The record will be re-delivered on the next poll cycle (at-least-once). |

> **Important:** Without DLQ, a persistently failing record (poison pill) will block the consumer indefinitely in non-batch mode, as it will be retried on every poll cycle. Enabling DLQ is strongly recommended for production use to prevent this scenario.

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

- **Start with the default** of `dlqMaxRetries=3` — this handles transient errors well without adding too much latency
- **Monitor the DLQ topic** to detect recurring failures and fix root causes
- **Use the DLQ headers** (`CpiKafkaPlusDlqError`, `CpiKafkaPlusDlqOriginalTopic`) to build automated alerting or reprocessing pipelines
- **Combine with `BATCH_COMPLETE` commit strategy** to ensure at-least-once delivery — records are only committed after successful processing or DLQ routing
- **Enable `retryOnlyTransientErrors`** (default) to avoid wasting retries on mapping errors, NPEs, or other permanent failures
- **Set `retryDelaySeconds=2`** when using DLQ with transient-error-prone backends to give downstream systems time to recover
