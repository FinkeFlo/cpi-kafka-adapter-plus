# FAQ

Frequently asked questions about the Kafka Adapter Plus.

---

## Consumer — Batch Processing & Partitions

### In what order are partitions processed?

The order is determined by **Kafka, not the adapter**. `kafkaConsumer.poll()` fetches
records from all assigned partitions in a single broker request. The Kafka client returns
partitions in the order of the fetch responses — typically ascending by partition ID in
practice, but with no strict guarantee.

The adapter groups the returned records by partition and processes them sequentially:
all batches from the first partition, then the next, and so on. The offset order
**within** a partition is always ascending and guaranteed.

### Can a partition be starved — never processed while others keep receiving records?

No. `poll()` fetches **all** assigned partitions simultaneously in every cycle. If the
MPL log shows several consecutive entries for only one partition, it simply means the
other partitions had no remaining lag at that point — they were not skipped.

### Why do I sometimes see only one partition per poll cycle in the MPL?

Partitions accumulate lag independently depending on when and how much was produced.
Once a partition is caught up, `poll()` returns no records for it. Several consecutive
poll cycles showing a single partition is normal, correct behaviour — it means the other
partitions are already up to date.

### Which headers tell me how many records are in a batch and which partition they came from?

In batch mode the adapter sets the following headers on each iFlow execution:

| Header | Description |
|---|---|
| `CpiKafkaPlusRecordCount` | Number of records in this batch |
| `CpiKafkaPlusPartition` | Partition all records in this batch came from |
| `CpiKafkaPlusFirstOffset` | Offset of the first record in the batch |
| `CpiKafkaPlusLastOffset` | Offset of the last record in the batch |
| `CpiKafkaPlusPayloadSize` | Total payload size in bytes |

See [Kafka Headers](features/kafka-headers.md) for the full list.

### Why is `CpiKafkaPlusRecordCount` sometimes lower than the configured batch size?

A batch contains all records the adapter could fetch for one partition in one poll cycle,
up to **Max Records per IFlow Run (MPL)** (`batchSize`). There are several reasons why
fewer records are returned:

| Reason | Explanation |
|---|---|
| **Partition lag** | The partition simply had fewer records available than `batchSize` at that moment. |
| **`maxPollRecords` shared across partitions** | `max.poll.records` is a cap on the **total** records per `poll()` call across all partitions. If multiple partitions have records, each partition gets only a share of that budget — so a partition with high lag may still yield fewer than `batchSize` records per cycle. |
| **`max.partition.fetch.bytes`** | Kafka limits the bytes fetched per partition per request (configurable). If individual records are large, fewer fit within the byte limit even if the lag is high. |
| **Broker-side availability** | The broker returns what is currently available within the configured `fetch.max.wait.ms`. Under low-throughput conditions a poll may return fewer records than the maximum even when lag exists. |

A consistently low `CpiKafkaPlusRecordCount` combined with high lag is a signal to review
`maxPollRecords` and `max.partition.fetch.bytes` in relation to your average record size.

---

## Consumer — Offset & Delivery

### When are offsets committed?

With the default **After Successful Processing (At-Least-Once)** (`BATCH_COMPLETE`)
strategy, offsets are committed after each batch has been processed successfully. If the
iFlow fails, the batch is delivered again on the next poll — this is the at-least-once
guarantee.

With **Auto Commit (Periodic)** (`AUTO`), Kafka commits offsets in the background on a
timer regardless of whether processing succeeded. This is not recommended for
production-critical scenarios.

### What happens when a rebalance occurs during processing?

The adapter commits pending offsets for revoked partitions inside the rebalance callback
before handing them to another consumer. Records that were already fetched but not yet
committed will be redelivered to whichever consumer takes over the partition.

---

## Consumer — Connection & Health

### The consumer leaves its group between polls — what causes this?

This is caused by `max.poll.interval.ms` being shorter than the configured
**Polling Interval (Seconds)**. The adapter automatically sets `max.poll.interval.ms`
to `pollingIntervalSeconds + 10 min` so the heartbeat thread does not send a `LeaveGroup`
between polls. If you see unexpected rebalances, verify that `pollingIntervalSeconds` is
within the supported range (1 – 21600).

---

## Producer

### Can I override the target topic or partition at runtime?

Yes. Set the following headers on the exchange before sending:

| Header | Description |
|---|---|
| `CamelKafkaTopic` | Overrides the topic configured on the endpoint |
| `kafka.PARTITION_KEY` | Target partition (numeric string) |
| `kafka.KEY` | Record key |

Both are evaluated per message. See [Kafka Headers](features/kafka-headers.md) for the full list.

### What happens if the producer batch exceeds the Kafka message size limit?

The producer will throw an exception and the iFlow execution fails. To avoid this,
tune `max.request.size` and `message.max.bytes` on the broker, or reduce the number
of records per batch on the sender side.
