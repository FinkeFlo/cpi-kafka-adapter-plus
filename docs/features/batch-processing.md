# Batch Processing & Drain Mode

## How the consumer works

The Kafka adapter polls Kafka at regular intervals defined by **Polling Interval (Seconds)** (`pollingIntervalSeconds`) and processes records as batches through the CPI iFlow. The diagram and calculations below apply when **Consumption Mode** (`consumptionMode`) is set to `SCHEDULED`.

!!! note "STREAMING mode"
    When **Consumption Mode** (`consumptionMode`) is set to `STREAMING`, **Polling Interval (Seconds)** and **Drain Backlog** are not used. The consumer polls continuously (greedy scheduling) with no timer delay between polls as long as records keep arriving. See [Consumption Mode](#consumption-mode) below.

```mermaid
flowchart TD
    A["Camel timer fires<br>(every pollingIntervalSeconds)"] --> B["kafkaConsumer.poll()<br>fetches up to maxPollRecords records"]
    B --> C["Records are split into batches<br>(batchSize)"]
    C --> D["Each batch -> 1 iFlow execution<br>(1 MPL entry)"]
    D --> E["Offsets are committed<br>(BATCH_COMPLETE)"]
    E --> F["Timer waits pollingIntervalSeconds"]
    F --> A
```

## The three configuration groups

### 1. Polling - how often records are fetched

| Parameter | Default | Description |
|---|---|---|
| **Polling Interval (Seconds)** | 5 | Wait time between poll cycles |

### 2. Kafka poll - how many records are fetched per poll

| Parameter | Default | Description |
|---|---|---|
| **Max Poll Records** | 500 | Maximum number of records per `kafkaConsumer.poll()` call |

### 3. Batch - how records are passed to the iFlow

| Parameter | Default | Description |
|---|---|---|
| **Batch Mode** | true | Multiple records per iFlow execution |
| **Max Records per IFlow Run (MPL)** | 100 | Maximum records per batch (= per iFlow execution) |
| **Batch Output Format** | JSON_ARRAY | Format: `JSON_ARRAY` or `XML_LIST`. |

> **Poll Timeout (ms)** (default 5000) controls how long `kafkaConsumer.poll()` blocks when the topic is empty. It is located in the **Consumption → Polling** tab, not in the Batch/Message Handling group. See [Poll Timeout — how poll() blocking works](#poll-timeout--how-poll-blocking-works) below.

## How the parameters interact

### Example 1: Default configuration

| Parameter | Value |
|---|---|
| Polling Interval | 5s |
| Max Poll Records | 500 |
| Max Records per IFlow Run (MPL) | 100 |

Per poll cycle:
- `kafkaConsumer.poll()` fetches up to 500 records
- 500 / 100 batch size = 5 iFlow executions, 5 MPL entries

Throughput: 500 records / 5s = 100 msg/s

### Example 2: High throughput

| Parameter | Value |
|---|---|
| Polling Interval | 1s |
| Max Poll Records | 2000 |
| Max Records per IFlow Run (MPL) | 500 |

Per poll cycle: 2000 records, 4 iFlow executions

Throughput: 2000 / 1s = 2000 msg/s

### Example 3: Infrequent polling (hourly)

| Parameter | Value |
|---|---|
| Polling Interval | 3600s (1 hour) |
| Max Poll Records | 500 |
| Max Records per IFlow Run (MPL) | 500 |
| Drain Backlog | OFF |

!!! warning
    At 10 msg/s, 36,000 messages arrive per hour. Only 500 are fetched per poll, so the backlog keeps growing.

    Solution: enable Drain Backlog (see below).

---

## Consumption Mode

The **Consumption Mode** (`consumptionMode`) parameter switches between two scheduling strategies.

### SCHEDULED (default)

The consumer fires on a fixed timer defined by **Polling Interval (Seconds)** (`pollingIntervalSeconds`). After each poll-and-process cycle the adapter waits the configured interval before polling again. This is the classic behaviour described in the rest of this page.

### STREAMING

In `STREAMING` mode the consumer uses Camel greedy scheduling:

- As long as `poll()` returns records, the next poll fires **immediately** with no delay.
- When the topic is idle, `poll()` blocks for up to the **Poll Timeout** (`batchTimeout`, default 5000 ms) and the scheduler then waits a fixed **1 second** before retrying — the idle cadence is therefore `batchTimeout` + 1 s. This costs no latency: `poll()` returns as soon as records arrive, so a record landing in an idle topic is picked up immediately.

This mirrors the standard SAP Kafka adapter's continuous behaviour and eliminates the latency of scheduled polling that can be as high as **Polling Interval (Seconds)** (`pollingIntervalSeconds`).

**Configuration notes for STREAMING:**
- **Polling Interval (Seconds)** (`pollingIntervalSeconds`) — ignored (greedy scheduling replaces the interval)
- **Drain Backlog** (`drainEnabled`) — ignored (continuous polling already drains the backlog naturally)
- Both fields are greyed out in the CPI adapter UI when `STREAMING` is selected.
- Freely combinable with batch mode (`batchMode`, **Max Records per IFlow Run (MPL)** / `batchSize`, **Batch Output Format** / `batchOutputFormat`).

---

## Drain Backlog

!!! note "STREAMING mode"
    **Drain Backlog** has no effect when **Consumption Mode** (`consumptionMode`) is set to `STREAMING`. The greedy scheduler already fetches records as fast as they arrive; there is no **Polling Interval (Seconds)** delay to bridge.

### The problem: backlog during traffic spikes

Without drain mode, each poll cycle fetches at most the configured **Max Poll Records** (`maxPollRecords`). If more messages arrive than can be processed per cycle, a backlog grows:

| Metric | Value |
|---|---|
| Production | 50 msg/s |
| Consumption rate | 500 records / 30s interval = ~17 msg/s |
| Difference | 33 msg/s backlog growth |
| After 1 hour | 118,800 unprocessed messages |

### The solution: enable Drain Backlog

With `Drain Backlog = ON`, the consumer fetches all available records in a single timer fire, not just the configured **Max Poll Records** (`maxPollRecords`):

```mermaid
flowchart TD
    A["Timer fires"] --> B["poll() + process batch\n(up to maxPollRecords records)"]
    B --> C{"records returned == 0\nor below minBacklogToDrain?"}
    C -- "no" --> B
    C -- "yes" --> D["Timer waits pollingIntervalSeconds"]
    D --> A
```

Example with **Max Poll Records** = 500 and **Min Backlog to Drain** = 100: iteration 1 returns 500 records (continue), iteration 2 returns 500 records (continue), iteration 3 returns 220 records (continue), iteration 4 returns 30 records (`30 < 100` → stop).

Drain continues until the topic is almost empty. Stop conditions:

1. **Topic is empty** - `poll()` returns 0 records
2. **Min Backlog threshold** - when `minBacklogToDrain > 0`, drain stops once a poll returns fewer records than the threshold
3. **iFlow is stopped** - undeploy/restart in CPI

### Min Backlog to Drain — when drain should stop

With a constant message stream, drain mode could keep running because new messages keep arriving. `Min Backlog to Drain` defines: **stop drain when fewer than X records are returned.**

| Value | Behavior |
|---|---|
| **0** (Default) | Drain until the topic is empty |
| **100** | Drain stops when `poll()` returns fewer than 100 records |
| **500** | Drain stops when `poll()` returns fewer than 500 records |

Example with `Min Backlog to Drain = 100`, `Max Poll Records = 500`:

Topic has 1,200 messages, 5 msg/s continue to arrive:

| Iteration | `poll()` returns | Threshold check | Action |
|---|---|---|---|
| 1 | 500 | 500 ≥ 100 | continue |
| 2 | 500 | 500 ≥ 100 | continue |
| 3 | 220 | 220 ≥ 100 | continue |
| 4 | 30 | 30 < 100 | **STOP** |

!!! note
    30 records remain in the topic and are fetched in the next interval.
    Result: ~1,250 records processed, topic almost empty, no endless drain.

Without a threshold (default 0), drain would continue in iteration 4 and also fetch the 30 remaining records. With a threshold > 0, drain stops earlier and leaves small remaining batches for the next regular poll cycle.

=== "Start"

    | Metric | Value |
    |---|---|
    | Committed offset | 10,000 |
    | Log-end offset (broker) | 11,200 |
    | **Lag** | **1,200** |

    Topic has a backlog of 1,200 messages. `Drain Backlog = ON`, `Min Backlog to Drain = 100`.

=== "Iteration 1"

    | Metric | Value |
    |---|---|
    | `poll()` returns | 500 records (offsets 10,000–10,499) |
    | Committed offset (after) | 10,500 |
    | Log-end offset (broker) | 11,215 *(+15 new arrivals)* |
    | **Lag** | **715** |
    | Check | 500 ≥ 100 → continue |

=== "Iteration 2"

    | Metric | Value |
    |---|---|
    | `poll()` returns | 500 records (offsets 10,500–10,999) |
    | Committed offset (after) | 11,000 |
    | Log-end offset (broker) | 11,230 *(+15 new arrivals)* |
    | **Lag** | **230** |
    | Check | 500 ≥ 100 → continue |

=== "Iteration 3"

    | Metric | Value |
    |---|---|
    | `poll()` returns | 220 records (offsets 11,000–11,219) |
    | Committed offset (after) | 11,220 |
    | Log-end offset (broker) | 11,250 *(+20 new arrivals)* |
    | **Lag** | **30** |
    | Check | 220 ≥ 100 → continue |

=== "Iteration 4 (stop)"

    | Metric | Value |
    |---|---|
    | `poll()` returns | 30 records (offsets 11,220–11,249) |
    | Committed offset (after) | 11,250 |
    | Log-end offset (broker) | ~11,250 |
    | **Lag** | **~0** |
    | Check | 30 < 100 → **STOP** |

    Drain stops. The topic is almost empty; any further trickle of new messages is picked up on the next regular poll cycle after **Polling Interval (Seconds)** (`pollingIntervalSeconds`).

### Reduced max.poll.interval.ms risk

Kafka removes a consumer from the group if it does not call `poll()` within `max.poll.interval.ms`. The adapter significantly reduces this risk by polling regularly (including keep-alive polls between emit cycles) and by deriving `max.poll.interval.ms` from **Polling Interval (Seconds)** (`pollingIntervalSeconds`) with an additional processing buffer (capped at 6 h 10 min).

### Limitation: Drain + Auto Commit (Periodic)

Drain Backlog is not compatible with `Offset Commit Strategy = Auto Commit`. The adapter rejects this combination during startup because auto commit could commit offsets before records are processed, which can cause data loss.

---

## Example scenarios with calculations

### Scenario A: Normal operation (5 msg/s)

| Parameter | Value |
|---|---|
| Polling Interval | 30s |
| Max Poll Records | 500 |
| Max Records per IFlow Run (MPL) | 100 |
| Drain Backlog | OFF |

| Metric | Value |
|---|---|
| Arrive per 30s | 5 msg/s x 30s = 150 records |
| Fetched per poll | 150 (< 500 Max Poll Records) |
| iFlow executions | 150 / 100 = 2 (batches with 100 and 50 records) |
| MPL entries/minute | ~4 |
| Backlog | 0 (consumption rate > production rate) |

!!! note
    Drain is not needed; standard polling is sufficient.

### Scenario B: Traffic spike (200 msg/s, 2 hours)

| Parameter | Value |
|---|---|
| Polling Interval | 30s |
| Max Poll Records | 500 |
| Max Records per IFlow Run (MPL) | 500 |
| Drain Backlog | ON |

**Without drain:**

| Metric | Value |
|---|---|
| Consumption rate | 500 records / 30s = ~17 msg/s |
| Production | 200 msg/s |
| Backlog growth | 183 msg/s |
| After 2h | 1,317,600 unprocessed messages |

**With drain:**

| Metric | Value |
|---|---|
| Arrive per 30s | 200 x 30 = 6,000 records |
| Drain fetches | all 6,000 in one timer fire: 12 iterations x 500 records x ~50ms = ~0.6 seconds |
| Pause after drain | 30s |
| Effective consumption rate | 6,000 / 30s = 200 msg/s |
| Backlog | stable at 0 |
| MPL entries | 12 per cycle, ~24/minute |

### Scenario C: 8-hour polling with 1 million messages/day

| Parameter | Value |
|---|---|
| Polling Interval | 28800s (8 hours) |
| Max Poll Records | 500 |
| Max Records per IFlow Run (MPL) | 500 |
| Drain Backlog | ON |

| Metric | Value |
|---|---|
| Arrive per 8h | ~333,000 records |
| Drain duration | 333,000 / 500 = 666 iterations x ~50ms = ~33 seconds |
| iFlow executions | 666 |
| MPL entries | 666 |

!!! note
    During the 33s drain, ~380 new records arrive → they are fetched in the last iterations → topic is empty after ~33s → next drain after 8 hours.

### Scenario D: Testing/debugging (small batches)

| Parameter | Value |
|---|---|
| Polling Interval | 60s |
| Max Poll Records | 20 |
| Max Records per IFlow Run (MPL) | 5 |
| Drain Backlog | ON |

100 records in the topic:

| Iteration | `poll()` returns | Action |
|---|---|---|
| 1 | 20 records | 4 batches of 5 → 4 iFlow executions |
| 2 | 20 records | 4 iFlow executions |
| ... | ... | ... |
| 5 | 20 records | 4 iFlow executions |
| 6 | 0 records | **STOP** |

!!! note
    Total: 20 iFlow executions, < 1 second

---

## Choosing values for your scenario

There is no single correct configuration — the right values depend on your message
rate, latency requirements, and how bursty the traffic is. Use the scenarios above and
the rules of thumb below to derive values for your own case instead of copying fixed
numbers.

### Rule of thumb

**Drain OFF:** when the production rate is low enough that **Max Poll Records** (`maxPollRecords`) per cycle is sufficient:
`production rate x Polling Interval (Seconds) < Max Poll Records`

**Drain ON:** when traffic spikes are possible or the polling interval is long (> 60s). Drain is harmless at low volume because it stops after the first iteration when records are below **Max Poll Records** (`maxPollRecords`).

**Min Backlog to Drain:** set to > 0 when a constant message stream exists and drain should not run endlessly. A typical value is equal to or smaller than **Max Poll Records** (`maxPollRecords`), for example 100 with **Max Poll Records** = 500.

### Throughput formula

| Mode | Throughput |
|---|---|
| Without drain | `Max Poll Records / Polling Interval (Seconds)` msg/s |
| With drain | Unbounded (limited by iFlow processing time + network) |

---

## Batch Output Formats

### JSON Array (`JSON_ARRAY`) (default)

**JSON Array** (`JSON_ARRAY`) produces a nested JSON object, not a raw array. The extra `record` level keeps the root compatible with CPI JSON-to-XML conversion.

```json
{
  "kafkaRecords": {
    "record": [
      {
        "key": "k1",
        "value": {"field": "v1"},
        "topic": "my-topic",
        "partition": 0,
        "offset": 100,
        "timestamp": 1700000000000
      },
      {
        "key": "k2",
        "value": {"field": "v2"},
        "topic": "my-topic",
        "partition": 0,
        "offset": 101,
        "timestamp": 1700000000001
      }
    ]
  }
}
```

### XML List (`XML_LIST`)

**XML List** (`XML_LIST`) wraps records in `<kafkaRecords count="N"><record>...</record>...</kafkaRecords>`. Each record contains `key`, `value`, `topic`, `partition`, `offset`, and `timestamp`.

Values that look like XML are automatically detected and embedded as parsed child elements, marked with `format="xml"`. Non-XML, null, or empty values use text/CDATA content with `format="text"`. This detection is always on and is not configurable.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<kafkaRecords count="3">
  <record>
    <key>k1</key>
    <value format="xml"><root><field>v1</field></root></value>
    <topic>my-topic</topic>
    <partition>0</partition>
    <offset>100</offset>
    <timestamp>1700000000000</timestamp>
  </record>
  <record>
    <key>k2</key>
    <value format="text"><![CDATA[{"field":"v2"}]]></value>
    <topic>my-topic</topic>
    <partition>0</partition>
    <offset>101</offset>
    <timestamp>1700000000001</timestamp>
  </record>
  <record>
    <key>k3</key>
    <value format="text"></value>
    <topic>my-topic</topic>
    <partition>0</partition>
    <offset>102</offset>
    <timestamp>1700000000002</timestamp>
  </record>
</kafkaRecords>
```

**CPI iFlow pattern:**

```mermaid
flowchart TD
    A["Kafka Adapter (XML_LIST)"] --> B["Iterating Splitter\n(XPath: /kafkaRecords/record)"]
    B --> C{"Router / branch on\nvalue/@format"}
    C -- "format=\"xml\"" --> D["mapping can access value/root/..."]
    C -- "format=\"text\"" --> E["extract string(value),\nthen parse if needed"]
```

Alternative filter: `/kafkaRecords/record[value/@format='xml']` to process only XML records.

!!! warning "XML well-formedness"
    XML values that look like XML must be well-formed. The adapter only uses a lightweight check before embedding; malformed XML content can make the batch XML invalid.

---

## Poll Timeout — how poll() blocking works

`Poll Timeout (ms)` is not the time the adapter waits to collect records. It is the maximum time `kafkaConsumer.poll()` blocks waiting for the Kafka broker:

- Topic has records: `poll()` returns immediately (milliseconds)
- Topic is empty: `poll()` waits up to Poll Timeout ms, then returns 0 records

The parameter only affects idle detection for an empty topic.

---

## Offset Commit

With **Offset Commit Strategy** (`commitStrategy`) set to **After Successful Processing (At-Least-Once)** (`BATCH_COMPLETE`) (recommended), offsets are committed only after each batch has been processed successfully. If an error occurs, records from the failed batch are delivered again on the next poll (at-least-once semantics).

With **Offset Commit Strategy** (`commitStrategy`) set to **Auto Commit (Periodic)** (`AUTO`), Kafka commits periodically in the background, independently of whether processing succeeded. This is not recommended for production-critical scenarios.
