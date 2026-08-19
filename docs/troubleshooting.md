# Troubleshooting

This page is written for the situation where something has already gone wrong. It describes what the
adapter writes, where to find it, and how to read it. This is the *how*; see
[ADR 0004](adrs/0004-diagnostic-logging-contract.md) for the *why*.

Two diagnostic channels are available: the **tenant trace file** (always) and the **Message
Processing Log** (per-message, when a failure occurs during an exchange). Both are described here.

## Why the adapter logs the way it does

Three properties of the SAP Cloud Integration runtime shape every logging decision in this adapter.
They are not obvious, and each one has previously caused a real incident to go undiagnosed.

**Only `ERROR` reaches the tenant trace file.** `WARN`, `INFO` and `DEBUG` are invisible in
production. In one analysed corpus of 870,613 trace lines, 32,708 came from this adapter and *all*
of them were `ERROR` — including a `LOG.warn` statement that was known to execute. The practical
consequence for an investigation: **the absence of a `WARN` in the trace proves nothing at all.**
Anything that matters when something fails is therefore logged at `ERROR`, even when a lower level
would look more tasteful.

**The trace appender discards the `Throwable`.** A conventional `LOG.error("...", exception)` keeps
the rendered message and throws the stack trace away. The adapter therefore serialises the exception
— class, message, frames, the whole cause chain and any suppressed exceptions — *into the message
text*. That is why failure lines are long.

**`#` is not a safe field delimiter.** Trace records are `#`-separated, but the thread name itself
contains a `#` (for example `... thread #429917`), so a record splits into a variable number of
fields. Parse from the end of the line, or with a regular expression — never by fixed column index.

## Error codes

Every failure line carries a stable error code in the form `KP-AREA-NNN`. Quote this code in support
tickets: it identifies the failure class precisely and survives copy-paste better than a stack trace.

| Code | Symbol | Description |
|------|-----------|-------------|
| `KP-PROD-001` | `producer.init.failed` | Producer initialisation failed |
| `KP-PROD-002` | `producer.send.timeout` | Send timed out |
| `KP-PROD-003` | `producer.send.record_too_large` | Record too large for the broker's `message.max.bytes` |
| `KP-PROD-004` | `producer.send.serialization_failed` | Serialisation failed |
| `KP-TXN-001` | `txn.fenced` | Producer fenced (another instance took over the transactional id) |
| `KP-TXN-002` | `txn.failed` | Transaction failed |
| `KP-SEC-001` | `security.authentication_failed` | Authentication failed (wrong credentials, expired token) |
| `KP-SEC-002` | `security.authorization_denied` | Authorisation denied (ACL blocks the operation) |
| `KP-CFG-001` | `config.invalid_topic` | Invalid topic (does not exist, auto-create disabled) |
| `KP-CFG-002` | `config.json_schema_validation_failed` | JSON schema validation failed |
| `KP-META-001` | `metadata.fetch_timeout` | Metadata fetch timed out |
| `KP-META-002` | `metadata.monitor_state_fault` | KAFKA-10902 monitor fault (see below) |
| `KP-DLQ-001` | `dlq.delivery_failed` | Dead-letter queue delivery failed |
| `KP-SR-001` | `schema_registry.failed` | Schema Registry call failed |
| `KP-GEN-001` | `unclassified` | No code matched. Treated as unknown rather than guessed — see *Exception classification* below |

The symbol is not the operation token at the start of the log line; the two are separate
namespaces. Grep for the code (`KP-META-002`), which appears on every line that carries it, rather
than for the symbol.
| `KP-SR-001` | `schema_registry.failed` | Schema Registry operation failed |
| `KP-GEN-001` | `unclassified` | Unclassified error |

`KP-GEN-001` means the adapter did not recognise the exception. It is not necessarily fatal — the
full cause chain in `error=` is the evidence. If the same `KP-GEN-001` recurs, report it so the
classification can be extended.

## Exception classification

Failure lines carry a `classification` field with one of four values. These drive the adapter's
automatic mitigation and tell you what to try next.

| Classification | Meaning | What to do |
|----------------|---------|------------|
| `RETRIABLE` | Transient failure (timeout, network blip). The adapter will retry automatically. | Wait. If it persists, check broker connectivity. |
| `FATAL_PRODUCER_UNUSABLE` | The producer cannot recover without being rebuilt (fenced, auth revoked, bad broker version). | The adapter rebuilds the producer automatically. Check credentials, ACLs, broker version. |
| `FATAL_DATA_ERROR` | A problem with the record itself (too large, invalid topic, serialisation failure). No producer rebuild will help. | Fix the payload or configuration. |
| `UNKNOWN_FATAL` | The adapter does not recognise this exception. | Read the full `error=` field. Report it so the classification can be improved. |

`UNKNOWN_FATAL` is deliberately conservative: the adapter does not guess. If you see it, the cause
chain is the evidence. A rebuild is not triggered automatically because it might be pointless.

## Finding the adapter's lines

Every line the adapter writes carries one marker and only one:

```
[CPI-KAFKA-PLUS-DIAG]
```

```bash
grep '\[CPI-KAFKA-PLUS-DIAG\]' trace.log
```

There is deliberately no second marker. An earlier version used a different one on four lines, which
happened to be the adapter start failures — so the single most useful grep missed exactly the lines
worth finding.

### Which node produced a line

The node address is the **second-to-last** `#`-separated field of a trace record, and the node index
is the last. This is not adapter output; the platform adds it. It is worth knowing about: in the
incident that motivated this page, every occurrence of a failure turned out to sit on **one worker
node out of seven**, which is what ruled out the broker, the topic configuration and the adapter's
own logic in a single step.

```bash
# Distribution of failures across nodes
grep '\[CPI-KAFKA-PLUS-DIAG\]' trace.log | grep -E 'producer\.batch\.record\.(send|await)' \
  | awk -F'#' '{print $(NF-1)}' | sort | uniq -c | sort -rn
```

Compare that against the distribution of *all* lines. A failure count that is concentrated on one
node while overall traffic is spread evenly is a node-local problem, not a Kafka problem.

## Reading a failure line

Failure lines are flat `key=value` sequences. A value is wrapped in single quotes if it is empty or
contains a space, an `=` or a quote; a quote inside such a value is **doubled**. So a lone `'` always
ends the value and `''` always means a literal quote — which matters here, because Kafka quotes topic
names in its own exception messages and those messages end up inside `error=`.

```
[CPI-KAFKA-PLUS-DIAG] producer.batch.record.send producerPath=SHARED phase=SYNC_SEND
  clientId=… topic=… recordIndex=0 batchSize=3 bufferedRecords=0 elapsedMs=12
  thread=… error='…' STACK …
```

### Field reference

| Field | Meaning |
|---|---|
| *(first token after the marker)* | The operation, e.g. `producer.batch.record.send`, `producer.init.failed`, `producer.send.monitorFaultRetry` |
| `phase` | `SYNC_SEND` — `send()` threw synchronously, the record never entered the accumulator. `AWAIT_FUTURE` — the record was accepted and failed later. Only the first is safe to retry |
| `producerPath` | `SHARED` (non-transactional) or `TRANSACTIONAL`. Check this before investigating anything transactional |
| `topic` | Target topic |
| `recordIndex` | Position in the batch. A failure at `0` typically means the metadata wait, not the payload |
| `batchSize` | Size of the batch |
| `elapsedMs` | Time since the batch started |
| `thread` | Worker thread. Several distinct threads failing points at a node-wide condition rather than one poisoned thread |
| `mplId`, `applicationId`, `exchangeId` | Correlation to one integration-flow message; `mplId` is what the monitor is searchable by |
| `consecutiveFailures` | Consecutive failures on this producer. Governs reconnect only, never whether something is logged |
| `classification` | One of `RETRIABLE`, `FATAL_PRODUCER_UNUSABLE`, `FATAL_DATA_ERROR`, `UNKNOWN_FATAL` (see above) |
| `errorCode` | Stable error code, e.g. `KP-PROD-002` (see above) |
| `producerRecreated`, `durationMs`, `attemptsSinceLastSuccess` | Emitted on the separate `producer.rebuild.outcome` line, not on the failure line. A rebuild is not reported inline — correlate by `clientId`, then look for `producer.rebuild.effect` with `outcome=RECOVERED` to see whether it actually helped |
| `retryOutcome`, `retryCount`, `batchRetriesLeft`, `stopReason` | Outcome of the bounded retry described below |
| `error` | Serialised exception: class, message, frames, then `CAUSED_BY` per cause level and `SUPPRESSED` where present |

### Choosing what to investigate

1. **`producerPath` first.** It costs nothing and rules out half the code. An earlier investigation
   spent its effort on the transactional path for failures that had all occurred on the shared one.
2. **`phase` next.** `SYNC_SEND` means nothing was ever handed to Kafka. `AWAIT_FUTURE` means it was.
3. **`thread` and the node field.** One thread suggests a local problem; many threads on one node
   suggest the node; all nodes suggest the broker or the configuration.
4. **`error` last.** By then you know where to read it.

## `IllegalMonitorStateException: current thread is not owner`

This one has a specific, known cause and the adapter handles it, so it is worth recognising.

It comes from [KAFKA-10902](https://issues.apache.org/jira/browse/KAFKA-10902), which is open and
unfixed. `ProducerMetadata.awaitUpdate` is `synchronized` and passes `this` to
`SystemTime.waitObject`, which enters the same monitor again and calls `Object.wait()` at monitor
recursion depth 2. Under conditions the JDK does not guarantee, the thread loses ownership and the
wait throws.

Two details make it identifiable from the message alone:

- `Object.wait()` is the **only** construct that produces the bare text `current thread is not
  owner`. `ReentrantLock` and the other `java.util.concurrent` locks throw the same exception type
  with a `null` message, so they cannot be confused with it.
- The adapter contains no `wait`, `notify` or `synchronized` block on the send path, so an occurrence
  did not originate in adapter code.

**What the adapter does about it.** The metadata for each topic is fetched once up front and
`metadata.max.idle.ms` is raised to one hour, so the vulnerable path is normally not entered at all.
The client default of five minutes discards the metadata of an idle topic, which meant a flow
producing less often than that hit the blocking metadata wait on *every* message. If the fault does
occur on a synchronous `send()`, it is retried — safely, because `KafkaProducer.doSend()` calls
`waitOnMetadata()` *before* `accumulator.append(...)`, so the record was never queued and cannot be
duplicated.

**How often it retries.** Three bounds apply and the first one reached stops it:

| Bound | Limit |
|---|---|
| Per record | 3 retries |
| Per batch | 5 retries in total, regardless of how many records the batch holds |
| Wall clock | Never past the existing batch deadline |

Worst case is five extra attempts and roughly 250 ms for a whole batch.

```bash
# Did the adapter recover from it, and how often?
grep 'producer.send.monitorFaultRetry' trace.log | grep -o 'retryOutcome=[A-Z]*' | sort | uniq -c
```

`stopReason=BATCH_BUDGET_EXHAUSTED` or a run of `retryOutcome=FAILED` means the node is not merely
glitching. Redeploying or restarting the integration flow causes the workers to be placed again, and
the node field above is the evidence to attach to a ticket.

## Node fault escalation

When the same fault (keyed by exception class and error code) recurs **5 times within 20 minutes**
on one JVM despite the adapter's mitigation, a `producer.node.fault.escalation` line is emitted.
This is the single most actionable diagnostic line the adapter can produce: it means the node is
experiencing a persistent problem that automatic recovery cannot fix.

```
[CPI-KAFKA-PLUS-DIAG] producer.node.fault.escalation faultClass=TimeoutException
  errorCode=KP-PROD-002 classification=RETRIABLE countInWindow=5 windowMinutes=20
  producerPath=SHARED topic=… thread=… advice='The same fault has recurred 5 times
  within 20 minutes on this JVM despite mitigation…'
```

**What to do:**

1. Extract the node address from the trace record's second-to-last `#`-separated field.
2. Compare it against the distribution of all failures (see the grep recipe above). If the fault is
   concentrated on one node while traffic is spread evenly, the problem is node-local, not Kafka.
3. Collect the evidence (error code, classification, node address) and open a support ticket
   requesting that the node be recycled or the integration flow redeployed.

The escalation fires once per fault identity per 20-minute window. If the fault stops and returns,
it fires again. Successful sends reset the window.

## Producer rebuild tracking

When the adapter rebuilds its producer (after a `FATAL_PRODUCER_UNUSABLE` classification or repeated
failures), two events report the outcome:

- `producer.rebuild.outcome` — emitted immediately after the rebuild attempt, reporting whether a
  new producer was successfully constructed (`producerRecreated=true/false`), how long it took
  (`durationMs`), and how many rebuild attempts have occurred since the last successful send
  (`attemptsSinceLastSuccess`).

- `producer.rebuild.effect` — emitted on the **next successful send** after a rebuild, confirming
  that the rebuild actually helped (`outcome=RECOVERED`).

If a `producer.rebuild.outcome` is followed immediately by another failure (rather than by a
`producer.rebuild.effect`), the rebuild did not help. Check the error code and classification on the
subsequent failure line.

## Dead-letter producer rebuild tracking

The dead-letter producer heals itself the same way, and the equivalent events are
`dlq.producer.rebuild.outcome` and `dlq.producer.rebuild.effect`. Read them the same way: an
`outcome` line followed by `effect outcome=RECOVERED` means the record reached the dead-letter topic
on the replacement client; `effect outcome=STILL_FAILING` means it did not, and the exception on
that line is the one to investigate.

Two fields on `dlq.send.failed` decide what to do next:

| Field | Meaning |
| --- | --- |
| `rebuildJustified` | Whether the classification says a fresh client could help. `false` for `RETRIABLE` and `FATAL_DATA_ERROR` — a record the broker rejects will be rejected by a new client too, so no rebuild is attempted |
| `rebuildEscalated` | `true` when the classification did *not* ask for a rebuild but three consecutive sends have failed, so one is attempted anyway. This is what stops a "retriable" verdict from becoming a permanent stall |
| `consecutiveFailures` | Consecutive dead-letter send failures. Any value above 1 means the partition is not advancing |
| `rebuildBackoffElapsed` | `false` means a rebuild was skipped only because one happened moments ago. A run of these is a record being redelivered, not a new fault |
| `rebuildTriggered` | The decision all of the above produce. If this stays `false` while sends keep failing, the cause is in the record or the broker, not in the client |
| `duplicateRisk` | `true` when the first attempt failed after the record had already been accepted (`phase=AWAIT_FUTURE`), so the retry may have written the record twice. Deduplicate on `CpiKafkaPlusDlqOriginalTopic` + `Partition` + `Offset` |

## Common situations

### A consumer stops advancing and no dead-letter records appear

This is the shape of a stalled consumer, and it is worth recognising because the symptom and the
cause sit far apart. A dead-letter send is what allows the offset to be committed: if the send
throws, nothing is committed, the same record is polled again on the next cycle, and every later
message is stuck behind a record that can never succeed. What you observe is a partition whose
committed offset does not move and a dead-letter topic that has stopped receiving anything — while
the integration flow keeps producing one failed message per delivery, which looks like a *processing*
problem rather than a *dead-letter* problem.

Confirm it in this order:

1. Grep the trace for `dlq.send.failed`. The line carries the original `topic`, `partition` and
   `offset`, the serialised cause, and `consequence='offset not committed, record will be
   reprocessed'`. The repeated offset in successive lines is the stalled one.
2. Compare the group's committed offset with the partition's high watermark. A constant gap that
   equals the number of deliveries since the stall began confirms it.
3. Check whether a rebuild was attempted and what it achieved, using the fields above.

Note when counting dead-letter records that the adapter partitions them by the original record key,
so a multi-partition dead-letter topic spreads them across all partitions. Reading only partition 0
will make records look lost that are not.

### Nothing from the adapter in the trace at all

Check the marker spelling first, then whether the flow ran at all. A *successful* send now produces
a periodic heartbeat line (`producer.heartbeat status=HEALTHY`) at most once every 5 minutes, so a
healthy producer is provably alive. If neither failures nor heartbeats appear, the flow did not run.

### `producer.heartbeat`

The throttled success heartbeat. Emitted at most once per 5 minutes when sends are succeeding, and
immediately on recovery after a failure. It carries `sendsSinceLastHeartbeat` so you can gauge
throughput. If the heartbeat is present but failures are absent, the producer is healthy.

### `runEmitCycle: alive`

The consumer's counterpart, and it behaves differently on purpose. Only `reason=STATE_CHANGE`
reaches the tenant trace file — it is written when the consumer's `initialized` state flips, so at
startup and whenever the consumer is torn down or rebuilt. The recurring `reason=INTERVAL` tick is
logged at INFO and is therefore visible on a local run but not on a tenant.

Do not use the absence of this line as an alarm, and do not use its presence as an all-clear. It
reports that the scheduler called `poll()`, which stays true while a partition does not advance at
all — the stall in the 1.2.7 dead-letter incident would have been accompanied by an unbroken series
of `alive` lines. To judge a consumer, use `closing consumer for reconnect`, the poll failure lines
and the committed offsets. Older adapter versions wrote the tick at ERROR, which is why they
contribute several hundred red lines per day and endpoint that mean nothing.

### `poll: closing consumer for reconnect`

Repeated poll failures crossed either the count threshold (5 consecutive) or the duration threshold
(60 s), so the consumer is closed and rebuilt on the next poll. `reason` says which threshold fired,
and both counters restart afterwards — the line therefore appears once per reconnect, not once per
failed poll, and seeing it repeat at a steady cadence means the outage is ongoing rather than that
the adapter is thrashing. The individual poll failures are logged separately with the cause.

### `poll: detected invalid OSGi bundle wiring`

This line means Kafka class loading failed with the CPI hot-update signature
`bundle wiring ... no longer valid` (typically wrapped in `NoClassDefFoundError` or
`ClassNotFoundException`). In this state the route can still tick "alive" while poll/assignment
logic is broken.

From this version onward, the adapter treats that signature as a dedicated recoverable state: it
closes the current consumer immediately and rebuilds it on the next poll cycle, rather than waiting
for the generic failure thresholds. If wiring remains stale, it additionally tries a bounded
automatic restart of the affected Camel route (`stopRoute`/`startRoute`, max 2 attempts, 15-minute
cooldown), which mirrors the manual "restart iFlow" fix path.

If the line keeps repeating even after those attempts, the tenant runtime still serves a stale class
space and needs platform-side runtime recovery.

### `adapter.mpl.unavailable`

The Message Processing Log binding could not be resolved, so MPL traces will not be written for this
endpoint. It is reported once per endpoint rather than per message. The adapter itself continues to
work; only the MPL enrichment is lost.

### `producer.init.failed`

Initialisation failed. `bootstrapServers` and `securityProtocol` are in the line, and the serialised
cause chain usually names the problem directly — a rejected credential, a TLS handshake failure, or
an unreachable broker.

### `producer.metadata.prewarm outcome=FAILED`

The up-front metadata fetch did not succeed. This is not itself a failure: the send path performs the
same fetch and will report a more precise error. Treat it as a hint that the broker was slow or
unreachable at that moment.

## Message Processing Log enrichment

When a failure occurs during an exchange, the adapter enriches the Message Processing Log with
structured information. This is available in the CPI monitor even when MPL *tracing* is disabled
(which is the default). Two channels are used, both trace-independent:

### Custom header properties

Four custom header properties are attached to the failed message's MPL entry:

| Property | Value |
|----------|-------|
| `KafkaAdapterErrorCode` | The stable error code, e.g. `KP-PROD-002` |
| `KafkaAdapterTopic` | The target topic |
| `KafkaAdapterProducerPath` | `SHARED` or `TRANSACTIONAL` |
| `KafkaAdapterRetryable` | `true` if the classification was `RETRIABLE`, otherwise `false` |

These properties are visible in the monitor's message detail view and are searchable. They do **not**
require MPL tracing to be enabled — they are attached via `addCustomHeaderProperty`, which works
regardless of trace level.

### Status event

If the adapter can obtain an MPL handle with status support (`getMessageLogWithStatus`), it fires a
`FAILED` status event with the error code and a truncated error message. This marks the message as
failed in the monitor, making failures visible at a glance without opening each message.

### Attachment with the full error block

Independently of the trace level, the adapter attaches the complete serialised error block —
exception chain, causes and frames — to the message's monitor entry under the name
`KafkaAdapterError`. This is the one channel that is not subject to the 8,000-character line limit
of the tenant trace file, so when a cause chain is deep this is where to read it in full.

A second trace-independent channel writes the same four facts as adapter attributes
(`errorCode`, `topic`, `producerPath`, `retryable`) via `putAdapterAttribute`. It is redundant on
purpose: the two APIs surface in different places in the monitor depending on tenant configuration,
and neither had ever been verified to be visible, so the adapter writes both rather than betting on
one.

## Diagnostics level

The adapter's `diagnosticsLevel` option controls the verbosity of diagnostic output. Two levels are
supported:

| Level | Description |
|-------|-------------|
| `STANDARD` (default) | All failure paths log the full serialised cause chain. Sufficient for root-causing most production incidents. |
| `FULL` | Adds one thing: a bounded thread dump attached to `producer.node.fault.escalation`, capped at 20 threads and 10 frames each, with lock owners resolved. That dump is the only evidence that can settle a monitor-ownership question, and it is the reason `FULL` exists. |

`STANDARD` is fully diagnostic on its own. Do not run at `FULL` permanently: the extra output adds
volume and latency on every send. Switch to `FULL` only when you need the additional detail, then
switch back.

See [Configuration](configuration.md) for how to set this option.
