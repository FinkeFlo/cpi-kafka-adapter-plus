# Troubleshooting

This page is written for the situation where something has already gone wrong and the only evidence
available is the tenant trace file. It describes what the adapter writes, where to find it, and how
to read it.

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
grep '\[CPI-KAFKA-PLUS-DIAG\]' trace.log | grep 'producer.batch.send' \
  | awk -F'#' '{print $(NF-1)}' | sort | uniq -c | sort -rn
```

Compare that against the distribution of *all* lines. A failure count that is concentrated on one
node while overall traffic is spread evenly is a node-local problem, not a Kafka problem.

## Reading a failure line

Failure lines are flat `key=value` sequences. Values containing spaces are wrapped in single quotes.

```
[CPI-KAFKA-PLUS-DIAG] producer.batch.send producerPath=SHARED consecutiveFailures=1
  fatalClassification=false reconnectTriggered=false thread=… topic=… batchMode=JSON
  recordCount=3 mplId=… exchangeId=… error='…'
```

### Field reference

| Field | Meaning |
|---|---|
| *(first token after the marker)* | The operation, e.g. `producer.batch.send`, `producer.init.failed`, `producer.send.monitorFaultRetry` |
| `phase` | `SYNC_SEND` — `send()` threw synchronously, the record never entered the accumulator. `AWAIT_FUTURE` — the record was accepted and failed later. Only the first is safe to retry |
| `producerPath` | `SHARED` (non-transactional) or `TRANSACTIONAL`. Check this before investigating anything transactional |
| `topic` | Target topic |
| `recordIndex` | Position in the batch. A failure at `0` typically means the metadata wait, not the payload |
| `batchSize`, `recordCount` | Size of the batch |
| `elapsedMs` | Time since the batch started |
| `thread` | Worker thread. Several distinct threads failing points at a node-wide condition rather than one poisoned thread |
| `mplId`, `applicationId`, `exchangeId` | Correlation to one integration-flow message; `mplId` is what the monitor is searchable by |
| `consecutiveFailures` | Consecutive failures on this producer. Governs reconnect only, never whether something is logged |
| `fatalClassification` | Whether the cause was classified as fatal |
| `reconnectTriggered` | Whether the producer was rebuilt as a result |
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

## Common situations

### Nothing from the adapter in the trace at all

Check the marker spelling first, then whether the flow ran at all. Note that a *successful* send is
not logged at `ERROR`, so an absence of lines is normal for a healthy flow.

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
