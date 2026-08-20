# Producer Retry

An outer retry for the receiver (producer) direction: when a send fails, the adapter can build a
brand-new producer and try again.

It exists because Kafka's own retry cannot help in the case that motivated it. `retries` and
`delivery.timeout.ms` only repeat *inside* one producer instance, and after a
`NetworkException: Disconnected from node 3` a transactional producer is left in a state where no
further call on it can succeed. Only a throw-away producer recovers from that.

The feature is **off by default** (`producerRetryMaxAttempts=1`). It lengthens how long a
transaction slot and an HTTP worker thread are held, and it changes the timing of existing
integration flows, so it is opt-in.

## The rule that governs everything: no duplicates

A retry is only permitted where the previous attempt provably wrote **nothing**.

| Path | Retried? | Why |
|---|---|---|
| Transactional, failure **before** the commit | :material-check: yes | The aborted transaction is never visible to `read_committed` consumers. The successor producer uses the same `transactional.id`, fences the old epoch and has the broker clean up. |
| Transactional, failure **during** the commit | :material-close: no | The outcome is unknowable. The commit may have succeeded broker-side with only the response lost, so a retry would commit the batch a second time. |
| Non-transactional **single** message | :material-check: yes | One record, one acknowledgement. Requires `enableIdempotence=true`, and the shared producer is reused so the broker can still deduplicate on `(PID, sequence)`. |
| Non-transactional **batch** | :material-close: no | Records `0..k-1` may already be committed when `k` fails. Resending the batch would duplicate them, and a new producer would get a new PID, disabling broker-side deduplication. |

This is not a judgement call. The Kafka client documents both clean-up mechanisms the transactional
case relies on, and both carry the same exemption for a commit that has already begun:

* `KafkaProducer.close(Duration)`: "It will also abort the ongoing transaction **if it's not already
  completing**."
* `KafkaProducer.initTransactions()`: "If the previous instance had failed with a transaction in
  progress, **it will be aborted**. If the last transaction had begun completion, but not yet
  finished, **this method awaits its completion**."

So a failure inside the commit is not a residual risk — it is documented behaviour that the batch
gets committed. That is why the adapter tracks the transaction phase explicitly and refuses to
retry from `COMMIT` onwards, with `stopReason=COMMIT_OUTCOME_UNKNOWN`.

If `producerRetryMaxAttempts > 1` is configured on a non-transactional batch channel, the adapter
writes a warning at start-up saying the setting has no effect there. Enable transactions to use the
retry for batches.

## Parameters

| Parameter | Default | Range | Meaning |
|---|---|---|---|
| `producerRetryMaxAttempts` | `1` | 1–5 | **Total** attempts, not additional ones. `1` switches the feature off. |
| `producerRetryDelaySeconds` | `2` | 1–30 | Constant wait between attempts. |
| `producerRetryOnlyTransientErrors` | `true` | — | `true`: only `RETRIABLE`. `false`: also an unusable transactional producer. |
| `producerRetryTotalBudgetSeconds` | `30` | 5–300 | Hard bound for all attempts of one message together. |

These are **separate** from the sender-side `retryDelaySeconds` / `retryOnlyTransientErrors`, which
belong to the consumer's dead-letter path and use exponential backoff. One option meaning two
different things depending on channel direction would be a support trap.

The delay is constant, not exponential, on purpose: the Kafka client has already exhausted an
exponential backoff before giving up. A second exponential wait on top only blocks the worker
thread. And it has a floor of one second, because three immediate retries finish inside a second
and hit the same broker node that just disappeared.

Data errors (`RecordTooLargeException`, serialization failures, an invalid topic) and unclassified
errors are **never** retried, whatever the configuration says. A record that is too large will be
exactly as too large on the next attempt.

### One exception: the KAFKA-10902 monitor fault

`IllegalMonitorStateException: current thread is not owner` is retried even under
`producerRetryOnlyTransientErrors=true`, although it matches no Kafka exception hierarchy and is
therefore classified `UNKNOWN_FATAL`. It is a transient JVM-level monitor state rather than a broker
verdict — a production trace showed it arriving in bursts on individual worker nodes while the same
broker served every other channel on the tenant — and the classifier simply has no category for a
defect in the client itself.

Retrying it cannot duplicate: in the transactional path the phase rule has already stopped
everything from the commit onwards, so the failed attempt's transaction is aborted and never visible
to `read_committed` consumers; in the single path idempotence is a precondition, so the broker
deduplicates on `(PID, sequence)`. The exemption requires a Kafka frame in the stack trace, so the
same exception raised by application code gets no special treatment.

This is a second line of defence. The primary mitigation is the metadata pre-warm and the raised
`metadata.max.idle.ms` that keep the client out of the defective code path, plus the bounded inner
retry described in [Troubleshooting](../troubleshooting.md).

### `producerRetryOnlyTransientErrors=false`

`ProducerFencedException` and friends mean "this producer instance is finished, build a new one" —
which is exactly what the transactional retry loop does anyway. So a retry would be technically
sensible.

It is still not the default, because a fencing error usually means a *different* instance has taken
over the same `transactional.id`: two integration flows sharing a `transactionalIdPrefix`, or a
deployment overlap. Retrying then becomes a contest for the `transactional.id` that both sides lose.
Only switch this off when you can show the prefix is used exclusively by this channel.

On the non-transactional single path an unusable producer is never retried regardless of this
setting: that path reuses the shared producer, and a broken shared producer belongs in the existing
rebuild path.

## Sizing the budget

The adapter runs inside the request thread of a synchronous HTTP call. The binding constraint is
therefore not the transaction slot limit but the caller's patience.

**One attempt costs roughly four times `deliveryTimeoutSeconds` on the transactional path**, not
once. `delivery.timeout.ms` only bounds the wait for acknowledgements; `initTransactions()`, the
metadata block inside `send()` and `commitTransaction()` are each bounded by `max.block.ms`, which
the adapter sets to `min(30s, deliveryTimeoutSeconds)`.

```
blockSeconds = min(30, deliveryTimeoutSeconds)
perAttempt   = 3 x blockSeconds + deliveryTimeoutSeconds + 5s (producer close)
worstCase    = maxAttempts x perAttempt + (maxAttempts - 1) x producerRetryDelaySeconds
```

If `worstCase` exceeds `producerRetryTotalBudgetSeconds`, the channel **fails to start** with a
message naming both numbers. A retry configuration that could never reach its second attempt is a
promise to operations that the adapter cannot keep, and a warning would only be discovered during
the outage it was configured for.

| `deliveryTimeoutSeconds` | `producerRetryMaxAttempts` | Worst case | Fits in 30 s? |
|---|---|---|---|
| 120 (default) | 2 | ~970 s | no — start fails |
| 8 | 2 | ~76 s | no — start fails |
| 3 | 2 | ~36 s | no — just over |
| **2** | **2** | **~28 s** | yes |
| 2 | 3 | ~43 s | no |

**Recommended starting point:**

```
producerRetryMaxAttempts=2
producerRetryDelaySeconds=2
deliveryTimeoutSeconds=2
producerRetryTotalBudgetSeconds=30
```

On a transactional channel the adapter derives `transaction.timeout.ms` from
`deliveryTimeoutSeconds` (delivery timeout plus up to 30 s of commit headroom, never below the
client default of 60 s), so the broker keeps the transaction open for at least as long as the client
waits for acknowledgements. Up to and including 1.3.1 the option was left at its 60 s default and
any `deliveryTimeoutSeconds >= 60` was rejected instead — which made the shipped default of 120 s
undeployable for exactly the channels the retry was built for. The remaining bound is the broker's
`transaction.max.timeout.ms` (15 minutes by default), so `deliveryTimeoutSeconds` may not exceed
870 s on a transactional channel.

!!! warning "The budget must stay below the caller's timeout"
    If the calling system times out while the adapter is still retrying, it will consider the call
    failed and may resend the request — while the retry succeeds in the background. The result is
    two business messages in the topic. **No exactly-once mechanism inside the adapter can prevent
    this**, because the duplication happens outside it. Keep
    `producerRetryTotalBudgetSeconds` **plus the processing time of the rest of the integration
    flow** below the caller's timeout. The 30 s default is chosen to sit under the 60 s timeout
    common in clients and gateways.

!!! note "Trade-off of a low `deliveryTimeoutSeconds`"
    Shortening it also shortens Kafka's *internal* retry, which absorbs leader elections and short
    broker restarts. You are trading internal staying power for external attempts with a fresh
    producer. For a synchronous HTTP caller that trade is right — the caller will not wait minutes
    anyway. For asynchronous scenarios it is not, which is another reason the feature is off by
    default.

## Effect on transaction slots and worker threads

The retry loop runs **inside** the transaction slot it acquired, because every attempt must use the
same `transactional.id`. During a broker outage all sending threads therefore hold their slot for
the whole retry duration, and the adapter blocks more broadly than it does today. With
`maxConcurrentTransactions=1` a single retry blocks every other send on the node; the adapter warns
about that combination at start-up. Size `maxConcurrentTransactions` accordingly.

The same applies one level up: the retry blocks the HTTP worker thread, not just the slot. For
callers that cannot tolerate the delay, the right answer is a decoupling buffer (a JMS queue in
front of the Kafka call), not a longer retry budget.

## What you see when it happens

All retry events are written at **ERROR** with the single marker `[CPI-KAFKA-PLUS-DIAG]`. That is
not a severity claim — the SAP CPI tenant trace file receives only ERROR, so anything lower would be
invisible in production. They are bounded (at most 5 per message, and capped by the budget) and only
occur during a real fault.

| Event | When | Key fields |
|---|---|---|
| `producer.retry.attempt` | before each repeated attempt | `attempt`, `maxAttempts`, `txnPhase`, `classification`, `delaySeconds`, `remainingBudgetMs` |
| `producer.retry.effect` | success with `attempt > 1` | `attempts`, `totalElapsedMs`, `recovered=true` |
| `producer.retry.exhausted` | all attempts used up | `attempts`, `stopReason`, `txnPhase`, `totalElapsedMs`, `recovered=false` |
| `producer.retry.skipped` | failure was not retry-eligible | `stopReason`, `classification`, `txnPhase` |

Nothing is written while the feature is off, so an ordinary failure does not gain a second,
contentless line.

```bash
grep 'CPI-KAFKA-PLUS-DIAG producer.retry' trace.log
```

`producer.retry.effect` and `producer.retry.exhausted` carry the same `attempts` and `recovered`
fields, so "how often does the retry actually rescue a message" is a ratio of two greppable lines.

`stopReason` is always present on a stop and answers the question the log otherwise leaves open:

| `stopReason` | Meaning |
|---|---|
| `ATTEMPTS_EXHAUSTED` | every configured attempt was used |
| `BUDGET_EXHAUSTED` | the next attempt would have exceeded `producerRetryTotalBudgetSeconds` |
| `COMMIT_OUTCOME_UNKNOWN` | the failure was in or after the commit; a retry could duplicate |
| `PERMANENT` | a data error or an unclassifiable failure |
| `IDEMPOTENCE_DISABLED` | single path with `enableIdempotence=false` |
| `RETRY_DISABLED` | `producerRetryMaxAttempts=1` |
| `INTERRUPTED` | the thread was interrupted while waiting between attempts |

### In Message Monitoring

* **Failure:** the MPL status text carries `(after N retry attempts, stopReason=…)`, and
  `retryAttempts`, `stopReason`, `txnPhase` and `totalElapsedMs` are added to the
  `KafkaAdapterError` attachment (subject to `writeMplErrorAttachment`).
* **Success after a retry:** the message gets the property and custom header property
  `KafkaRetryAttempts`, so messages that only got through thanks to a retry are filterable.
* **In the integration flow:** the header `CamelKafkaPlusRetryAttempts` is set when more than one
  attempt was needed.

## Related

* [Producer Batch Send](producer-batch.md) — transactional batching and `maxConcurrentTransactions`
* [Dead Letter Queue](dead-letter-queue.md) — the *consumer* side retry, which is a different
  mechanism with different parameters
