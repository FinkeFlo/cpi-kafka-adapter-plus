# ADR 0001: Handling Concurrency in Transactional Kafka Batching

## Status
Accepted (Option 2 — see Decision below), pending review

## Context
When sending large data sets in "Producer Batch Mode" (e.g. 10.000 records), network timeouts or broker errors can lead to a partial batch failure / data inconsistency. The first records might be committed to Kafka, while the rest fail. If the SAP CPI retries the message, it creates duplicates.

To solve this, we want to introduce **Kafka Transactions** (Atomic Batches) via `producer.beginTransaction()` and `producer.commitTransaction()`. 

However, Kafka enforces a strict rule: **A single KafkaProducer instance can only have exactly one open transaction at a time.** 
The SAP CPI Kafka Adapter can be invoked concurrently by multiple threads (e.g. when the IFlow is triggered simultaneously). If two threads attempt to start a transaction on the same shared `KafkaProducer` simultaneously, it results in a `ConcurrentTransactionsException`.

## Considered Options

### Option 1: Synchronized Shared Producer
Wrap the batch sending logic in a `synchronized` block, forcing concurrent CPI threads to wait in line.
*   **Pros:** Very simple to implement. Reuses the existing single `KafkaProducer` instance efficiently.
*   **Cons:** High risk of **Thread Exhaustion** in SAP CPI. Sending a large batch of 10.000 records might take 2-3 seconds. If 10 IFlows trigger concurrently, the 10th thread blocks for 20-30 seconds waiting for the monitor lock. This is generally considered an anti-pattern in integration middleware.

### Option 2: New Producer Instance Per Transaction
Instead of using the globally shared `KafkaProducer` for transactional batches, the adapter dynamically creates a new `KafkaProducer` instance for every incoming CPI `Exchange`, executes the transaction, and closes the producer.
*   **Pros:** 100% thread-safe and non-blocking. CPI threads can process transactions completely in parallel.
*   **Cons:** Connection overhead. Creating a new `KafkaProducer` involves establishing TCP connections and fetching cluster metadata. This typically takes ~50-150ms. 
*   *Mitigation:* Since transactions are primarily used for *large batches* (which take seconds to process), an overhead of 100ms is usually negligible compared to the total processing time.

### Option 3: Producer Object Pool
Implement an object pool (e.g., using Apache Commons Pool) that maintains a set of ready-to-use `KafkaProducer` instances (e.g. 5-10 producers). When a CPI thread needs to send a transactional batch, it borrows a producer from the pool and returns it afterward.
*   **Pros:** Best of both worlds. Zero connection overhead and non-blocking parallel execution (up to the pool size limit).
*   **Cons:** High implementation complexity. Managing producer lifecycles, pool exhaustion, and connection validation adds significant technical debt to the adapter.

## Local Validation
Before deciding, we validated all three options locally with a Docker/Testcontainers-based spike (Kafka broker running in Docker, no changes to production code) against a real Kafka broker rather than deciding on paper alone.

**Spike A — Concurrency proof.** A single shared, transactional `KafkaProducer` with 10 concurrent threads calling `beginTransaction()`/`commitTransaction()` without external synchronization fails for 9/10 threads (`IllegalStateException: Invalid transition ... IN_TRANSACTION to IN_TRANSACTION`, the client-side equivalent of `ConcurrentTransactionsException`). This confirms the premise of this ADR: a shared producer instance cannot safely run concurrent transactions without coordination.

**Spike B — Comparing Options 1/2/3 under concurrent load** (10 threads x 2000 records each): Option 1 (synchronized) showed clear head-of-line blocking (threads waiting a large share of the total run time for the lock); Option 2 (new producer per transaction) ran fully in parallel with only a one-time producer/connection-init overhead per batch; Option 3 (mini pool, size 4) was fastest. A follow-up burst test against Option 3 (20 threads vs. a pool of 4, i.e. 5x oversubscription) showed graceful degradation (bounded queuing, no outright rejections) at this local scale — but this only rules out failure at small scale, not at production peak concurrency.

**Spike C — Failure injection mid-batch.** Sending a 10,000-record batch and pausing the broker mid-flight (after the first half was flushed) reproduced the exact defect this ADR exists to fix: **without transactions, 5,711 of 10,000 records ended up visible** (a genuine partial batch / data inconsistency). **With transactions, 0 of 10,000 records were visible** to a `read_committed` consumer after the failure — the transaction was never committed, even though part of the data had already been physically written to the broker's log. The producer surfaced a clear signal (`KafkaException: ... safe to abort the transaction and continue`) that the real implementation needs to catch and explicitly resolve via `abortTransaction()`.

**Multi-node consideration.** SAP CPI runs multiple worker nodes concurrently in production (not just multiple threads within one JVM, which is all the spike above exercised). For Option 2, each worker node independently creates its own short-lived transactional producers. Kafka fences (aborts) a producer as soon as another producer starts a transaction under the *same* `transactional.id` (zombie fencing) — so the `transactionalIdPrefix` alone is not sufficient; the generated `transactional.id` must be unique per worker node as well as per in-flight transaction. The adapter's consumer side already solves an analogous problem for `group.instance.id` (`CpiKafkaPlusConsumer.resolveStaticMemberSuffix()`, using `CF_INSTANCE_INDEX` with a `HOSTNAME` fallback to get a stable per-worker-node identifier) — the producer-side implementation should reuse the same pattern rather than inventing a new one.

## Decision
**Option 2: New Producer Instance Per Transaction.**

Reasoning:
- It fully resolves the concurrency problem from the Context section (validated in Spike A/B), without introducing the thread-exhaustion risk of Option 1 or the pooling/lifecycle complexity of Option 3.
- It is the most backward-compatible of the three options: the existing long-lived, lazily-initialized `KafkaProducer` used for the current (non-transactional) send path in `CpiKafkaPlusProducer` is left completely untouched. The transactional path (gated behind `enableTransactions`) is fully additive — it creates, uses, and closes its own producer per transaction, so there is no shared state or locking that could affect the default (non-transactional) behavior. Option 1 cannot make this guarantee cleanly, since a Kafka producer that has been initialized with a `transactional.id` must use transactions for every send from then on.
- The measured connection/init overhead (Spike B) is a one-time cost per batch and is negligible relative to the multi-second processing time of the large batches this feature targets.
- Option 3 was fastest in the local spike but is not being chosen now: it adds real lifecycle complexity (pool sizing, health-checking/eviction of a producer after a failed transaction, pool shutdown handling) that isn't justified given Option 2 already meets the correctness and performance bar. It remains a candidate for later if producer-creation overhead is measured to be a real problem in production.

## Consequences
- `CpiKafkaPlusProducer`/`ProducerBatchHelper` gain a new, isolated transactional code path (active only when `enableTransactions=true`); the existing non-transactional path and its shared producer are unaffected.
- The implementation must generate a `transactional.id` that is unique per worker node and bounded per node — **not** a fresh random value (e.g. a `UUID`) per transaction. A brand-new `transactional.id` on every transaction would never be reused, and Kafka retains transaction-coordinator state (in the internal `__transaction_state` topic) for each distinct `transactional.id` until `transactional.id.expiration.ms` (default 7 days) elapses — at high batch throughput this would accumulate large numbers of never-reused, orphaned IDs and needlessly bloat coordinator state. Instead, use a small, bounded, *reused* ID space:
  - Format: `transactionalIdPrefix` + `-` + a stable per-worker-node identifier (reusing the existing `resolveStaticMemberSuffix()` pattern: `CF_INSTANCE_INDEX` with `HOSTNAME` fallback) + `-` + a bounded slot index (e.g. `0`–`3`, sized to the max. concurrent transactions allowed per node), e.g.:
    ```
    cpi-kafka-adapter-txn-0-0   (worker node 0, slot 0)
    cpi-kafka-adapter-txn-0-1   (worker node 0, slot 1)
    cpi-kafka-adapter-txn-1-0   (worker node 1, slot 0)
    ```
  - Slots are managed with a simple bounded resource (e.g. a `Semaphore(N)` per node): a transaction acquires a free slot, builds its `transactional.id` from that slot number, creates a new `KafkaProducer` for the transaction (Option 2's model is unaffected — the producer/connection is still created and closed per transaction), and releases the slot when the transaction finishes (commit or abort).
  - Reusing the same `transactional.id` string across successive, non-overlapping transactions on the same slot is safe: the transaction coordinator tracks state by ID (bumping the producer epoch on every `initTransactions()` call), not by producer object/connection. If a previous transaction on that slot crashed without committing, the *next* `initTransactions()` call with the same ID automatically fences/aborts the dangling one — this is the intended zombie-recovery behavior, not a risk.
  - This keeps the total number of distinct `transactional.id`s bounded (worker nodes × slots per node) and reused indefinitely, avoiding the coordinator-state bloat of a per-transaction GUID, while still preventing cross-node fencing collisions.
- The implementation must explicitly handle the commit/abort recovery sequence surfaced in Spike C: a failed `commitTransaction()` (timeout or unacknowledged-messages error) requires calling `abortTransaction()`, and the resulting state needs to be handled correctly rather than assumed to always succeed on the first attempt.
- Option 3 (pool) is not ruled out permanently — if producer-creation overhead becomes a measured problem in production after Option 2 ships, it can be revisited, informed by a dedicated pool-exhaustion load test under realistic peak concurrency (the local burst test only validated small scale).

