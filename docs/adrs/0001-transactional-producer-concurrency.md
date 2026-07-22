# ADR 0001: Handling Concurrency in Transactional Kafka Batching

## Status
Proposed / Under Validation

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

## Decision
[To be decided after validation]

## Consequences
[To be documented based on the decision]
