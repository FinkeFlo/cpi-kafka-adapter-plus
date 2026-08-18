# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project follows [Semantic Versioning](https://semver.org/). See
[VERSIONING.md](https://github.com/finkeflo/cpi-kafka-adapter-plus/blob/main/VERSIONING.md) for how the adapter version maps to SAP CPI
iFlow compatibility.

## [Unreleased]
### Fixed
- Eight further log statements carried the marker but still lost their stack trace, in `AvroDeserializerHelper`, `AvroSerializerHelper`, `CredentialHelper`, `DlqProducerHelper` and `RecordProcessor`. Each passed `e.getMessage()` into the text and the exception itself as a trailing SLF4J argument, which the CPI trace appender discards — the same defect that made the original incident undiagnosable, in five files nobody had looked at. All now serialise the cause chain through `AdapterDiagnostics`.
- The iFlow monitoring publish failure was logged at WARN under a comment explaining that it had to stay visible so a future ADK API change would surface rather than hide. WARN does not reach the tenant trace file, so the stated intent was defeated by the level; it is now reported once at ERROR.
- A failure while closing the DLQ producer was logged at WARN without a marker, although it can mean buffered records were never flushed.
- Enriching a failure with Message Processing Log context could itself throw and replace the exception it was meant to describe: the context map was dereferenced without a null check and the topic default was evaluated eagerly, both outside the `try`. A `finally` block performing reflection could also displace an in-flight exception, so a close failure would be reported instead of the binding defect that actually mattered.
- A single flag suppressed *every* subsequent ADK binding failure once any one of them had been reported, so a second, unrelated binding going dead after a platform upgrade would never be reported at all. Suppression is now per distinct failure, keeping the one-line-per-kind guarantee.
- The producer is now rebuilt when the exception classification says it is unusable, rather than only after a consecutive-failure counter happens to trip. A `RETRIABLE` or `FATAL_DATA_ERROR` outcome deliberately does not rebuild — a record that is too large will be exactly as too large on a fresh producer, and rebuilding on a poison message would be a self-inflicted outage. Rebuilds are rate-limited so a failing batch cannot cause a rebuild storm, and their effect is now measured: `producer.rebuild.outcome` reports what was rebuilt and `producer.rebuild.effect` reports whether the *next* send actually recovered, because rebuilding and then having no idea whether it helped was the previous state of affairs.
- A transactional send recovering after failures was logged at INFO and therefore invisible in the tenant trace, so recovery looked identical to continued failure.

- Producer initialisation failures are now always logged at ERROR. The first nine attempts used to be WARN, which does not reach the CPI tenant trace file — so the most common real failure class (credentials, TLS, an unreachable broker) produced nothing visible until the tenth attempt.
- Every adapter log line now carries one marker. `[CPI-KAFKA-PLUS]` competed with `[CPI-KAFKA-PLUS-DIAG]` on exactly the four most valuable lines, the adapter start failures and endpoint validation messages, so a single grep could not retrieve an incident. `CredentialHelper` and `SecurityConfigHelper` carried no marker at all, which excluded the whole credential and TLS failure class from any marker-based search.
- Message Processing Log tracing now works at all. The ADK message-log factory was fetched with `camelContext.getRegistry().lookupByName("com.sap.it.api.msglog.adapter.AdapterMessageLogFactory")`, but ADK APIs are not Camel registry beans — `ITApiFactory` resolves them through an OSGi Declarative Services component that binds handlers by an `apiType` service property, with no named bean involved. The lookup therefore always returned `null`, tracing was permanently disabled, and that was reported at DEBUG, which never reaches the tenant trace file. It now uses `ITApiFactory.getApi(...)`, the documented path already used successfully by `CredentialHelper`, resolved per exchange because the exchange is the context argument.
- Three reflective ADK lookups could never have succeeded, all masked by the same swallowed exception. `getMessageLog` was looked up with `Exchange.class` where the ADK declares `Object`, and `Class.getMethod` requires an exact parameter-type match — the same defect class as the historical `setException(Exception.class)` bug. `isTraceActive`, `createTraceMessage`, `writeTrace` and `setEncoding` were resolved on the platform's internal implementation classes rather than on the interfaces, which fails at `invoke` with `IllegalAccessException` for a non-public class. All are now resolved on the ADK interfaces with the declared parameter types, and a new test asserts every signature against the real ADK jars.
- A dead Message Processing Log binding is now reported once at ERROR instead of silently at DEBUG. At DEBUG a broken binding is indistinguishable from a working one, which is exactly how the above stayed unnoticed.
- Error traces now use the `RECEIVER_INBOUND_FAULT` trace type and carry the full serialised stack trace rather than only the messages of the cause chain.
- Producer send failures are now always logged at ERROR, with the full cause chain and stack trace serialised into the message text. Previously a failure was logged only when its cause matched a three-entry "fatal" allow-list, or after three *consecutive* failures; an unclassified exception with successful sends in between reset the counter and produced no log line at all. Analysis of a production tenant trace found five send failures across three topics and eighteen minutes for which the adapter emitted exactly zero log lines. The consecutive-failure threshold now governs only whether the producer reconnects — which is what it was for — and both that decision and the classification are reported as fields (`fatalClassification`, `reconnectTriggered`, `consecutiveFailures`).
- Transactional send failures now log the throwable itself instead of only `getClass().getSimpleName()`, which discarded the message, the cause chain and the stack trace.

### Added
- New `diagnosticsLevel` adapter option (`STANDARD` by default, `FULL`) with a corresponding field in both the sender and receiver metadata, so verbose extras can be switched on during an investigation. `STANDARD` remains fully diagnostic on its own: no failure information was moved behind `FULL`, only genuinely expensive extras such as a bounded thread dump.
- Stable, greppable error codes (`CpiKafkaPlusErrorCode`, `KP-AREA-NNN`) that a support ticket can quote, emitted both in the ERROR log line and as an MPL header property so the same fault yields the same code regardless of which path reports it.
- Three-way exception classification (`RETRIABLE`, `FATAL_PRODUCER_UNUSABLE`, `FATAL_DATA_ERROR`, `UNKNOWN_FATAL`) replacing a three-entry allow-list that let nearly every real failure fall through as "not fatal", including the exception behind the incident. It classifies on the Kafka 4.x `ApplicationRecoverableException` and `RefreshRetriableException` base classes rather than enumerating leaf exceptions, so a future client version adding a new leaf is classified correctly without a code change, and tests pin each hierarchy relationship it depends on. An unrecognised exception is reported as `UNKNOWN_FATAL` rather than being given a confident label, because a classifier that guesses silently is the failure mode this work exists to remove.
- Failures now reach the Message Processing Log independently of whether trace is switched on, via custom header properties (`KafkaAdapterErrorCode`, `KafkaAdapterTopic`, `KafkaAdapterProducerPath`, `KafkaAdapterRetryable`), the full error block as an attachment, and a `FAILED` status event so a failed message is marked failed in the monitor instead of appearing successful. MPL tracing was never active in the analysed corpus, which is why the trace-independent channel is the primary one. Error traces are also now written from the consumer and record-processing paths, not only from the producer, and use the fault-specific trace types.
- `producer.node.fault.escalation`: when the same fault — keyed by exception class and error code — recurs five times within twenty minutes on one JVM despite the mitigation, the adapter says so explicitly and names where the node address can be found in the trace record, because the decisive fact in the analysed incident was that every occurrence sat on one worker node out of seven. The counter is guarded so concurrent worker threads cannot lose increments, which matters because multiple threads failing at once is the signal it exists to detect.
- Producer diagnostics now carry the producer path on every line, the resolved `client.id`, and the measured metadata wait, so a slow or blocking metadata fetch is visible before it becomes a failure — that wait is the exact place the incident occurred. A throttled success heartbeat distinguishes "healthy and quiet" from "not running at all", which previously looked identical in the trace.
- `DiagnosticContractTest` enforces the logging contract mechanically by scanning the sources: one marker, no `Throwable` handed to SLF4J as a parameter, no second marker, and no ADK reflection resolved on implementation classes. The contract was previously only a written promise. The test found ten violations on its first run.
- Two further contract rules guarding against dead instrumentation: every instrumentation entry point must have a caller outside the class that declares it, and every reflectively resolved ADK method must also be invoked. Both were verified by deliberately breaking them, because a guard that cannot fail is the exact defect it is meant to prevent. The second rule matters more than it looks: resolving a `Method` and never invoking it compiles, runs, throws nothing and writes nothing.
- New ADR 0004 recording the diagnostic logging contract, the runtime facts that forced it, and the alternatives rejected along the way.

- New docs page `docs/troubleshooting.md`: how the adapter logs and why, a field reference for the structured failure lines, grep recipes for the tenant trace (including how to read the node field, which is what identified the incident as node-local), the retry bounds, and what the KAFKA-10902 signature means.
- Adapter failure lines now carry the identifiers that tie them to one integration-flow message: `mplId` (`SAP_MessageProcessingLogID`), `applicationId` and `exchangeId`. The tenant trace format reserves four correlation fields per record and all four were empty on every adapter line in the analysed corpus, so a failure could be seen but not attributed to a message, a sender or a payload.
- The producer now avoids the KAFKA-10902 code path instead of only recovering from it. `KafkaProducer.waitOnMetadata()` blocks in the defective `ProducerMetadata.awaitUpdate()` only when a topic's partition count is missing from the client's metadata cache, so two changes keep it present: the metadata for each topic is fetched once up front, before any record is submitted, and `metadata.max.idle.ms` is raised from the client default of 5 minutes to 1 hour. That default mattered more than it appears — it discards the metadata of a topic that has not been produced to for five minutes, so an integration flow producing less often than that entered the vulnerable blocking path on *every* message. Freshness is unchanged, being governed by `metadata.max.age.ms`, whose refresh runs on the client's network thread and does not block senders. A failed pre-warm never fails the exchange.
- The producer now survives KAFKA-10902, an open, unfixed defect in the Kafka client that is present in the embedded `kafka-clients` version. `ProducerMetadata.awaitUpdate` is `synchronized` and hands `this` to `SystemTime.waitObject`, which re-enters the same monitor and calls `Object.wait()` at recursion depth 2; when the thread loses ownership the send fails with `IllegalMonitorStateException: current thread is not owner`. A synchronous failure of that kind is retried, which provably cannot duplicate a record: `KafkaProducer.doSend()` calls `waitOnMetadata(...)` before `accumulator.append(...)`, so the record never entered the accumulator. Retrying is bounded three ways and the first bound reached stops it — at most 3 retries per record, at most 5 across an entire batch regardless of record count, and never past the existing batch deadline. Worst case is 5 extra attempts and about 250 ms per batch. Every outcome is logged, including successful recovery, with `retryOutcome`, `retryCount`, `batchRetriesLeft` and `stopReason`.
- Producer batch failures now report which of the two throw sites they came from (`phase=SYNC_SEND` or `phase=AWAIT_FUTURE`), plus `recordIndex`, `batchSize` and `elapsedMs`. The two previously produced byte-identical messages and could not be told apart in a trace, which matters because only the synchronous one is safe to retry.
- New internal `AdapterDiagnostics` helper producing single-line, greppable `key=value` diagnostics under one marker. It serialises the throwable — cause chain, suppressed exceptions and stack frames — into the message text, because the CPI tenant trace appender discards the `Throwable` argument of a log call and keeps only the rendered message. It also centralises the SLF4J call itself: `log.error(text, throwable)` would treat a `{}` occurring inside an exception message as a placeholder and silently drop the stack trace.
- New docs page `docs/features/kafka-headers.md` listing all consumer, producer, and DLQ Kafka headers with descriptions, types, and conditions.

### Changed
- `transaction.two.phase.commit.enable` is no longer described as controlling "Kafka Transaction Protocol V2 (KIP-890)". It is KIP-939 (Two-Phase Commit), a client-side opt-in; KIP-890 is negotiated between client and broker and has no client config of that name. The behaviour of the `transactionV2Enabled` option is deliberately unchanged — correcting the name would break existing iFlow configurations — and the mismatch is now recorded with the repository's `TODO [tech-debt]` convention instead of an incorrect comment.

- The consumer emit-cycle heartbeat is now throttled to at most one line every 5 minutes instead of one line per poll cycle, and is always emitted immediately when the initialisation state changes. Analysis of a production tenant trace showed this single statement produced 32,708 of 32,708 adapter log lines — 3.76% of the entire shared-tenant log — while carrying no information beyond "still alive", and while not a single genuine send failure was logged. It stays at ERROR on purpose, because only ERROR reaches the CPI tenant trace file; the per-cycle detail remains available at DEBUG.
- The Java 11 API level is now enforced by the compiler instead of only being documented. `pom.xml` uses `maven.compiler.release=11` rather than `maven.compiler.source`/`target`, so javac links against the Java 11 API signatures. Previously the compiler linked against the JDK 17 class library, so a Java 12+ API (for example `String.formatted`) compiled cleanly and only failed at runtime on the tenant with `NoSuchMethodError` — a gap that `CHANGELOG` 1.2.2 described but did not close. Verified: a `String.formatted` call now fails the build. Bytecode remains class-file version 55; the `compile-jgss-stubs` execution keeps its own `release` 8 override.
- Standardized pull request quality gates: enforce Conventional Commit format for PR titles in CI, set squash merge to use PR title and body, add repository-managed local Git hooks (`commit-msg` for Conventional Commits validation, `pre-push` for fast unit tests + secret scans).
- Added setup script and documentation for local hook and commit-template activation in CONTRIBUTING.md.
- Enforce `REVIEW_REQUIRED` and `CODEOWNER_REVIEW` on main branch protection; disallow force push and branch deletion.

## [1.2.4] - 2026-08-17
### Added
- New adapter parameter `transactionV2Enabled` (default `true`) to disable Kafka Transaction Protocol V2 (KIP-890). Set to `false` as a workaround for `IllegalMonitorStateException` in Kafka 4.x clients when a transactional producer is fenced during `initTransactions()`. V1 behavior (Kafka ≤ 3.x compatible) is stable and avoids the race condition in the `TransactionManager` sender thread.
- Topic hash in `transactional.id` schema: the computed ID now includes an 8-character SHA-256 prefix of the target topic name (`{prefix}-{topicHash}-{instanceIndex}-{slotId}`). This structurally prevents two iFlows with the same `transactionalIdPrefix` but different target topics from sharing a transactional producer ID and fencing each other.
- `AdapterTracingHelper.traceError()`: enriched MPL error tracing with structured context fields (topic, transactionalId, batchSize, errorType). Called in transactional batch, regular batch, and single-send failure paths. No-op unless CPI Trace level is active.

### Fixed
- `InvalidProducerEpochException` and `IllegalMonitorStateException` on transactional producer iFlows sharing the same `transactionalIdPrefix` across multiple topics.
- `CAMEL_CONTEXT_NOT_STARTED` caused by leading/trailing whitespace in adapter config fields (e.g. topic name). SAP CPI does not URL-encode field values; spaces in query parameter values now trimmed automatically before Camel parses the URI.
- Startup failures now logged as `[CPI-KAFKA-PLUS] Adapter failed to start (topic='...') : <cause>` before re-throwing, making `CAMEL_CONTEXT_NOT_STARTED` diagnosable from the trace log.

### Changed
- Startup log now includes the full computed `transactional.id` example and the effective `transactionV2Enabled` flag for diagnostics.
- `transactional.id` length is validated at startup; throws `IllegalArgumentException` if the computed ID exceeds 249 characters (Kafka broker limit).

## [1.2.3] - 2026-08-11
### Fixed
- Prevented `Node Crashed` on a plaintext Security Protocol against TLS-only brokers by probing bootstrap listeners for TLS before creating Kafka clients (producer, transactional producer, consumer).
- Detect TLS listeners that reject the probe handshake with a TLS alert before sending a certificate, avoiding false inconclusive results for TLS-version, cipher-suite or fronting-device mismatches.
- Cache TLS listener-probe results per bootstrap/security configuration so transactional batches do not open probe connections and wait on silent endpoints for every message.
- Bounded producer send waits (`delivery.timeout.ms` + guard margin) so a dead Kafka sender thread cannot block CPI worker threads indefinitely; applies to single-send, batch and DLQ paths.
- Bounded topic-existence checks and removed unbounded `flush()` on batch abort paths to avoid hangs in the same sender-thread failure mode.

### Changed
- Added `scripts/build-esa.sh` for CI-parity local ESA builds via Docker and hardened it against macOS `.DS_Store` clean-step interference.
- Added a `tls-mismatch` scenario to the producer E2E suite that points a plaintext Security Protocol at a TLS-only listener on the test tenant and fails if the adapter ever lets the node crash again. CI-only; no effect on the adapter runtime.

## [1.2.2] - 2026-08-10
### Changed
- CI now fails a pull request that leaves `CHANGELOG.md` untouched, enforcing a rule that was documented but unchecked. Add the `no-changelog` label to a pull request that genuinely needs no entry. `CONTRIBUTING.md` states the rule per pull request instead of per commit, since merges are squashed into a single commit on `main`.
- Regenerated `THIRD-PARTY.txt`: the Confluent test dependencies are pinned to 7.9.9 in `pom.xml`, but the generated license list still named 7.9.8. No dependency change.
- CI now fails when `THIRD-PARTY.txt` no longer matches the resolved dependencies. Dependency bumps change the versions without regenerating the file, which is how it drifted twice.
- Corrected the documented bytecode target from Java 8 to Java 11 in `CONTRIBUTING.md`, `README.md` and the PR template. `maven.compiler.source`/`target` have been `11` since the initial release, so the "Java 8 bytecode" claim was never accurate — class-file version 55 cannot load on a Java 8 JVM at all. The API constraint is retained and restated: `source`/`target` link against the JDK 17 class library, so a Java 12+ API compiles but fails at runtime.

## [1.2.1] - 2026-08-10
### Added
- Added a maintainer-only, manually triggered `Deploy to CPI (E2E tenant)` GitHub Actions workflow (`workflow_dispatch`, environment-protected, restricted to the maintainer) to build and deploy a chosen branch/tag directly to the CPI E2E test tenant via the Integration Content OData API (`IntegrationAdapterDesigntimeArtifacts` + `DeployIntegrationAdapterDesigntimeArtifact`), enabling pre-release E2E testing without needing a full release cycle first. No effect on the adapter runtime; CI-only tooling.
- Chained the existing `e2e-consumer-tests.yml` / `e2e-producer-tests.yml` suites (now also callable as reusable workflows via `workflow_call`) as an automated post-deploy round-trip smoke test in `Deploy to CPI (E2E tenant)`, closing #15.

### Fixed
- Fixed silent message loss when an iFlow swallowed a processing error internally — for example via an exception subprocess, or an Error End event in a sub-iFlow called with ProcessDirect. Camel's error handler clears the exception once it is marked as handled, so `exchange.getException()` came back `null` and the record looked successfully processed: the consumer committed the Kafka offset and the message was dropped without ever reaching its destination and without an error in the MPL. The consumer now additionally checks Camel's `Exchange.FAILURE_HANDLED` flag and raises a `CamelExchangeException` (chaining the original cause from `Exchange.EXCEPTION_CAUGHT`, so the DLQ record and the MPL entry still show the real root cause), which blocks the offset commit so the record is redelivered or routed to the DLQ. Because a swallowed error is deterministic and would fail identically on retry, it is classified as a permanent error and — with the default `retryOnlyTransientErrors=true` — goes straight to the DLQ instead of burning `dlqMaxRetries` attempts.
  - **Behaviour change, no opt-in:** iFlows that deliberately catch an error and complete via an exception subprocess were previously treated as success and had their offset committed. They are now treated as failures and will be redelivered (or routed to the DLQ once `dlqMaxRetries` is exhausted). If an iFlow is *meant* to absorb certain errors and acknowledge the record anyway, handle them so the route completes normally instead of terminating in the exception subprocess. Routes using `continued(true)` semantics — where the error is ignored and the original route runs to completion — are deliberately **not** affected and still commit their offsets.
- Fixed connection failures being reported only as `TimeoutException: Topic ... not present in metadata after N ms`. Kafka uses that one message whenever no topic metadata arrives, so rejected credentials, a failed TLS handshake and a security protocol that does not match the broker's listener all looked like a missing topic. The producer's topic probe already detected them but discarded the exception; it now keeps the cause, and the send error carries it in the message text and as a suppressed exception. Rejected authentication and TLS handshake failures fail the exchange immediately (354 ms instead of a full timeout in a local test) because they would recur; a probe that merely timed out does not, since the send may still succeed. Authorization errors are excluded — a missing DESCRIBE permission says nothing about producing.
  - **Behaviour change, no opt-in:** a send to a topic that does not exist is now rejected by the adapter. On clusters with `auto.create.topics.enable=true` — the Apache Kafka default, switched off on Confluent Cloud and most managed offerings — the first send previously created the topic implicitly; such topics must now be created explicitly. A topic that was only just created is not affected: a missing topic is re-checked briefly before the send fails, so "create the topic, then send" keeps working.
- Fixed a non-TLS `securityProtocol` against a TLS-only broker being undiagnosable: the adapter now names the protocol as the likely cause. Kafka cannot report this as a handshake error, because a `PLAINTEXT`/`SASL_PLAINTEXT` client is dropped before any negotiation. Also logged at deployment time, and by the Sender on repeated poll failures.

### Changed
- **Security Protocol** dropdown entries now state whether TLS is used: `SASL_SSL (SASL over TLS)`, `SSL (TLS, certificate authentication)`, `SASL_PLAINTEXT (no TLS)`, `PLAINTEXT (no TLS, no authentication)`. Values are unchanged.
- **SSL Keystore Alias** help text now leads with the common case: empty is correct for brokers with a publicly trusted certificate (e.g. Confluent Cloud) and TLS stays fully active — the alias is only for a private CA, a self-signed certificate, or mTLS.
- Both ship as micro bumps edited in place per SAP's versioning rules (`metadata-receiver-1.1.1.xml` → `1.1.2`, `metadata-sender-1.2.0.xml` → `1.2.1`), so existing channels in **both** directions pick them up automatically with all settings preserved.
- Docs: the security protocols now have a "when to use" column, plus a troubleshooting section for the `Topic ... not present in metadata` symptom in [Authentication](docs/security/authentication.md).
- `VERSIONING.md` now records the ADK rule that a micro bump must edit its variant file in place — a second file for the same direction is rejected with *"All Receivers should have different significant version (X.X. Micro version ignored)"*.
- `.gitignore` now covers local AI coding agent state; shared instruction files stay tracked.
- Test infrastructure: `KafkaTestInfrastructure.createTopic`/`createSaslTopic` now poll until the broker serves the new topic in metadata, instead of returning as soon as the controller accepted the request.

## [1.2.0] - 2026-07-30
### Added
- Added a **Streaming (Continuous) consumption mode** for the Sender (Consumer) adapter via the new `consumptionMode` option (`SCHEDULED` default | `STREAMING`). In `STREAMING` mode the consumer uses Camel's greedy scheduling: as long as a `poll()` returns records the next poll fires immediately with no delay, so records are consumed continuously with minimal latency; when the topic is idle the consumer only waits out the poll timeout (`batchTimeout`) plus a fixed 1 second heartbeat delay before retrying (no latency cost — `poll()` returns as soon as records arrive). This mirrors the standard SAP Kafka adapter's continuous behaviour and eliminates the up-to-`pollingIntervalSeconds` latency of scheduled polling. Delivered as the new `metadata-sender-1.2.0.xml` variant (Consumption tab → "Consumption Mode"); existing iFlows keep the frozen `metadata-sender-1.1.0.xml` until "Update Version". In `STREAMING` mode the `pollingIntervalSeconds` and `drainEnabled` fields are ignored (greedy scheduling replaces the interval/drain mechanism) and are greyed out in the UI. Streaming is freely combinable with batch mode. In `STREAMING` the start-up validations for the drain-related fields (`drainEnabled` + `AUTO` commit, `minBacklogToDrain` bounds) and for `pollingIntervalSeconds` are skipped, so a stale value left over from a `SCHEDULED` configuration cannot block the iFlow from starting. Consumer-only; the Receiver line is unchanged.

### Changed
- Removed **Individual Exchanges (No Batching)** (`SPLIT_EXCHANGES`) from the Batch Output Format dropdown in `metadata-sender-1.2.0.xml`. `batchMode = false` is fully equivalent and is the recommended way to process records individually. Existing iFlow channel configs that already store `SPLIT_EXCHANGES` continue to work unchanged — the runtime still handles the value for backward compatibility.

### Fixed
- Fixed a consumer shutdown race where `doStop()` could close the `KafkaConsumer` on the shutdown thread while the scheduler's poll thread was still inside `poll()`/commit, logging `KafkaConsumer is not safe for multi-threaded access` and skipping a clean close (missed `LeaveGroup`, slower rebalance after undeploy/redeploy). `doStop()` now waits (bounded) until Camel reports polling has ceased before closing, guaranteeing single-threaded access. Pre-existing behaviour for scheduled polling too; surfaced and validated by the new greedy-streaming integration test.

## [1.1.1] - 2026-07-30
### Changed
- Reorganized the Sender (Consumer) adapter UI into a clearer, data-flow-oriented tab layout (Connection · Consumption · Advanced · Message Handling · Avro/Schema Registry · Error Handling). The new fetch-tuning fields (`fetchMinBytes`, `fetchMaxWaitMs`) now live under Advanced → Fetch Tuning next to `maxPartitionFetchSizeKb`. Pure re-layout: all `ReferenceName`s and defaults are unchanged, so existing iFlow channels keep working after "Update Version".
- Reorganized the Receiver (Producer) adapter UI symmetrically (Connection · Producing · Advanced · Message Handling · Avro/Schema Registry), moving Header Mapping onto the Producing tab and Transactions + Performance Tuning onto Advanced. Delivered as the new `metadata-receiver-1.1.1.xml` variant; the frozen `metadata-receiver-1.0.0.xml` is untouched. No `ReferenceName`/default changes.
- Relabeled the consumer `batchSize` field to **"Max Records per IFlow Run (MPL)"** to clarify that it groups Kafka records into a single IFlow execution (one MPL per batch).
- Relabeled the consumer `batchTimeout` field to **"Poll Timeout (ms)"**, moved it to Consumption → Polling, and removed its `batchMode` edit condition. It is the per-poll `poll()` blocking timeout and applies to every poll cycle regardless of batch mode; the previous label/placement were misleading.
- Relabeled the producer `producerBatchSizeKb` field to **"Producer Batch Size (KB)"** to distinguish it from the "Batch Send Mode" (`producerBatchMode`) option.

### Added
- Added default-value assertions for `fetchMinBytes` (1) and `fetchMaxWaitMs` (500) in `CpiKafkaPlusComponentTest`, closing the CONTRIBUTING gap for the endpoint options introduced in 1.1.0. Test-only; no runtime change.

## [1.1.0] - 2026-07-29
### Removed
- Removed the dead legacy `metadata.xml` (superseded by the `metadata-sender-*.xml` / `metadata-receiver-*.xml` split; unreferenced by the build). No adapter behavior change.

### Changed
- **Breaking (headers only):** The single-message producer no longer sets `CamelKafkaTopic`, `CamelKafkaPartition`, `CamelKafkaOffset`, or `CamelKafkaTimestamp` response headers. It now sets the adapter-native `CpiKafkaPlusTopic`, `CpiKafkaPlusPartition`, `CpiKafkaPlusOffset`, `CpiKafkaPlusTimestamp`, and `CpiKafkaPlusStatus` (`"OK"`) headers instead, consistent with the batch producer and consumer. `SAP_Receiver` is unchanged. **Action required:** any iFlow reading the old `CamelKafka*` headers after a single-message Kafka Adapter (Out) step (e.g. the E2E producer test iFlows) must be updated to read the new `CpiKafkaPlus*` headers. (#84, #85)

### Added
- Added `fetchMinBytes` (`fetch.min.bytes`) and `fetchMaxWaitMs` (`fetch.max.wait.ms`) consumer endpoint properties, exposed in the adapter UI, to enable true broker-side batching and reduce small/frequent CPI executions under low load. Defaults (1 / 500) match the Kafka client defaults, so existing configurations are unaffected unless explicitly changed. UI-visible via the new `metadata-sender-1.1.0.xml` variant (minor bump to `1.1.0`); existing iFlow channels stay on `1.0.19` until "Update Version" is used. (#88)
- Added `CpiKafkaPlusStatus` header (value `"OK"`) to the batch producer response, alongside the existing `<status>OK</status>` XML summary body, for consistent success signaling. (#85)

### Removed
- Removed the dead `embedXmlValues` endpoint property, which was never wired into the `XML_LIST` batch formatting code path and had no UI exposure. The actual behavior (auto-detecting and embedding XML-looking values, falling back to CDATA text otherwise) is unchanged. (#89)

## [1.0.19] - 2026-07-28
### Fixed
- Re-released to fix broken versions inside metadata resulting from a CI bug.

## [1.0.18] - 2026-07-28
### Fixed
- Fixed bug in GitHub Actions release workflow that wiped uncommitted version bumps.

## [1.0.17] - 2026-07-28
### Fixed
- Updated GitHub Actions release workflow to only target active minor version variants and avoid replacing `version::` in frozen metadata files.

## [1.0.16] - 2026-07-28

### Added
- **Receiver UI**: Exposed transactional producer settings (`enableTransactions`, `transactionalIdPrefix`, `maxConcurrentTransactions`) in the adapter variant metadata so they are configurable via the Integration Suite UI.

### Changed
- **Java Upgrade**: Upgraded compiler source/target compatibility from Java 8 to **Java 11**.
- **Kafka Upgrade**: Upgraded Apache Kafka client library from 3.x to **4.x** (`kafka-clients 4.3.1`).
- **Dependency Optimization**: Excluded default multi-platform `zstd-jni` from `kafka-clients` and replaced it with a targeted `linux_amd64` variant, significantly reducing the final `.esa` archive size.
- **Deprecation Cleanup**: Removed deprecated `ProducerConfig.RETRIES_CONFIG` from DLQ producer (now managed via `delivery.timeout.ms`) and `SslConfigs.SSL_PROTOCOL_CONFIG` (relying on Kafka 4.x TLSv1.3 default).
- **OSGi Compatibility**: Refactored `org.ietf.jgss` empty security stubs into an isolated compiler execution block (`src/stubs/java` under JDK 8 release target) to avoid JPMS split-package errors on Java 11+.
- **CI Workflows**: Renamed manual ESA preview workflow to "Build ESA Preview" to resolve a GitHub Actions sidebar display bug.
- **Documentation**: Aligned `CONTRIBUTING.md` with FlowMate toolkit standards, including rules for Conventional Commits and Changelog Enforcement.

## [1.0.15] - 2026-07-23

### Added
- **Producer (Receiver adapter)**: Added optional Transactional Batching mode (ADR 0001). When `enableTransactions` is enabled, each batch is sent within an isolated Kafka transaction (`producer.beginTransaction()`, `commitTransaction()`). A bounded number of transaction IDs (`transactionalIdPrefix`) are reused across worker nodes to avoid producer fencing and coordinator bloat.

## [1.0.14] - 2026-07-23

### Fixed
- **Producer (Receiver adapter)**: Configured Kafka topics are now resolved at runtime from Camel Simple expressions, including header and exchange-property lookups (e.g. `${header.topic}`, `${property.topic}`). Property-alias expressions are normalized to exchange properties for CPI compatibility, and the adapter fails fast when an expression cannot be resolved to a concrete topic.

## [1.0.13] - 2026-07-21

### Fixed
- **JSON Batch Parser**: Fixed parsing of BadgerFish-style JSON arrays generated by the standard SAP CPI XML to JSON Converter for `headers` objects containing duplicate keys.

## [1.0.12] - 2026-07-20

### Added
- **Producer (Receiver adapter)**: Support for configurable header mapping (`allowedHeaders`). Headers matching the pattern will be sent to Kafka.
- **Producer (Receiver adapter)**: Batch records (JSON/XML) can now contain explicit `headers` that bypass the exchange filter and overwrite exchange headers of the same name.

### Fixed
- **XML Batch Parser**: Fixed a critical bug where `<headers>` tags nested inside the XML payload of a record were falsely extracted.

### Changed
- **Dependencies**: Bumped `awaitility` to 4.3.0 and `jacoco-maven-plugin` to 0.8.15.

## [1.0.11] - 2026-07-20

### Fixed
- **Producer (Receiver adapter)**: Fixed an issue where the batch mode would reject a single JSON object. It now correctly wraps a single object into an array, accommodating SAP CPI's standard XML-to-JSON conversion behavior where arrays of size 1 are flattened to single objects.

## [1.0.10] - 2026-07-20

### Added
- Producer (Receiver adapter) now also sets `CpiKafkaPlusTopic` on the response, mirroring the consumer's header name (same value as the existing `CamelKafkaTopic`/`SAP_Receiver`). Lets the same downstream mapping script read the topic on both consumer and producer sides. `CpiKafkaPlusConsumerGroup` remains consumer-only (no such concept on the producer side).

## [1.0.9] - 2026-07-20

### Changed
- **Breaking (runtime headers only, no channel/metadata impact):** renamed the producer (Receiver adapter) response headers to match the consumer's naming convention, dropping the redundant `Batch` infix: `CpiKafkaPlusBatchRecordCount` → `CpiKafkaPlusRecordCount`, `CpiKafkaPlusBatchFirstOffset` → `CpiKafkaPlusFirstOffset`, `CpiKafkaPlusBatchLastOffset` → `CpiKafkaPlusLastOffset`, `CpiKafkaPlusBatchPartitions` → `CpiKafkaPlusPartitions`. `CpiKafkaPlusBatchInputFormat`/`CpiKafkaPlusBatchOutputFormat` are unaffected. If any downstream flow step (Groovy, Mapping, Router) reads the old names, update it to the new ones.

## [1.0.8] - 2026-07-20

### Fixed
- Receiver variant was missing the ADK `IsRequestResponse` flag, so SAP Cloud Integration rejected the adapter with "is not supported for the adapter" on **Send** and **Request-Reply** steps. It only worked on a plain End Message Event channel. Now works on Send/Request-Reply too, and the Request-Reply call returns the producer result in the response body.

## [1.0.7] - 2026-07-14

### Added
- Design-time validation for `pollingIntervalSeconds` (sender): the configuration dialog in the Web UI now rejects values outside 1-21600 immediately, instead of only at IFlow start. (#44)
- Design-time validation for `credentialAlias` (sender + receiver): now enforced as non-empty when Security Protocol is `SASL_SSL` or `SASL_PLAINTEXT`.

### Changed
- Renamed the `credentialAlias` GUI label from "SASL Credential Alias" to "Credential Alias" (sender + receiver). Cosmetic only; the underlying parameter name and existing channel configurations are unaffected.
- Increased producer default `maxRequestSizeKb` from 1024 KB (1 MB) to 5120 KB (5 MB).
- Increased producer default `producerBatchSizeKb` from 249 KB to 1024 KB (1 MB); removed a stale in-code comment referencing a 250 KB CPI message-size tier that does not currently apply.

## [1.0.6] - 2026-07-13

Test and CI hardening release; no runtime behavior changes.

### Added
- Real OSGi resolution integration test that verifies the ESA standalone bundles resolve in an isolated OSGi runtime, including a negative guard for unresolvable input.

### Changed
- Scoped the ESA-producing OSGi resolution check to a dedicated `osgi-resolution` Maven profile and CI job, so regular builds and integration-test shards are not slowed.
- Made ESA selection in the resolution test deterministic (fails fast on an ambiguous `target/` state) and clean up the OSGi framework storage directory after each run.

## [1.0.5] - 2026-07-13

Release focused on runtime dependency maintenance and CI stability.

### Changed
- Updated runtime dependencies: `avro` 1.11.5 → 1.12.1, `confluent` 7.9.5 → 7.9.8, and `json-schema-validator` 1.0.87 → 1.5.9.
- Updated build tooling: enforcer, license plugin, compiler, animal-sniffer, surefire/failsafe, dependency, antrun, and ASM.
- Sharded integration tests across 3 CI jobs to reduce wall-clock build time.

### Fixed
- Hardened the OSGi bundle test to validate the current build artifact instead of stale jars in `target/`.
- Pinned `commons-io:2.22.0` for Testcontainers/Ryuk compatibility.

## [1.0.4] - 2026-07-07

Dependency maintenance release. Keeps the adapter deployable on SAP CPI while
updating third-party libraries surfaced by dependency scanning.

### Changed
- Updated `kafka-clients` 3.9.1 → 3.9.2.
- Updated Jackson (core/databind/dataformats/datatypes) 2.16.0 → 2.22.0.
- Updated test dependency `commons-lang3` 3.14.0 → 3.20.0.

### Fixed
- Restored CPI-compatible LZ4 compression after the Kafka 3.9.2 bump: excluded the
  `at.yawk.lz4:lz4-java` fork (broken OSGi manifest that fails the CPI OSGi resolver)
  and pinned the well-behaved `org.lz4:lz4-java:1.8.0`, embedded in the fat bundle.

### Added
- `maven-enforcer` build guard that fails early if a known CPI-incompatible transitive
  dependency is pulled in.
- ESA content check in the preview build: fails if any stray standalone jar escapes the
  fat bundle.
- Dependabot configuration for Maven and GitHub Actions (Camel excluded — provided by CPI).

## [1.0.3] - 2026-07-02

First public release.

### Added
- **Kafka Sender adapter** (Consumer → CPI) and **Kafka Receiver adapter** (CPI → Kafka Producer).
- **Batch processing** in JSON_ARRAY, XML_LIST and SPLIT_EXCHANGES modes.
- **Avro / Confluent Schema Registry** serialization and deserialization.
- **Security:** SASL/PLAIN, SASL/SCRAM, SSL/TLS and mTLS.
- **At-least-once delivery** via manual offset commit after successful processing, with
  durable per-partition offset tracking that survives consumer-group rebalances.
- **Dead Letter Queue** routing for deserialization / poison-pill failures.
- **CPI MPL tracing** and IFlow connection-status monitoring.
- **Configurable polling** with rebalance-safe consumer-group handling.
- **Header-based routing** — dynamic topic, key and partition override via exchange headers.
- **Semantic adapter versioning** aligned with SAP CPI's compatibility model.
