# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project follows [Semantic Versioning](https://semver.org/). See
[VERSIONING.md](https://github.com/finkeflo/cpi-kafka-adapter-plus/blob/main/VERSIONING.md) for how the adapter version maps to SAP CPI
iFlow compatibility.

## [Unreleased]
### Added
- Added a maintainer-only, manually triggered `Deploy to CPI (E2E tenant)` GitHub Actions workflow (`workflow_dispatch`, environment-protected, restricted to the maintainer) to build and deploy a chosen branch/tag directly to the CPI E2E test tenant via the Integration Content OData API (`IntegrationAdapterDesigntimeArtifacts` + `DeployIntegrationAdapterDesigntimeArtifact`), enabling pre-release E2E testing without needing a full release cycle first. No effect on the adapter runtime; CI-only tooling.
- Chained the existing `e2e-consumer-tests.yml` / `e2e-producer-tests.yml` suites (now also callable as reusable workflows via `workflow_call`) as an automated post-deploy round-trip smoke test in `Deploy to CPI (E2E tenant)`, closing #15.

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
