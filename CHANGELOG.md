# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project follows [Semantic Versioning](https://semver.org/). See
[VERSIONING.md](https://github.com/finkeflo/cpi-kafka-adapter-plus/blob/main/VERSIONING.md) for how the adapter version maps to SAP CPI
iFlow compatibility.

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
