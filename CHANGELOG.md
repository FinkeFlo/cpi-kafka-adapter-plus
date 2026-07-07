# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project follows [Semantic Versioning](https://semver.org/). See
[VERSIONING.md](VERSIONING.md) for how the adapter version maps to SAP CPI
iFlow compatibility.

## [1.0.4] - Unreleased

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
