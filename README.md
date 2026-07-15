# Kafka Adapter Plus

![Kafka Adapter Plus logo](docs/assets/branding/logo.png)

Custom Apache Kafka adapter for SAP Cloud Integration (CPI) built with the SAP Adapter Development Kit (ADK). Provides full Sender and Receiver adapter support with advanced batch processing, Avro/Schema Registry integration, and enterprise-grade security.

📖 **Full documentation:** **[finkeflo.github.io/cpi-kafka-adapter-plus](https://finkeflo.github.io/cpi-kafka-adapter-plus/)**

## Features

- **Kafka Sender Adapter** -- Consumes messages from Kafka topics and feeds them into a CPI integration flow (used on a Sender channel)
- **Kafka Receiver Adapter** -- Publishes integration flow messages to Kafka topics (used on a Receiver channel)
- **Batch Processing** -- Combine multiple Kafka records into a single message (JSON array or XML list) for higher throughput, or process each record as its own IFlow execution (no batching) when per-record handling is required
- **Avro / Schema Registry** -- Confluent Schema Registry integration for Avro serialization and deserialization
- **Security** -- SASL/PLAIN, SASL/SCRAM, SSL/TLS, and mTLS authentication
- **At-Least-Once Delivery** -- Manual offset commit after successful message processing
- **CPI Tracing** -- Full MPL (Message Processing Log) integration and IFlow connection monitoring
- **Header-Based Routing** -- Dynamic topic, key, and partition override via exchange headers

## Quick Start

### Prerequisites

- Java 17+ (compiles to Java 8 bytecode for CPI runtime compatibility)
- Maven 3.8+
- SAP ADK artifacts in local Maven repository

### Build

```bash
mvn clean install
```

This produces:
1. An OSGi bundle JAR (fat bundle with embedded Kafka, Avro, Confluent, and Jackson dependencies)
2. An ESA archive (via ADK `build` goal) deployable to SAP CPI

### Build ESA in GitHub Actions (no release tag)

Use the **ESA Preview** workflow (`.github/workflows/esa-preview.yml`) via **Actions → ESA Preview → Run workflow**.
It runs `mvn -B clean install` on the selected branch, executes all tests, and uploads the generated `.esa`
as a workflow artifact for download.

### Run Tests

```bash
mvn test
mvn verify
```

`mvn test` generates a JaCoCo coverage report under `target/site/jacoco/`.
In CI, the `build` workflow uploads the unit-test JaCoCo report as a downloadable artifact.

## Architecture

The adapter follows the standard Apache Camel component model:

```
Component (cpi-kafka-plus)
  └── Endpoint (cpi-kafka-plus:topicName)
        ├── Consumer (Sender direction: Kafka → CPI)
        └── Producer (Receiver direction: CPI → Kafka)
```

- **CpiKafkaPlusComponent** -- Camel component factory; creates endpoints from `cpi-kafka-plus:topic` URIs
- **CpiKafkaPlusEndpoint** -- Central configuration holder with `@UriParam` fields for all adapter settings
- **CpiKafkaPlusConsumer** -- `ScheduledPollConsumer` that polls Kafka with batch support and manual offset commit
- **CpiKafkaPlusProducer** -- `DefaultProducer` that publishes CPI exchange bodies to Kafka synchronously

## Configuration

See [docs/configuration.md](docs/configuration.md) for detailed adapter configuration reference.

## Deployment

Deploy the generated ESA archive to SAP CPI via the Integration Content API or the CPI Web UI.

## License

This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**. See [LICENSE](LICENSE) for details.

You are free to use, deploy, and modify this adapter — including commercially. However, the AGPL-3.0 is a strong copyleft license: if you distribute a modified version **or** offer it to others over a network (e.g. as a hosted service), you must make your complete corresponding source code available under the same license, preserving the original copyright notices. In other words: the code may be used, but it cannot be turned into a closed-source/proprietary product.

### Third-Party Licenses

When you build this project, Maven will download third-party dependencies with their
own licenses. Notable license families include:

- **Apache License 2.0** — Kafka, Avro, Jackson, and most other dependencies
- **Confluent Community License v1.0** — Schema Registry Client, Kafka Avro Serializer
- **MIT License** — minimal-json
- **BSD 3-Clause** — re2j, ASM

See the [NOTICE](NOTICE) file for details. A complete, machine-readable list of all
dependencies and their licenses (`THIRD-PARTY.txt`) is generated locally when you build
the project (`mvn generate-resources`).

## Trademarks

This is an independent, community-developed adapter. It is **not affiliated with, endorsed by, or sponsored by SAP SE**.

*SAP*, *SAP Cloud Integration*, *SAP Integration Suite*, and other SAP product names referenced here are trademarks or registered trademarks of SAP SE (or an SAP affiliate) in Germany and other countries. *Apache*, *Apache Kafka*, and *Apache Camel* are trademarks of the Apache Software Foundation. *Confluent* and *Confluent Schema Registry* are trademarks of Confluent, Inc. All other trademarks are the property of their respective owners.

These names are used solely for descriptive and identification purposes to indicate interoperability, and do not imply any endorsement or partnership.
