# SAP CPI Kafka Adapter Plus

Custom Apache Kafka adapter for SAP Cloud Integration (CPI) built with the SAP Adapter Development Kit (ADK). Provides full Sender and Receiver adapter support with advanced batch processing, Avro/Schema Registry integration, and enterprise-grade security.

## Features

- **Kafka Sender Adapter** -- Kafka Consumer that feeds messages into CPI IFlows
- **Kafka Receiver Adapter** -- CPI IFlow output published to Kafka topics
- **Batch Processing** -- JSON_ARRAY, XML_LIST, and SPLIT_EXCHANGES modes for high-throughput consumption
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

### Run Tests

```bash
mvn test
```

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
