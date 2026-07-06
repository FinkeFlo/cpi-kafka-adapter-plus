# Kafka Adapter Plus

Custom Apache Kafka adapter for SAP Cloud Integration (CPI) built with the SAP Adapter Development Kit (ADK).

## Key Features

- **Kafka Sender Adapter** — Kafka Consumer that feeds messages into CPI IFlows
- **Kafka Receiver Adapter** — CPI IFlow output published to Kafka topics
- **Batch Processing** — JSON_ARRAY, XML_LIST, and SPLIT_EXCHANGES modes for high-throughput consumption
- **Avro / Schema Registry** — Confluent Schema Registry integration for Avro serialization and deserialization
- **Security** — SASL/PLAIN, SASL/SCRAM, SSL/TLS, and mTLS authentication
- **At-Least-Once Delivery** — Manual offset commit after successful message processing
- **CPI Tracing** — Full MPL (Message Processing Log) integration
- **Header-Based Routing** — Dynamic topic, key, and partition override via exchange headers

## Architecture

The adapter follows the standard Apache Camel component model:

```
Component (cpi-kafka-plus)
  └── Endpoint (cpi-kafka-plus:topicName)
        ├── Consumer (Sender direction: Kafka → CPI)
        └── Producer (Receiver direction: CPI → Kafka)
```

## Next Steps

- [Getting Started](getting-started.md) — Build the adapter and deploy it to CPI
- [Configuration Reference](configuration.md) — All available adapter parameters
- [Features](features/batch-processing.md) — Detailed feature documentation
