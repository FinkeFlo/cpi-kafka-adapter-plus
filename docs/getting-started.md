# Getting Started

## Prerequisites

- **Java 17+** — Compiles to Java 8 bytecode for CPI runtime compatibility
- **Maven 3.8+**
- **SAP ADK artifacts** in your local Maven repository

## Build

```bash
mvn clean install
```

This produces two artifacts:

1. **OSGi bundle JAR** — Fat bundle with embedded Kafka, Avro, Confluent, and Jackson dependencies
2. **ESA archive** — Deployable to SAP CPI via the ADK `build` goal

## Run Tests

```bash
mvn test
```

Tests run without a Kafka broker — no external dependencies required.

## Deployment to SAP CPI

Deploy the generated ESA archive to SAP CPI using one of these methods:

### Via CPI Web UI

1. Navigate to your CPI tenant's Design workspace
2. Open or create an Integration Package
3. Upload the ESA file as a custom adapter
4. The adapter appears as **CPI Kafka Plus** in the IFlow editor

### Via Integration Content API

Use the SAP CPI OData API to upload the ESA programmatically.

## Verify the Installation

After deployment, create a new IFlow and verify that:

- **Sender channel**: "CPI Kafka Plus" is available as an adapter type
- **Receiver channel**: "CPI Kafka Plus" is available as an adapter type

## What's Next

- [Configuration Reference](configuration.md) — Configure connection, security, batch, and Avro settings
- [Batch Processing](features/batch-processing.md) — Set up high-throughput batch consumption
- [Authentication](security/authentication.md) — Configure SASL/SSL security
