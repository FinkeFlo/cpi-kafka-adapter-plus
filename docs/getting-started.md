# Getting Started

## Download

Get the ready-to-deploy ESA archive from the [GitHub Releases page](https://github.com/FinkeFlo/cpi-kafka-adapter-plus/releases) — no build required.

Download the `.esa` file attached to the latest release and continue with the deployment steps below.

## Building from Source (optional)

If you want to build the adapter yourself instead of using a release artifact:

- **Java 17+** — Compiles to Java 8 bytecode for CPI runtime compatibility
- **Maven 3.8+**
- **SAP ADK artifacts** in your local Maven repository

```bash
mvn clean install
```

This produces two artifacts:

1. **OSGi bundle JAR** — Fat bundle with embedded Kafka, Avro, Confluent, and Jackson dependencies
2. **ESA archive** — Deployable to SAP CPI via the ADK `build` goal

Run the test suite with `mvn test` (no Kafka broker required).

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
