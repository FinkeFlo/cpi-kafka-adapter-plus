# Avro / Schema Registry

The adapter integrates with Confluent Schema Registry for Avro serialization and deserialization.

## Overview

When working with Avro-encoded Kafka topics, the adapter can automatically deserialize (Consumer/Sender) and serialize (Producer/Receiver) messages using schemas from a Confluent Schema Registry.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `schemaRegistryEnabled` | `false` | Enable Schema Registry integration |
| `schemaRegistryUrl` | — | Confluent Schema Registry URL |
| `schemaRegistryCredentialAlias` | — | Credential alias for Schema Registry authentication (from CPI Secure Store) |
| `autoRegisterSchemas` | `false` | Automatically register schemas when producing |
| `subjectNameStrategy` | `TopicNameStrategy` | Subject naming strategy for Producer (Receiver) serialization |
| `avroOutputFormat` | `JSON` | Deserialized output format: `JSON` or `XML` |
| `avroValueDeserialization` | `true` | Deserialize Avro message values when consuming |
| `avroValueSerialization` | `true` | Serialize message values to Avro when producing |

## Consumer (Sender) Setup

To consume Avro-encoded messages:

1. Enable Schema Registry integration
2. Keep `avroValueDeserialization` enabled
3. Configure `schemaRegistryUrl` with your Schema Registry endpoint
4. Set `schemaRegistryCredentialAlias` if authentication is required
5. Choose `avroOutputFormat` (`JSON` or `XML`) for the deserialized output

| Parameter | Value |
|---|---|
| `schemaRegistryEnabled` | `true` |
| `avroValueDeserialization` | `true` |
| `schemaRegistryUrl` | `https://schema-registry:8081` |
| `schemaRegistryCredentialAlias` | `SchemaRegistryCreds` |
| `avroOutputFormat` | `JSON` |

The adapter deserializes the Avro binary payload using the schema from the registry and converts it to the specified output format before passing it to the IFlow.

## Producer (Receiver) Setup

To produce Avro-encoded messages:

1. Enable Schema Registry integration
2. Keep `avroValueSerialization` enabled
3. Configure `schemaRegistryUrl`
4. Set `schemaRegistryCredentialAlias` if authentication is required
5. Optionally enable `autoRegisterSchemas`

| Parameter | Value |
|---|---|
| `schemaRegistryEnabled` | `true` |
| `avroValueSerialization` | `true` |
| `schemaRegistryUrl` | `https://schema-registry:8081` |
| `schemaRegistryCredentialAlias` | `SchemaRegistryCreds` |
| `autoRegisterSchemas` | `false` |

## Subject Naming Strategy

`subjectNameStrategy` affects Producer (Receiver) JSON-to-Avro serialization only. Deserialization does not use this setting.

| Strategy | Serialization Support | Subject Name | Notes |
|----------|-----------------------|--------------|-------|
| `TopicNameStrategy` | Supported | `<topic>-value` | Default and only supported strategy for Producer serialization |
| `RecordNameStrategy` | Not supported for serialization | — | Requires the record schema name, which is only known after fetching the schema |
| `TopicRecordNameStrategy` | Not supported for serialization | — | Requires the record schema name, which is only known after fetching the schema |

## Authentication

Schema Registry credentials are stored in the CPI Secure Store. Create a User Credentials artifact with the Schema Registry API key and secret, then reference its alias in `schemaRegistryCredentialAlias`.

!!! note
    Avro serialization/deserialization applies to message values only. Keys are always treated as strings.
