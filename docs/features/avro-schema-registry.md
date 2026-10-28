# Avro / Schema Registry

The adapter integrates with Confluent Schema Registry for Avro serialization and deserialization.

## Overview

When working with Avro-encoded Kafka topics, the adapter can automatically deserialize (Consumer/Sender) and serialize (Producer/Receiver) messages using schemas from a Confluent Schema Registry.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `schemaRegistryEnabled` | `false` | Enable Schema Registry integration |
| `schemaRegistryUrl` | — | Confluent Schema Registry URL, starting with `http://` or `https://`. Any other URL, such as `sr.example.com:8081`, fails the deployment while the channel uses Avro. |
| `schemaRegistryCredentialAlias` | — | Credential alias for Schema Registry authentication (from CPI Secure Store) |
| `autoRegisterSchemas` | `false` | Automatically register schemas when producing |
| `subjectNameStrategy` | `TopicNameStrategy` | Subject naming strategy for Producer (Receiver) serialization |
| `avroOutputFormat` | `JSON` | Deserialized output format: `JSON` or `XML` |
| `avroValueDeserialization` | `true` | Deserialize Avro message values when consuming |
| `avroValueSerialization` | `true` | Serialize message values to Avro when producing |

## Consumer (Sender) Setup

To consume Avro-encoded messages:

1. Enable Schema Registry integration
2. Keep **Avro Value Deserialization** (`avroValueDeserialization`) enabled
3. Configure **Schema Registry URL** (`schemaRegistryUrl`) with your Schema Registry endpoint
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

With `avroOutputFormat` `XML`, a field whose text has a character that XML 1.0 does not allow (a control character other than tab, line feed and carriage return, U+FFFE or U+FFFF) is written as Base64 of its UTF-8 bytes and marked with `format="base64"`, for example `<note format="base64">eAF5</note>` for `x`, U+0001, `y`. Up to 1.4.1 such a field made the whole output unreadable as XML.

### When the Schema Registry Cannot Serve a Schema

The sender keeps every schema it has fetched, by its ID, until the integration flow stops. A record
with a schema ID it has not fetched yet needs the registry. If the registry does not answer (refused
connection, TLS failure, timeout) or answers `401`, `408`, `429` or a `5xx` status, the record is not
at fault: its partition waits at the record and retries it with the backoff described under
[Error Handling Without DLQ](dead-letter-queue.md#error-handling-without-dlq) (**Retry Delay**, at least
1 second, doubling up to 5 minutes). This holds with or without a DLQ and whatever *Error Handling*
says. Nothing at or after the record is committed meanwhile, every retry counts for **Auto-Pause on
Errors**, and other partitions keep flowing as long as their schemas are known. Once the registry
answers again, the partition continues on its own.

The trace shows `adapter.failure.reported … retryable=true` with the registry's answer, followed by
`consumer.partition.retry`. A rejected login (`401`) waits as well: correct the User Credentials
artifact, then restart the integration flow, because the sender reads the credential when it starts.

A record that is not in the Confluent wire format, or whose schema ID the registry does not know
(`404`), is still a poison pill: it goes to the DLQ as `DESERIALIZATION`, or *Error Handling* decides.
Up to 1.4.1 a registry outage was treated the same way, so every record with an unknown schema ID
went to the DLQ, or was skipped with *Skip Failed Message*.

## JSON Values with the Confluent Header

Confluent's JSON Schema serializer writes the same 5-byte header in front of every value as its Avro
serializer: the magic byte `0x00`, then the schema ID as a 4-byte big-endian number, then the JSON
text. The sender reads that header only on its Avro path. Behind the header a JSON value is no longer
JSON. Without the option below it reaches the integration flow with the header as its first
characters, a *JSON Array* batch embeds it as a string, an *XML List* batch writes it as Base64 (see
[Batch Processing](batch-processing.md#xml-list-xml_list)), and **JSON Schema Validation** rejects it.

From metadata version 1.5, the sender option **Remove Confluent Header** (`removeConfluentHeader`,
tab *Message Handling*) removes the header before the value is read as UTF-8 text:

| Value | What the integration flow gets |
|---|---|
| At least 5 bytes, the first one `0x00`, and exactly one JSON value after the header | The bytes after the header as UTF-8 text. For `{"id":"a"}` behind the header of schema ID 42, the bytes `00 00 00 00 2A 7B 22 …`, the body is `{"id":"a"}`. |
| The header, but no JSON after it, such as Avro or Protobuf from Confluent's serializers | The value unchanged, header included, and no `CpiKafkaPlusSchemaId`. The sender logs a warning once per consumer. For Avro, use **Enable Schema Registry** with **Deserialize Values as Avro** instead; Protobuf is not supported. |
| Anything else, such as plain JSON from another producer | The value unchanged. JSON text never starts with `0x00`, so the two cannot be confused. |

- With **Batch Mode** off, the header `CpiKafkaPlusSchemaId` (`Integer`) carries the schema ID of the
  removed header. It is not set for a value without the header, nor on a batch.
- A *JSON Array* batch embeds the JSON as JSON, not as a string, and an *XML List* batch writes it as
  text, not as Base64. **JSON Schema Validation** checks the JSON after the header against the schema
  in the channel.
- The Schema Registry is not called and the schema ID is not checked, so no registry connection is
  needed. Whether JSON follows the header is checked on the value itself: JSON that was already
  malformed when it was produced keeps its header as well, so **JSON Schema Validation** then reports
  the control characters of the header rather than the syntax error behind it.
- A record that goes to the Dead Letter Queue keeps its header: the DLQ gets the value as it was
  consumed.
- Keys are not changed. The receiver writes no Confluent header.

The option can only be edited while **Enable Schema Registry** is off, and is ignored while it is on:
the Avro deserialization reads the header itself. iFlows on metadata version 1.0 to 1.4 have no such
field; move them to 1.5 with *Update Version*.

## Producer (Receiver) Setup

To produce Avro-encoded messages:

1. Enable Schema Registry integration
2. Keep `avroValueSerialization` enabled
3. Configure **Schema Registry URL** (`schemaRegistryUrl`)
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

A receiver channel with Avro serialization and a strategy other than `TopicNameStrategy` does not start. The name is compared in any letter case, at deployment and for every message, so `topicnamestrategy` works as well.

## Authentication

Schema Registry credentials are stored in the CPI Secure Store. Create a User Credentials artifact with the Schema Registry API key and secret, then reference its alias in `schemaRegistryCredentialAlias`.

If *Schema Registry Credential Alias* is set but no User Credentials artifact of that name is deployed,
the channel fails with `'Schema Registry Credential Alias' (schemaRegistryCredentialAlias) '<alias>' could
not be resolved` (sender: at the first connect; receiver: at the first send). Before 1.4.1 it silently
connected without authentication. Leave the field empty for a registry without authentication. Outside
CPI, where no secure store exists, the alias is ignored as before.

!!! note
    Avro serialization/deserialization applies to message values only. Keys are always treated as strings.
