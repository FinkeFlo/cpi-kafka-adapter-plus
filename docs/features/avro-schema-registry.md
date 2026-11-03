# Avro / Schema Registry

The adapter integrates with Confluent Schema Registry for Avro serialization and deserialization. The
sender also reads JSON Schema values from the registry (see
[JSON Schema Values via the Schema Registry](#json-schema-values-via-the-schema-registry)).

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
| `avroValueDeserialization` | `true` | Deserialize message values via the Schema Registry when consuming: Avro and JSON Schema |
| `registrySchemaValidation` | `false` | Sender: validate JSON Schema values against their registry schema (metadata version 1.6) |
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

## JSON Schema Values via the Schema Registry

From 1.6, **Deserialize Values via Schema Registry** (`avroValueDeserialization`, up to metadata
version 1.5 labelled *Deserialize Values as Avro*) asks the registry for the `schemaType` of each
value's schema ID (`GET /schemas/ids/{id}`, fetched once per ID) and reads the value accordingly:

| `schemaType` | What the integration flow gets |
|---|---|
| `AVRO` (or none) | As before: the Avro record converted to JSON or XML (`avroOutputFormat`). |
| `JSON` | The bytes after the 5-byte header as UTF-8 text, unchanged. They must be valid UTF-8 and exactly one JSON value. `avroOutputFormat` `XML` does not apply; the sender logs that once per consumer as `consumer.config.ineffective parameter=avroOutputFormat schemaType=JSON`. |
| `PROTOBUF` or other | Nothing: the record fails as `DESERIALIZATION`. |

- One topic may mix Avro and JSON Schema values; each record is read with the type of its own
  schema ID.
- With **Batch Mode** off, the headers `CpiKafkaPlusSchemaId` (`Integer`) and `CpiKafkaPlusSchemaType`
  (`String`, `AVRO` or `JSON`) carry the schema ID and type, for Avro values as well.
- The runtime is shared by all metadata versions: a channel on 1.5 or older with the Schema Registry
  and *Deserialize Values as Avro* on reads JSON Schema values too. Up to 1.5 each such record was a
  poison pill, because its JSON schema failed to parse as Avro.
- A value that is not valid UTF-8 or not exactly one JSON value, an Avro value whose bytes do not fit
  its schema (such as a truncated one), or a schema type other than `AVRO` and `JSON`, is a poison pill
  with the code `KP-SR-002`: it goes to the DLQ as `DESERIALIZATION`, or
  *Error Handling* decides. The trace and the MPL carry `schemaId` and `schemaType`. A registry that
  cannot serve the schema is handled as described above, with `KP-SR-001`.

### Validating against the registry schema

From metadata version 1.6, **Validate with Registry Schema** (`registrySchemaValidation`, tab
*Avro / Schema Registry*, default off) validates each JSON Schema value against the schema registered
for its ID. A violation is handled like a failed **JSON Schema Validation**: reported with
`KP-CFG-002` (with **Report Validation Errors**), sent to the DLQ with the original bytes, header
included, or handled by *Error Handling*. With both validations on, a record that violates both is
handled once, with both messages; the registry's is prefixed `Registry schema <id>: `.

- The draft follows the schema's `$schema`: draft-04, draft-06, draft-07, 2019-09 or 2020-12. Without
  `$schema`, draft-07 applies, as for Confluent's serializer. Another `$schema` is a `KP-SR-002`
  failure of the record.
- The sender loads no other schema: a schema with `references` (to other registered subjects), or with
  a `$ref` to a remote or classpath URI, fails every record of that schema ID with `KP-SR-002` while
  validation is on. Without validation, references do not matter.
- Avro values are not validated by this option.
- With the option on but *Deserialize Values via Schema Registry* off, the sender logs
  `consumer.config.ineffective parameter=registrySchemaValidation` at start.
- Sender channels on metadata version 1.2 to 1.5 have no such field; move them to 1.6 with *Update Version*.
  Channels on 1.0 and 1.1 cannot use *Update Version* and have to be recreated.

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
| The header, but no JSON after it, such as Avro or Protobuf from Confluent's serializers | The value unchanged, header included, and no `CpiKafkaPlusSchemaId`. The sender logs this once per consumer as ERROR (`consumer.config.ineffective parameter=removeConfluentHeader`, see [Troubleshooting](../troubleshooting.md#consumerconfigineffective-and-consumerconfigclamped)). For Avro, use **Enable Schema Registry** with **Deserialize Values via Schema Registry** instead; Protobuf is not supported. |
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
the registry deserialization reads the header itself. With a registry at hand, prefer
[JSON Schema Values via the Schema Registry](#json-schema-values-via-the-schema-registry), which also
checks the value against its schema ID. iFlows on metadata version 1.0 to 1.4 have no such
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
