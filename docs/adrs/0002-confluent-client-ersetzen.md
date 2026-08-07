# ADR 0002: Replace Confluent Schema Registry Client with a JDK-only Implementation

## Status
Proposed

## Context

The adapter currently embeds the Confluent Schema Registry client libraries
(`kafka-schema-registry-client`, `kafka-avro-serializer`, `kafka-schema-serializer`)
to serialize and deserialize Avro messages. These libraries pull in a significant
number of transitive dependencies that substantially increase the adapter size:

| Dependency (direct + transitive via Confluent) | Size |
|---|---|
| guava | 3.0 MB |
| snakeyaml | 0.3 MB |
| kafka-schema-registry-client | 0.3 MB |
| logredactor + logredactor-metrics | 0.04 MB |
| re2j | 0.1 MB |
| minimal-json | 0.03 MB |
| kafka-schema-serializer + jackson-dataformat-csv | 0.2 MB |
| kafka-avro-serializer + common-utils | 0.06 MB |
| **Total** | **~4 MB** |

The entire value of these libraries reduces to three HTTP operations
against the Confluent Schema Registry REST API:

1. `GET /schemas/ids/{id}` — fetch schema by ID (consumer / deserialization)
2. `GET /subjects/{subject}/versions/latest` — fetch latest schema (producer)
3. `POST /subjects/{subject}/versions` — register schema (producer, optional)

The Confluent wire format is publicly documented and trivial:
```
Byte 0:    0x00  (magic byte)
Bytes 1–4: schema ID (big-endian int)
Bytes 5+:  Avro binary payload
```

The actual Avro serialization (`Schema.Parser`, `GenericDatumReader/Writer`,
`JsonDecoder`/`BinaryDecoder`) is provided by the `org.apache.avro:avro` library,
which is **not affected** by this decision and remains embedded.

### Comparison with community adapters

Research into other open-source SAP CPI Kafka adapters shows that the same
Avro/Schema Registry feature can be implemented without any Confluent client
library — using only `java.net.HttpURLConnection` and the publicly documented
REST API. This confirms the approach is production-viable.

## Considered Options

### Option 1: Keep Confluent client (status quo)
- **Pro:** Battle-tested, full feature set (subject strategies, retry logic, SSL config)
- **Con:** ~4 MB of unnecessary transitive deps; Guava poses risks for the OSGi resolver

### Option 2: Custom `SchemaRegistryHttpClient` with JDK-only (chosen)
- `java.net.HttpURLConnection` / `HttpsURLConnection` for HTTP(S)
- Basic Auth via `Authorization: Basic` header
- Schema cache via `ConcurrentHashMap`
- No external dependencies
- **Pro:** ~4 MB savings; no Guava; clean, maintainable implementation
- **Con:** No built-in retry logic (can be added later); Basic Auth only (no OAuth)

### Option 3: Exclude Confluent dependencies, provide stubs
- Replace Confluent classes with empty stubs (similar to existing Kerberos/JGSS approach)
- **Con:** Complex and fragile; the Confluent object model is too deeply integrated

## Decision

**Option 2: Custom `SchemaRegistryHttpClient` with JDK-only.**

Rationale:
- The API surface used from the Confluent libraries is minimal (3 REST endpoints +
  wire format serialization). There is no justification for carrying ~4 MB of
  dependencies for this functionality.
- The `org.apache.avro:avro` library continues to handle the actual Avro parsing —
  the hard part is not reimplemented.
- `HttpURLConnection` supports HTTPS natively via the JVM truststore, matching
  the behaviour of the Confluent client.
- The `AvroSerializationIT` integration tests cover both paths (consumer + producer)
  against a real Schema Registry server and validate correctness.

## Consequences

- New class `SchemaRegistryHttpClient.java` in package `com.finkeflo.cpi.kafka`:
  - `fetchSchemaById(int id)` → `GET /schemas/ids/{id}`
  - `fetchSchemaBySubject(String subject)` → `GET /subjects/{subject}/versions/latest`
  - `getSchemaId(String subject)` → schema ID from subject metadata
  - `registerSchema(String subject, String schemaJson)` → `POST /subjects/{subject}/versions`
  - Schema cache via `ConcurrentHashMap<String, Schema>` and `ConcurrentHashMap<String, Integer>`
  - Basic Auth, HTTPS, configurable timeouts

- `AvroDeserializerHelper` and `AvroSerializerHelper` are updated to use the new client;
  all `io.confluent` imports are removed.

- `CredentialHelper.configureSchemaRegistryAuth()` is removed; the credential helper
  instead returns a `String[]{apiKey, apiSecret}` pair.

- Removed from `pom.xml`: `kafka-avro-serializer`, `kafka-schema-registry-client`,
  `kafka-schema-serializer`, `common-utils` (all `io.confluent`).

- Removed from `Embed-Dependency`: `guava`, `failureaccess`, `logredactor`,
  `logredactor-metrics`, `re2j`, `minimal-json`, `snakeyaml`, `jackson-dataformat-csv`.

- **Not supported** by this change (conscious limitation):
  - OAuth / Bearer Token authentication against Schema Registry (was not explicitly supported before either)
  - Automatic retry on Schema Registry timeouts (can be added in a follow-up)

- ESA size reduces by approximately 4 MB (~21 MB → ~17 MB).
  Combined with ADR 0003 (unpack kafka-clients), a target of ~14–15 MB is realistic.
