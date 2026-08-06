# ADR 0003: Unpack kafka-clients as Extracted Classes Instead of a Nested JAR

## Status
Proposed

## Context

The adapter uses `maven-bundle-plugin` with `Embed-Dependency` to embed all
dependencies as nested JARs inside the bundle JAR. This technique has a significant
downside for ESA size:

A ZIP-of-ZIPs (ESA contains JAR, JAR contains JARs) compresses poorly because
the inner JARs are already ZIP-compressed. The outer compression achieves almost
no savings on already-compressed data.

The largest single dependency is `kafka-clients` at 9.7 MB (uncompressed).
As a nested JAR in the ESA it occupies virtually the full 9.7 MB.
If the classes were extracted and then zipped into the ESA, uncompressed `.class`
files would compress significantly better.

### Comparison with community adapters

Research into other open-source SAP CPI Kafka adapters confirms that unpacking
`kafka-clients` directly into the bundle — rather than embedding it as a nested
JAR — is a production-proven packaging approach that achieves significantly
better compression.

## Considered Options

### Option 1: Unpack all dependencies

Extract all embedded dependencies with `unpack` into `target/classes` and
use `Private-Package` instead of `Embed-Dependency` in the bundle plugin.

- **Pro:** Maximum compression savings (~5 MB)
- **Con:**
  - `snappy-java`, `lz4-java`, `zstd-jni` contain native libraries (`.so`/`.dll`).
    These are no longer found by the OSGi ClassLoader when unpacked.
  - `META-INF/services` entries (ServiceLoader) from Kafka, Avro, and Jackson
    overwrite each other → broken runtime behaviour.
  - Too risky without significant additional effort (manual merge logic).

### Option 2: Unpack only `kafka-clients` (chosen)

Extract only `kafka-clients`; keep all other dependencies as nested JARs.

- **Pro:**
  - ~2–3 MB savings from better compression of Kafka classes
  - No risk to native libraries or ServiceLoader conflicts
  - Proven approach: validated against community research on other open-source CPI adapters
  - `kafka-clients` contains no native libraries and has clean,
    non-conflicting `META-INF/services` entries
- **Con:** Lower savings than Option 1

### Option 3: Status quo (Embed-Dependency for all)

- **Con:** Unnecessarily large ESA; no technical justification

## Decision

**Option 2: Unpack only `kafka-clients`.**

Rationale:
- The risk/benefit ratio of Option 1 is unfavourable. The native library problem
  with `snappy-java`/`lz4-java`/`zstd-jni` and the ServiceLoader conflicts are
  real runtime failures that would be difficult to debug.
- Option 2 is the proven approach from the reference adapter and achieves a
  substantial saving without these risks.
- `kafka-clients` is the largest single dependency (9.7 MB) and therefore the
  biggest lever when unpacking.

## Consequences

- `maven-dependency-plugin:unpack` extracts `kafka-clients` into `target/classes`,
  excluding the following files:
  - `META-INF/MANIFEST.MF`, `META-INF/*.SF`, `META-INF/*.DSA`, `META-INF/*.RSA`
  - `META-INF/INDEX.LIST`, `META-INF/maven/**`, `module-info.class`
  - `META-INF/versions/**` (Multi-Release JAR — not needed for Java 11 target)
  - `org/apache/kafka/common/security/kerberos/KerberosError.class`
    (Kerberos stub already present in the adapter)

- `kafka-clients` remains as a `compile` dependency in `pom.xml` (still needed
  for the compiler classpath) but is removed from the `Embed-Dependency` list
  in the bundle plugin.

- All other dependencies (`avro`, `jackson-*`, `snappy-java`, `lz4-java`,
  `zstd-jni`, `json-schema-validator`, etc.) remain as nested JARs via
  `Embed-Dependency`.

- **Not unpacked** (conscious limitation):
  - `snappy-java`, `lz4-java`, `zstd-jni` — contain native libraries
  - `avro`, `jackson-*`, `json-schema-validator` — have ServiceLoader entries

- ESA size reduces by approximately 2–3 MB (~17 MB → ~14–15 MB).
  Prerequisite: ADR 0002 (remove Confluent client) is implemented first,
  as its ~4 MB saving establishes the ~17 MB baseline.
