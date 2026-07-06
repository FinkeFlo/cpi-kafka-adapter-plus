# Contributing to Kafka Adapter Plus

Thanks for your interest in contributing! This document explains how to build,
test and submit changes.

## Prerequisites

- **JDK 17** (the project builds with JDK 17 but targets **Java 8 bytecode** —
  see the constraint below)
- **Maven 3.8+**
- **Docker** (only needed for integration tests / `*IT.java`)

## Build & test

```bash
mvn clean install     # full build: OSGi bundle JAR + ESA archive
mvn test              # unit tests (JUnit 4 + Camel test support) — no broker needed
mvn verify            # full verification, matching CI (runs integration tests)
mvn test -Dtest=CpiKafkaPlusComponentTest#someMethod   # a single test method
```

Unit tests must pass **without** a running Kafka broker. Integration tests
(`*IT.java`) use Testcontainers and require Docker; they run in `mvn verify`.

## How to contribute

1. **Fork** the repository and create a topic branch off `main`.
2. Make your change with focused commits.
3. Ensure `mvn verify` passes locally.
4. Open a **pull request**. All CI checks must be green and the maintainer must
   approve before merging.

## Security

Never commit secrets, credentials, tenant names or other private/internal data.
Every pull request is scanned for secrets (gitleaks) in CI.

## Code conventions

- **Java 8 bytecode target.** Even though we compile with JDK 17, the CPI runtime
  requires Java 8 compatibility. **Do not use Java 9+ APIs** (`List.of`, `Map.of`,
  `var`, records, `String.isBlank`, etc.). The compiler will *not* warn you.
- 4-space indentation, UTF-8, LF line endings (see `.editorconfig`).
- No wildcard imports — explicit imports, Java stdlib first, then third-party.
- Utility/helper classes are `final` with a `private` constructor.
- Logging via SLF4J: `private static final Logger LOG = LoggerFactory.getLogger(...)`.
  Diagnostic logs use the `[CPI-KAFKA-PLUS-DIAG]` prefix.
- Kafka client creation must go through `BundleBackedClassLoader` (required for
  SASL/SSL under CPI's OSGi runtime).
- Tests use **JUnit 4** (`org.junit.Assert`), not JUnit 5.

### Adding an endpoint option

New `@UriParam` fields on `CpiKafkaPlusEndpoint` must also be added to
`src/main/resources/metadata/metadata.xml` in **two** places (an
`<AttributeReference>` and a top-level `<AttributeMetadata>`), plus a
getter/setter and a default-value assertion in `CpiKafkaPlusComponentTest`.

## License

By contributing, you agree that your contributions are licensed under the
GNU Affero General Public License v3.0 (AGPL-3.0), consistent with the rest of this project.
