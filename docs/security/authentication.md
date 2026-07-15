# Authentication

The adapter supports multiple security protocols for connecting to Kafka brokers.

## Security Protocols

| Protocol | Description |
|----------|-------------|
| `SASL_SSL` | SASL authentication over SSL/TLS (default, recommended) |
| `SASL_PLAINTEXT` | SASL authentication without encryption |
| `SSL` | SSL/TLS with client certificate (mTLS) |
| `PLAINTEXT` | No authentication or encryption |

## SASL Authentication

### SASL/PLAIN

The most common setup for managed Kafka services (Confluent Cloud, MSK, etc.):

1. Create a **User Credentials** artifact in CPI Secure Store with your Kafka API key and secret
2. Configure the adapter:

| Parameter | Value |
|---|---|
| `securityProtocol` | `SASL_SSL` |
| `saslMechanism` | `PLAIN` |
| `credentialAlias` | `MyKafkaCredentials` |

### SASL/SCRAM

For Kafka clusters using SCRAM authentication:

| Parameter | Value |
|---|---|
| `securityProtocol` | `SASL_SSL` |
| `saslMechanism` | `SCRAM-SHA-256` |
| `credentialAlias` | `MyKafkaCredentials` |

Supported SCRAM mechanisms: `SCRAM-SHA-256` and `SCRAM-SHA-512`.

## SSL/TLS and mTLS

For broker connections that need a private/custom CA and/or client certificate
authentication, configure a CPI keystore alias:

| Parameter | Value |
|---|---|
| `securityProtocol` | `SSL` |
| `sslKeystoreAlias` | `MyKafkaKeystore` |

Or, when SASL and custom TLS material are both required:

| Parameter | Value |
|---|---|
| `securityProtocol` | `SASL_SSL` |
| `saslMechanism` | `PLAIN` |
| `credentialAlias` | `MyKafkaCredentials` |
| `sslKeystoreAlias` | `MyKafkaKeystore` |

How it works at runtime:

- If `sslKeystoreAlias` is **empty**, the Kafka client behaves as before and uses
  the JVM default truststore. This is the right choice for publicly trusted
  brokers such as Confluent Cloud.
- If `sslKeystoreAlias` is **set**, the adapter creates a Kafka `SslEngineFactory`
  backed by CPI's `KeystoreService`.
- CPI trust managers are used for custom/private CA validation.
- If the configured alias also contains a client keypair, the same setup enables
  mutual TLS (mTLS).

## CPI Secure Store

All credentials are managed through the SAP CPI Secure Store:

- **User Credentials**: Username/password pairs for SASL and Schema Registry authentication
- **Keystore entries**: Private keys/certificates and trusted CA certificates for SSL/TLS

Credential artifacts are referenced by their **alias** in the adapter configuration. The adapter resolves them at runtime via the `ITApiFactory`.
