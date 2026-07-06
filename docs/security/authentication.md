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

```
securityProtocol = SASL_SSL
saslMechanism = PLAIN
credentialAlias = MyKafkaCredentials
```

### SASL/SCRAM

For Kafka clusters using SCRAM authentication:

```
securityProtocol = SASL_SSL
saslMechanism = SCRAM-SHA-256
credentialAlias = MyKafkaCredentials
```

Supported SCRAM mechanisms: `SCRAM-SHA-256` and `SCRAM-SHA-512`.

## SSL/TLS (mTLS)

## CPI Secure Store

All credentials are managed through the SAP CPI Secure Store:

- **User Credentials**: Username/password pairs for SASL and Schema Registry authentication

Credential artifacts are referenced by their **alias** in the adapter configuration. The adapter resolves them at runtime via the `ITApiFactory`.

!!! warning
    Never hardcode credentials in IFlow configurations. Always use the CPI Secure Store.

## Recommendations

- Use `SASL_SSL` for managed Kafka services (Confluent Cloud, Amazon MSK)
- Use `SSL` (mTLS) when your organization requires certificate-based authentication
- Avoid `PLAINTEXT` and `SASL_PLAINTEXT` in production environments
