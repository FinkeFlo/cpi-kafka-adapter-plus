# Authentication

The adapter supports multiple security protocols for connecting to Kafka brokers.

## Security Protocols

The protocol value encodes **both** the transport and the authentication method — there is no
separate "use TLS" switch, because TLS is part of the protocol itself. `SASL_SSL` *is* SASL over
TLS.

| Protocol (UI label) | TLS | Authentication | When to use |
|---|---|---|---|
| `SASL_SSL (SASL over TLS)` | yes | Username/password via SASL | Default and the right choice for almost every broker, including all managed services |
| `SSL (TLS, certificate authentication)` | yes | Client certificate (mTLS) | Brokers that authenticate clients by certificate instead of credentials |
| `SASL_PLAINTEXT (no TLS)` | **no** | Username/password via SASL | Local or test brokers on a trusted network only |
| `PLAINTEXT (no TLS, no authentication)` | **no** | none | Local development only |

!!! warning "Managed brokers accept TLS only"
    Confluent Cloud, Amazon MSK and comparable services serve TLS exclusively. A client
    configured `SASL_PLAINTEXT` or `PLAINTEXT` against such a broker never completes a
    connection — and never sees a handshake error either, because the broker simply drops it.
    The only symptom Kafka produces is a metadata timeout. See
    [Troubleshooting](#troubleshooting-topic-not-present-in-metadata) below.

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

!!! note "Kerberos / GSSAPI is not supported"
    Only `PLAIN`, `SCRAM-SHA-256` and `SCRAM-SHA-512` are supported. SASL/GSSAPI
    (Kerberos) is intentionally **not** available: CPI's OSGi runtime does not export
    the `org.ietf.jgss` package, so the adapter ships empty stubs for it purely to let
    the Kafka client resolve and start. These stubs are never invoked at runtime — a
    GSSAPI login would fail. Use `PLAIN`, `SCRAM`, or mTLS instead.

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

- If `sslKeystoreAlias` is **empty**, the Kafka client uses the JVM default
  truststore, which already trusts the public certificate authorities. This is the
  right choice for publicly trusted brokers such as Confluent Cloud — and note that
  **TLS is still fully active**: leaving the alias empty does not weaken the
  connection, it only means no custom trust material is needed.
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

## Troubleshooting: "Topic not present in metadata"

Kafka reports several unrelated problems with the same message:

```
org.apache.kafka.common.errors.TimeoutException:
Topic MY_TOPIC not present in metadata after 60000 ms
```

It means only that the client never received usable metadata for the topic. It does *not* say
whether the topic is missing, the broker is unreachable, or the connection was rejected. The
adapter therefore probes the broker before every send and turns that probe's result into a
message that names the actual cause.

**The topic does not exist**

```
Kafka topic 'MY_TOPIC' does not exist on the broker. Please create the topic before
sending to it (auto-create may be disabled on this cluster).
```

Create the topic. Most managed clusters run with `auto.create.topics.enable=false`, so a typo in
the topic name produces exactly this. A topic created afterwards is picked up on the next
message — no redeployment needed. The same check runs at deployment time and writes a warning to
the deployment log, so a wrong topic name is visible before the first message flows.

!!! warning "The adapter does not rely on broker-side auto-create"
    A send to a topic that does not exist is rejected, even on a cluster where
    `auto.create.topics.enable` is switched on. Create topics explicitly. A topic that was only
    just created is not affected — a missing topic is re-checked briefly before the send fails, so
    creating a topic and sending to it right away keeps working.

**Authentication was rejected or the TLS handshake failed**

```
Cannot connect to Kafka broker 'my-cluster:9092' for topic 'MY_TOPIC':
SaslAuthenticationException: Authentication failed: Invalid username or password.
Please check Security Protocol, Credential Alias and SSL Keystore Alias.
```

Reported immediately rather than after a timeout, because the same credentials or certificates
would be rejected again. Check the User Credentials artifact behind `credentialAlias`, and — if
`sslKeystoreAlias` is set — whether that keystore really contains the broker's CA.

**The security protocol does not match the broker's listener**

```
Failed to send message to Kafka topic 'MY_TOPIC': Topic MY_TOPIC not present in metadata
after 30000 ms. The broker could not be reached during the pre-send check either
(TimeoutException: ...). securityProtocol=SASL_PLAINTEXT does not use TLS. Managed brokers
such as Confluent Cloud accept TLS connections only — if the broker is reachable but never
answers, switch the Security Protocol to SASL_SSL (or SSL).
```

Set **Security Protocol** to `SASL_SSL (SASL over TLS)` and leave **SSL Keystore Alias** empty
for a broker with a publicly trusted certificate. This case cannot be reported as a handshake
error, because a client without TLS never gets far enough to negotiate one — which is why the
protocol is named as the likely cause instead.

!!! note "Why the send still waits when only a timeout is involved"
    A probe that merely times out is not conclusive: the broker may have hiccupped for a moment
    while the send still succeeds within `max.block.ms`. The adapter therefore does not abort the
    exchange in that case — it remembers why the probe failed and folds that cause into the send
    error if the send does go on to fail. Rejected authentication and failed TLS handshakes are
    different: those are definitive and fail the exchange straight away.
