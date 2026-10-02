# JSON Schema Validation

The adapter can validate Kafka message payloads against an inline JSON Schema (draft-07).

## Overview

When **JSON Schema Validation** (`jsonSchemaValidation`) is enabled, every message is checked against the configured schema before it is delivered. On the Consumer (Sender), this applies to incoming messages; on the Producer (Receiver), to outgoing messages. What happens to a message that fails validation depends on the direction:

| Direction | Invalid message | `jsonSchemaReportError=true` adds |
|-----------|-----------------|-----------------------------------|
| Sender (consumer) | Never reaches the IFlow. With a DLQ it is written there; without one it is discarded and its offset committed. **Error Handling** (Retry / Skip) does not apply, because an invalid message stays invalid. Without the flag, only a WARN log line is written, which does not reach the tenant trace in production — the message disappears without a visible trace. | A failed MPL entry with the validation error, visible in CPI monitoring; the payload is added to its trace if trace is active. |
| Receiver (producer) | Always fails the exchange; the message is not sent. Validation applies to **Batch Send Mode** `NONE` only — with `JSON_ARRAY` or `XML_LIST` it is skipped and a warning is logged. | The rejected payload in the MPL trace, if trace is active for the integration flow. The exchange fails either way. |

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `jsonSchemaValidation` | `false` | Enable JSON Schema validation of messages. |
| `jsonSchema` | — | Inline JSON Schema for message validation. |
| `jsonSchemaReportError` | `false` | Sender: also write a failed MPL entry for every invalid message. Receiver: also write the rejected payload to the MPL trace; the exchange fails regardless. See the table above. |

## Schema Format

The `jsonSchema` field must contain exactly one JSON Schema document (draft-07), up to 50,000 characters (the field's UI length limit). To validate against more than one possible message shape, combine them into a single document using `oneOf`, `anyOf`, or `allOf`:

### `oneOf` - exactly one of the listed schemas must match

```json
{
  "oneOf": [
    {
      "type": "object",
      "required": ["orderId", "amount"],
      "properties": {
        "orderId": { "type": "string" },
        "amount": { "type": "number" }
      }
    },
    {
      "type": "object",
      "required": ["invoiceId", "total"],
      "properties": {
        "invoiceId": { "type": "string" },
        "total": { "type": "number" }
      }
    }
  ]
}
```

A message that matches both branches (or neither) fails validation.

### `anyOf` - at least one of the listed schemas must match

```json
{
  "anyOf": [
    {
      "type": "object",
      "required": ["email"],
      "properties": {
        "email": { "type": "string", "format": "email" }
      }
    },
    {
      "type": "object",
      "required": ["phone"],
      "properties": {
        "phone": { "type": "string" }
      }
    }
  ]
}
```

A message with `email`, `phone`, or both passes; a message with neither fails.

### `allOf` - all of the listed schemas must match

```json
{
  "allOf": [
    {
      "type": "object",
      "required": ["id"],
      "properties": {
        "id": { "type": "string" }
      }
    },
    {
      "type": "object",
      "required": ["amount"],
      "properties": {
        "amount": { "type": "number", "minimum": 0 }
      }
    }
  ]
}
```

Useful for combining independent constraint fragments into one document - here, every message must have both an `id` and a non-negative `amount`.
