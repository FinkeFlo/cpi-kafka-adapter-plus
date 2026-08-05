# JSON Schema Validation

The adapter can validate Kafka message payloads against an inline JSON Schema (draft-07).

## Overview

When enabled, every message is checked against the configured schema before it is delivered. On the Consumer (Sender), this applies to incoming messages; on the Producer (Receiver), to outgoing messages. Messages that fail validation are either dropped or reported as errors in CPI monitoring, depending on `jsonSchemaReportError`.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `jsonSchemaValidation` | `false` | Enable JSON Schema validation of messages. |
| `jsonSchema` | — | Inline JSON Schema for message validation. |
| `jsonSchemaReportError` | `false` | Report JSON Schema validation failures as errors in CPI monitoring; otherwise invalid messages are dropped. |

## Schema Format

The `jsonSchema` field must contain exactly one JSON Schema document (draft-07), up to 50,000 characters (the field's UI length limit). To validate against more than one possible message shape, combine them into a single document using `oneOf`, `anyOf`, or `allOf`:

- `oneOf` - the message must match **exactly one** of the listed schemas.
- `anyOf` - the message must match **at least one** of the listed schemas.
- `allOf` - the message must match **all** of the listed schemas (useful for combining independent constraints into one document).

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
