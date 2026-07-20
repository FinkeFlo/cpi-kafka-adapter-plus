# Producer Batch Send

## Overview

The producer can accept an exchange body containing multiple records (JSON or XML)
and send each one as its own Kafka message. Internally it uses async send + flush
for maximum throughput.

**Core principle:** Mirrors the consumer batching. Whatever the consumer outputs as
`JSON_ARRAY` or `XML_LIST` can be used directly as producer input.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| **Batch Send Mode** | `NONE` | `NONE` = 1 exchange = 1 Kafka message. `JSON_ARRAY` = split records from `kafkaRecords.record` (see Input formats). `XML_LIST` = split XML `kafkaRecords`. |

## Input formats

### JSON_ARRAY

```json
{
  "kafkaRecords": {
    "record": [
      {"key": "order-1", "value": {"id": 1, "amount": 99.90}},
      {"key": "order-2", "value": {"id": 2, "amount": 45.00}},
      {"value": "message without key"}
    ]
  }
}
```

This is exactly the shape the consumer produces in `JSON_ARRAY` mode — the extra
`record` level exists because the CPI standard JSON→XML converter rejects root
objects whose only member is a multi-element array, so a plain `[...]` root
doesn't round-trip cleanly through CPI's own converters.

**Rules:**
- Root must be `{"kafkaRecords": {"record": [...]}}`
- Each element in `record` must contain at least `value`
- `key` is optional (if missing: falls back to the `kafka.KEY` header, then `null`)
- A `value` given as a JSON object is serialized to a JSON string
- `value: null` produces a Kafka tombstone message
- Unknown fields (`topic`, `partition`, `offset`, `timestamp`) are ignored

**Also accepted (legacy/compatibility forms):**
- `{"kafkaRecords": [...]}` — wrapped array without the `record` level
- `[...]` — a bare JSON array as root

### XML_LIST

```xml
<kafkaRecords>
  <record>
    <key>order-1</key>
    <value>{"id": 1, "amount": 99.90}</value>
  </record>
  <record>
    <value><![CDATA[<order><id>2</id></order>]]></value>
  </record>
</kafkaRecords>
```

**Rules:**
- Root element must be `<kafkaRecords>`
- Each `<record>` must contain at least `<value>`
- `<key>` is optional; missing = null key, empty = explicit empty-string key
- An empty `<value/>` produces a tombstone message
- For XML content as a value: **use CDATA** (recommended)
- Unknown elements are ignored (round-trip symmetry with the consumer)

## Key handling

Priority (high to low):

1. `key` in the record (JSON field or XML element)
2. `kafka.KEY` exchange header (fallback)
3. No key → `null` (Kafka round-robin partitioning)

## Response

### Headers

| Header | Description |
|--------|-------------|
| `SAP_Receiver` | Topic name (MPL monitoring) |
| `CamelKafkaTopic` | Topic |
| `CpiKafkaPlusRecordCount` | Number of records sent |
| `CpiKafkaPlusBatchInputFormat` | Configured batch mode |
| `CpiKafkaPlusFirstOffset` | Offset of the first record |
| `CpiKafkaPlusLastOffset` | Offset of the last record |
| `CpiKafkaPlusPartitions` | Partitions involved (comma-separated) |

### Body — XML summary

```xml
<?xml version="1.0" encoding="UTF-8"?>
<kafkaBatchResult>
  <status>OK</status>
  <topic>orders</topic>
  <recordCount>150</recordCount>
  <firstOffset>1000</firstOffset>
  <lastOffset>1149</lastOffset>
  <partitions>0,1,2</partitions>
  <durationMs>23</durationMs>
</kafkaBatchResult>
```

On error, the body is left unchanged (for debugging).

## Error handling

- **Parsing error:** Clear exception with error description and record index → MPL error
- **Send error:** Fail-fast on the first error; records already sent remain in Kafka
- **Fatal error (auth/authorization):** Reconnect is triggered automatically

## Limitations (v1)

- JSON Schema validation is skipped in batch mode (logged as a warning)
- `kafka.PARTITION_KEY` and `kafka.OVERRIDE_TIMESTAMP` apply to all records in the batch
- Exchange headers are copied to all Kafka records (no per-record headers)
- Avro serialization is not supported in batch mode (v1)

---

## CPI guide: building batch input

### JSON batch via Groovy script

Typical use case: send data from a DB query or API call to Kafka as a batch.

```groovy
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

def Message processData(Message message) {
    def body = message.getBody(String)
    def orders = new JsonSlurper().parseText(body)

    def batch = orders.collect { order ->
        [key: order.orderId.toString(), value: order]
    }

    message.setBody(new JsonBuilder([kafkaRecords: [record: batch]]).toString())
    return message
}
```

### XML batch via XSLT mapping

Typical use case: send an SAP IDoc or SOAP response with repeating groups to Kafka.

```xslt
<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

  <xsl:output method="xml" encoding="UTF-8" indent="yes"
              cdata-section-elements="value"/>

  <xsl:template match="/">
    <kafkaRecords>
      <xsl:for-each select="/Orders/Order">
        <record>
          <key><xsl:value-of select="OrderId"/></key>
          <value><xsl:value-of select="."/></value>
        </record>
      </xsl:for-each>
    </kafkaRecords>
  </xsl:template>
</xsl:stylesheet>
```

### XML batch via Groovy (recommended for XML values)

```groovy
import com.sap.gateway.ip.core.customdev.util.Message

def Message processData(Message message) {
    def body = message.getBody(String)
    def orders = new XmlSlurper().parseText(body)

    def sb = new StringBuilder()
    sb.append('<?xml version="1.0" encoding="UTF-8"?>\n')
    sb.append('<kafkaRecords>\n')

    orders.Order.each { order ->
        sb.append('  <record>\n')
        sb.append("    <key>${order.OrderId}</key>\n")
        sb.append("    <value><![CDATA[${groovy.xml.XmlUtil.serialize(order)}]]></value>\n")
        sb.append('  </record>\n')
    }

    sb.append('</kafkaRecords>')
    message.setBody(sb.toString())
    return message
}
```

### Note: CDATA for XML values

Always use CDATA for XML content used as a value:

- **XSLT:** `cdata-section-elements="value"` in `<xsl:output>`
- **Groovy:** insert `<![CDATA[...]]>` manually

The XML parser automatically un-escapes CDATA.
