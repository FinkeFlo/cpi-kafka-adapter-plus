# Producer Batch Send

## Ueberblick

Der Producer kann einen Exchange-Body mit mehreren Records (JSON oder XML) entgegennehmen
und jeden als eigene Kafka-Message senden. Intern wird async send + flush verwendet
fuer maximalen Durchsatz.

**Kernprinzip:** Spiegelbildlich zum Consumer-Batching. Was der Consumer als `JSON_ARRAY`
oder `XML_LIST` ausgibt, kann der Producer direkt als Input verwenden.

## Konfiguration

| Parameter | Default | Beschreibung |
|-----------|---------|-------------|
| **Batch Send Mode** | `NONE` | `NONE` = 1 Exchange = 1 Kafka-Message. `JSON_ARRAY` = JSON-Array splitten. `XML_LIST` = XML kafkaRecords splitten. |

## Input-Formate

### JSON_ARRAY

```json
[
  {"key": "order-1", "value": {"id": 1, "amount": 99.90}},
  {"key": "order-2", "value": {"id": 2, "amount": 45.00}},
  {"value": "message without key"}
]
```

**Regeln:**
- Root muss ein JSON-Array sein
- Jedes Element muss mindestens `value` enthalten
- `key` ist optional (wenn fehlend: Fallback auf `kafka.KEY` Header, dann `null`)
- `value` als JSON-Objekt wird zu JSON-String serialisiert
- `value: null` erzeugt eine Kafka-Tombstone-Nachricht
- Unbekannte Felder (`topic`, `partition`, `offset`, `timestamp`) werden ignoriert

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

**Regeln:**
- Root-Element muss `<kafkaRecords>` sein
- Jedes `<record>` muss mindestens `<value>` enthalten
- `<key>` ist optional; fehlend = null Key, leer = expliziter Empty-String-Key
- `<value/>` (leer) erzeugt eine Tombstone-Nachricht
- Bei XML-Inhalt als Value: **CDATA verwenden** (empfohlen)
- Unbekannte Elemente werden ignoriert (Round-Trip-Symmetrie mit Consumer)

## Key-Handling

Prioritaet (von hoch nach niedrig):

1. `key` im Record (JSON-Feld oder XML-Element)
2. `kafka.KEY` Exchange-Header (Fallback)
3. Kein Key → `null` (Kafka round-robin Partitioning)

## Response

### Headers

| Header | Beschreibung |
|--------|-------------|
| `SAP_Receiver` | Topic-Name (MPL-Monitoring) |
| `CamelKafkaTopic` | Topic |
| `CpiKafkaPlusBatchRecordCount` | Anzahl gesendeter Records |
| `CpiKafkaPlusBatchInputFormat` | Konfigurierter Batch-Mode |
| `CpiKafkaPlusBatchFirstOffset` | Offset des ersten Records |
| `CpiKafkaPlusBatchLastOffset` | Offset des letzten Records |
| `CpiKafkaPlusBatchPartitions` | Beteiligte Partitionen (kommasepariert) |

### Body — XML-Summary

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

Bei Fehler bleibt der Body unveraendert (fuer Debugging).

## Fehlerbehandlung

- **Parsing-Fehler:** Klare Exception mit Fehlerbeschreibung und Record-Index → MPL-Error
- **Send-Fehler:** Fail-Fast bei erstem Fehler, bereits gesendete Records bleiben bei Kafka
- **Fatal-Fehler (Auth/Authorization):** Reconnect wird automatisch ausgeloest

## Einschraenkungen (v1)

- JSON Schema Validation wird im Batch-Mode uebersprungen (Warnung im Log)
- `kafka.PARTITION_KEY` und `kafka.OVERRIDE_TIMESTAMP` gelten fuer alle Records im Batch
- Exchange-Headers werden auf alle Kafka-Records kopiert (keine per-Record-Headers)
- Avro-Serialisierung ist im Batch-Mode nicht unterstuetzt (v1)

---

## CPI-Anleitung: Batch-Input erstellen

### JSON-Batch per Groovy-Script

Typischer Use-Case: Daten aus DB-Abfrage oder API als Batch an Kafka senden.

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

    message.setBody(new JsonBuilder(batch).toString())
    return message
}
```

### XML-Batch per XSLT-Mapping

Typischer Use-Case: SAP IDoc oder SOAP-Response mit Wiederholgruppen an Kafka senden.

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

### XML-Batch per Groovy (empfohlen fuer XML-Values)

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

### Hinweis: CDATA fuer XML-Values

Bei XML-Inhalt als Value immer CDATA verwenden:

- **XSLT:** `cdata-section-elements="value"` im `<xsl:output>`
- **Groovy:** `<![CDATA[...]]>` manuell einsetzen

Der XML-Parser uneskaped CDATA automatisch.
