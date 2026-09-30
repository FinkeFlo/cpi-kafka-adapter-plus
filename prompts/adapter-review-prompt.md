# Review-Prompt: Kafka Adapter Plus (SAP Cloud Integration)

**Zweck:** Dieser Prompt lässt ein KI-Modell (z. B. Claude Code im Repo-Root) den Adapter
systematisch prüfen: Code-Robustheit, Parameter-Usability und – der Schwerpunkt – die Lücke
zwischen *„die UI akzeptiert es“* und *„das Deployment schlägt fehl“*.

**Benutzung:** Alles unterhalb der Linie `=== PROMPT ===` in eine neue Session im Repo-Root
einfügen. Der Prompt ist bewusst read-only (kein Edit, kein Commit, kein PR).
Stand der Vorab-Befunde: Adapter-Version 1.3.6, Branch `main` (Commit `e31faa8`).

=== PROMPT ===

# Rolle

Du bist ein erfahrener Reviewer für SAP-Cloud-Integration-Custom-Adapter (ADK, Apache Camel 3.14,
OSGi) und für Apache-Kafka-Clients. Du prüfst **kritisch und belegbar**, nicht wohlwollend.

# Auftrag

Reviewe den Adapter in diesem Repository (`cpi-kafka-adapter-plus`, Kafka Sender + Receiver für SAP
Cloud Integration). Drei Fragen sind zu beantworten:

1. **Robustheit:** Wo ist der Code nicht robust genug (Datenverlust, Duplikate, Hänger,
   Ressourcen-Leaks, Sicherheit, Fehlersichtbarkeit)? Was gehört robuster gemacht?
2. **Parameter-Usability:** Die Parameter wirken komplex und voneinander abhängig. Wo ist die
   Konfiguration unverständlich, widersprüchlich oder gefährlich vorbelegt?
3. **Validierungslücke (Schwerpunkt):** Eine falsche Konfiguration wird oft **nicht in der
   CPI-Design-UI** angezeigt, sondern erst beim **Deployment** – oder noch später (erster Poll /
   erster Send). Für **jede** Regel: Wo wird sie heute geprüft, wo *könnte* sie geprüft werden, und
   was muss sich dafür ändern?

# Harte Regeln

- **Read-only.** Keine Dateien im Repo ändern, nichts committen, keinen PR anlegen. Dein Ergebnis
  ist ein Bericht (siehe „Ausgabeformat“).
- **Belegpflicht.** Jede Aussage über Code braucht `Datei:Zeile` und – bei Verhalten – das zitierte
  Code-Fragment. Was du nicht verifiziert hast, markierst du als `UNVERIFIZIERT`. Nichts raten.
- **Vorab-Befunde sind Hypothesen.** Der Abschnitt „Vorab-Befunde“ stammt aus einer ersten
  Durchsicht. Prüfe jeden einzeln nach: `BESTÄTIGT`, `WIDERLEGT` (mit Begründung) oder
  `TEILWEISE`. Übernimm nichts ungeprüft.
- **Build nicht vorausgesetzt.** Die SAP-ADK-Artefakte (`com.sap.cloud.adk`, Version 2.3.0 in
  `pom.xml`) liegen nicht in jeder Umgebung im Maven-Repo. Behaupte nicht, dass du Build oder Tests
  laufen ließest, wenn du es nicht getan hast. Statische Analyse ist ausreichend.
- **Keine Tenant-Annahmen ohne Kennzeichnung.** Wie die CPI-Design-UI ein Feld tatsächlich prüft,
  lässt sich ohne Tenant nicht testen. Kennzeichne solche Aussagen als `TENANT-TEST NÖTIG` und
  formuliere den konkreten Testschritt.
- **Versionierungsregeln beachten** (`VERSIONING.md`). Jeder Verbesserungsvorschlag an den
  Metadaten muss einer Kategorie zugeordnet werden: *Micro* (In-place-Edit der aktuellen
  Variantendatei, nur Labels/Tooltips/kleine Korrekturen), *Minor* (neue Variantendatei
  `metadata-<sender|receiver>-<neu>.xml`, Alt-iFlows müssen „Update Version“ klicken) oder *Major*.
  **Freigegebene (frozen) Metadaten-Dateien** (`metadata-*-1.0.0/1.1.x/1.2.8.xml`, siehe
  `src/test/resources/released-metadata-checksums.txt`) dürfen nie geändert werden – Vorschläge
  dafür sind wertlos.
- **Quellen.** Externe Quellen nennst du mit URL und Abrufstatus. Wenn eine Seite (z. B.
  `community.sap.com`, `help.sap.com`) nicht erreichbar ist, schreibe das hin und markiere die
  Aussage als `UNVERIFIZIERT` – ersetze sie nicht durch Erinnerung.

# Kontext, den du kennen musst

**Architektur** (`README.md`, `docs/adrs/`): Camel-Komponente `cpi-kafka-plus`
(`CpiKafkaPlusComponent` → `CpiKafkaPlusEndpoint` → `CpiKafkaPlusConsumer` = Sender / Kafka→CPI,
`CpiKafkaPlusProducer` = Receiver / CPI→Kafka). Ein OSGi-Fat-Bundle mit entpacktem `kafka-clients`
4.3.1 (ADR 0003), Class-Space-Warmup gegen Adapter-Updates (ADR 0005), Kompilierung auf Java 11.

**Wie die Konfiguration entsteht:** Die CPI-Design-UI wird ausschließlich aus
`src/main/resources/metadata/metadata-{sender,receiver}-<version>.xml` erzeugt. Pro Parameter gibt
es eine `<AttributeReference>` (Tab/Gruppe, Tooltip, `EditCondition`, `Restriction`,
`ErrorMessage`) und eine `<AttributeMetadata>` (Typ, Default, `FixedValues`, `Usage`). Die
Java-Seite liest die Werte als `@UriParam` in `CpiKafkaPlusEndpoint`.

**Bereits belegte Mechanik der UI-Validierung** (Issue #44, Kommentar des Maintainers):
Die Design-UI prüft ein `AttributeReference`-Feld nur dann, wenn ein
`<Restriction>Constraint.isValidRegex(...)</Restriction>` gesetzt ist. `<Usage>true</Usage>` prüft
nur „nicht leer“. Ein `<ErrorMessage>` ohne `Restriction` hat daher vermutlich **keine Wirkung**.
Nur zwei Stellen nutzen heute eine `Restriction`: `pollingIntervalSeconds` (Sender) und
`credentialAlias` (beide Richtungen). Cross-Field-Regeln sind mit `Restriction` nicht ausdrückbar;
`EditCondition`/`OrCondition` blenden Felder nur ein/aus bzw. machen sie editierbar/ausgegraut.
→ Prüfe dies gegen die offizielle ADK-Dokumentation/das Metadaten-XSD und erweitere es, falls es
weitere Constraint-Typen gibt (z. B. für Zahlen, Pflichtfelder in Abhängigkeit).

**Laufzeit-Besonderheiten von CPI** (`docs/troubleshooting.md`, ADR 0004): Nur `ERROR` erreicht
das Tenant-Trace-File; `WARN`/`INFO` sind in Produktion unsichtbar. Der Trace-Appender verwirft
den `Throwable`. Deshalb loggt der Adapter Nicht-Fehler teils auf `ERROR` – bewerte diesen
Kompromiss, statt ihn zu ignorieren.

**Lazy Init:** Consumer (`CpiKafkaPlusConsumer.java:289`) und Producer
(`CpiKafkaPlusProducer.java:264`) bauen den Kafka-Client erst beim ersten Poll bzw. ersten Send.
Ein Deployment kann also „gestartet“ melden, obwohl Verbindung/Credentials nie funktionieren können.
SAPs Standard-Kafka-Adapter schlägt dagegen bei nicht auflösbaren Bootstrap-Servern schon beim
Deployment fehl (SAP KBA 3294904, `UNVERIFIZIERT`, nur aus Suchtreffer bekannt).

# Arbeitspakete

## A. Parameter-Inventur und Abhängigkeitsmatrix

Erzeuge eine Tabelle über **alle** Parameter beider Richtungen (Quelle:
`metadata-sender-1.3.0.xml`, `metadata-receiver-1.3.0.xml`, `CpiKafkaPlusEndpoint.java`,
`docs/configuration.md`). Spalten:

`Parameter | Richtung | Tab/Gruppe | UI-Label | Typ | Default (Java) | Default (Metadaten) | Default (Doku) |
Usage/Pflicht | FixedValues | EditCondition | Restriction | ErrorMessage wirkt? | Prüfung beim Start
(Datei:Zeile) | Prüfung erst bei Poll/Send | Abhängig von`

Markiere jede **Abweichung** zwischen Java-, Metadaten- und Doku-Default sowie jede Stelle, an der
Tooltip, Java-`@UriParam`-Beschreibung und Doku sich widersprechen. Leite daraus eine
**Abhängigkeitsmatrix** ab (welcher Parameter ist nur sinnvoll/zulässig, wenn welcher andere wie
gesetzt ist) und benenne jede Kombination, die die UI zulässt, der Start aber ablehnt.

## B. Validierungs-Lückenanalyse (Design-Time / Deployment / Laufzeit)

Ordne **jede** Regel genau einer Stufe zu und beantworte für jede, ob sie eine Stufe früher
möglich wäre:

| Stufe | Bedeutet |
|---|---|
| D0 Design-UI | Feld wird beim Bearbeiten/Speichern des Channels abgelehnt |
| D1 Deployment | `doStart()` wirft → iFlow-Deployment schlägt fehl |
| D2 Erster Poll/Send | Fehler erst im laufenden Betrieb, Deployment meldet „Started“ |
| D3 Still | Keine Fehlermeldung, Verhalten weicht nur vom Erwarteten ab |

Prüfe insbesondere:

1. **Einzelfeld-Regeln → `Restriction`/`FixedValues`:** Zahlenbereiche, positive Ganzzahlen,
   Pflichtfelder in Abhängigkeit. Formuliere konkrete `Constraint.isValidRegex`-Ausdrücke
   (XML-escaped) samt Testfällen. Kläre: Verträgt die Regex leere optionale Felder? Wird sie nur
   erzwungen, wenn das Feld sichtbar/editierbar ist? Wie verhält sie sich bei externalisierten
   Werten (`{{param}}`; die Felder sind `isparameterized=true`)? → ggf. `TENANT-TEST NÖTIG`.
2. **Kleine Wertebereiche → `FixedValues` (Dropdown) statt freiem Zahlenfeld** (z. B.
   `producerRetryMaxAttempts` 1–5). Vorteil: ungültige Werte unmöglich, und `EditCondition` kann auf
   konkrete Werte reagieren (heute nur Gleichheit auf einen Literalwert).
3. **Cross-Field-Regeln** (nicht per Metadaten prüfbar): Welche Alternativen gibt es?
   (a) Defaults so wählen, dass jede Ein-Schalter-Änderung deploybar bleibt, (b) abgeleitete statt
   eingegebene Werte (z. B. Retry-Budget aus `deliveryTimeoutSeconds` und `maxAttempts` berechnen),
   (c) Tooltips mit konkreter Rechnung/Beispielwerten, (d) verständlichere Start-Fehlermeldung mit
   Sollwert-Vorschlag, (e) Profile/Presets.
4. **Stale-Werte in ausgeblendeten Feldern:** Felder mit `EditCondition` behalten ihren Wert, wenn
   sie ausgeblendet werden. Prüfe für jede Start-Validierung, ob sie *unbedingt* läuft und damit ein
   **unsichtbares** Feld das Deployment blockieren kann. (Die Streaming-Sonderfälle in
   `CpiKafkaPlusConsumer.doStartInternal` zeigen, dass das Problem bekannt ist – ist es überall
   gelöst?)
5. **Lazy-Init-Fehler, die ohne Netzwerk erkennbar wären:** Existiert der Credential-Alias im
   Secure Store? Existiert der Keystore-Alias? Ist die Bootstrap-Liste syntaktisch `host:port`? Ist
   die Schema-Registry-URL eine gültige URL? Ist `groupId` gesetzt? Welche davon kann man in
   `doStart()` prüfen, ohne den Start zu blockieren (Netzwerk-Checks bleiben lazy)? Wie sieht der
   Betreiber den Fehler heute (Status, MPL, Trace)?
6. **Unbekannte Enum-/Stringwerte** (z. B. `securityProtocol`, `saslMechanism`, `autoOffsetReset`,
   `commitStrategy`, `acks`, `compressionType`, `producerBatchMode`, `batchOutputFormat`,
   `avroOutputFormat`, `subjectNameStrategy`): Was passiert bei Tippfehler, Groß-/Kleinschreibung,
   externalisiertem oder migriertem Wert? Wird ein Default still unterstellt?

## C. Usability-Review der Oberfläche

Bewerte Tabs, Gruppen, Labels, Tooltips, Defaults und Sichtbarkeitslogik aus Sicht eines
Integrationsentwicklers, der Kafka nur grob kennt:

- Verständlichkeit: Sind Label und Parametername irreführend (z. B. `batchTimeout` heißt in der UI
  „Poll Timeout (ms)“ und ist kein Batch-Füll-Timer)? Gibt es uneinheitliche Begriffe für dasselbe?
- Sichtbarkeit: Werden abhängige Felder konsequent ein-/ausgeblendet? Werden irrelevante Felder
  angezeigt (z. B. `saslMechanism` bei `SSL`/`PLAINTEXT`)?
- Gefährliche oder unpassende Defaults (z. B. `allowedHeaders=*`, `commitStrategy=AUTO` ohne
  Warnhinweis, `autoOffsetReset=latest`, Retry-/Timeout-Defaults, die sich gegenseitig
  ausschließen).
- Semantik-Fallen: Parameter, die je nach Richtung oder Modus etwas anderes bedeuten, oder deren
  Wirkung vom Namen abweicht (vgl. `jsonSchemaReportError`).
- **Golden Paths:** Gib für mindestens diese Szenarien eine *minimale, deploybare* Konfiguration an
  und prüfe, ob die UI den Nutzer dorthin führt: (1) Confluent Cloud SASL_SSL/PLAIN, (2)
  Self-managed SCRAM-SHA-512 mit privater CA, (3) mTLS, (4) Receiver mit Transaktionen und Retry,
  (5) Sender mit DLQ + Auto-Pause, (6) Avro mit Schema Registry. Offene Anforderung: Issue #105
  (Cloud Connector / On-Premise).
- Schlage eine **bessere Struktur** vor (Reihenfolge, Gruppierung, „Basis“ vs. „Erweitert“,
  Presets), ohne gegen die Versionierungsregeln zu verstoßen.

## D. Code-Robustheit

Prüfe mindestens diese Bereiche; jede Schwäche mit Szenario, Auswirkung und Fix:

1. **Sender-Fehlerpfad:** Verhalten bei Verarbeitungsfehler *ohne* DLQ (Endlosschleife? Offset-Commit?
   Poison Pill blockiert Partition?), `commitStrategy=AUTO` im Zusammenspiel mit Retry/DLQ/Drain,
   Verhalten bei JSON-Schema-Verstoß mit `jsonSchemaReportError=false` (still verworfen *und*
   committed?), DLQ-Topic identisch zum Quell-Topic, Rebalance-Handling, Commit auf entzogenen
   Partitionen, `max.poll.interval.ms` gegenüber Verarbeitungszeit × Retries × Backoff.
2. **Receiver-Fehlerpfad:** Retry-Entscheidungsbaum (`ProducerRetryPolicy`), Duplikatgarantien,
   Transaktionen (`transactional.id`-Ableitung, Fencing, Slot-Semaphore), wiederverwendeter vs.
   neu gebauter Producer, blockierende Aufrufe auf CPI-Worker-Threads, Wechselwirkung mit dem
   HTTP-Timeout des Aufrufers.
3. **Sicherheit:** Standard-`allowedHeaders=*` (werden `Authorization`, `Cookie`, `SAP_*`,
   Camel-interne Header nach Kafka geschrieben?), Credentials in Logs/MPL, JAAS-Escaping, eigene
   `SslEngineFactory` (Hostname-Verifikation, Trust-Verhalten), Schema-Registry-HTTP-Client (TLS,
   Redirects, Timeouts, Auth-Header, vom Nutzer steuerbare URL).
4. **Fehlersichtbarkeit:** Landet jeder Fehler, den der Betreiber kennen muss, als `ERROR` im Trace
   und – wo sinnvoll – im MPL? Welche Fehler werden geschluckt oder nur als `WARN` geloggt
   (unsichtbar in Produktion)? Gibt es falsch-positive `ERROR`-Zeilen, die Alerting fluten?
5. **Lifecycle/Nebenläufigkeit:** Thread-Sicherheit des `KafkaConsumer`, Stop/Undeploy, Cluster-Lock
   und Multi-Node-Betrieb in CPI, Ressourcen-Leaks (Producer, Consumer, HTTP-Clients, Threads).
6. **OSGi/Class-Space:** Robustheit gegenüber Adapter-Updates (ADR 0005), `DynamicImport-Package`,
   Kafka-Client-Version gegenüber der CPI-JVM, Kompatibilität des Camel-Importbereichs.
7. **Lieferkette und Lizenz:** Abgleich `README.md`/`NOTICE`/`pom.xml` (z. B. eingebettete
   `io.confluent`-Artefakte bei ADR 0002 im Status „Proposed“; Lizenzvereinbarkeit AGPL-3.0 mit
   Confluent Community License).
8. **Tests:** Welche der gefundenen Risiken sind ungetestet? Gibt es Tests, die Metadaten,
   Java-Defaults und Start-Validierung gegeneinander abgleichen? (Bisher scheint nur die
   Versionskonsistenz getestet zu sein: `CpiKafkaPlusMetadataVersionTest` – prüfen.)

## E. Abgleich mit externen Informationen

Vergleiche Adapter-Verhalten und Dokumentation mit:

- **SAP:** ADK-Dokumentation/Metadaten-XSD (Constraints, `EditCondition`, `Usage`, Externalisierung),
  Doku und bekannte Fehlerbilder des SAP-Standard-Kafka-Adapters (Sender/Receiver-Konfiguration,
  Deployment-Fehler, Einschränkungen wie Consumer-Group, Keystore, SCRAM).
- **Apache Kafka / Confluent:** Konfigurationsregeln und Wechselwirkungen
  (`enable.idempotence`/`acks`, `transaction.timeout.ms` ≤ Broker-`transaction.max.timeout.ms`,
  `delivery.timeout.ms` ≥ `linger.ms` + `request.timeout.ms`, `max.poll.interval.ms`,
  `session.timeout.ms`, Static Membership, `max.request.size` vs. Broker-`message.max.bytes`,
  `KAFKA-10902`).
- **Repo-Historie:** `CHANGELOG.md`, geschlossene Issues (#44, #133, #146, #148, #154, #166) und
  `docs/`: Welche Fehlerklassen traten wiederholt auf? Wo wurde ein Symptom gefixt, aber die
  Ursachenklasse (z. B. „Regel nur zur Laufzeit geprüft“) nicht?

Jede Abweichung zwischen Doku, Tooltip und Code als eigenen Befund aufnehmen.

# Vorab-Befunde (Hypothesen, einzeln zu verifizieren)

Zeilenangaben beziehen sich auf Adapter 1.3.6 (Commit `e31faa8`). Prüfe sie gegen den aktuellen
Stand, falls der Code sich bewegt hat. Schweregrade sind Erstschätzungen.

## V-A: Validierungslücke und Parameter-Abhängigkeiten

**V1 – Nur zwei Felder haben eine `Restriction` (Hoch).**
`metadata-sender-1.3.0.xml:78` (`credentialAlias`), `:145` (`pollingIntervalSeconds`),
`metadata-receiver-1.3.0.xml:73` (`credentialAlias`). Alle anderen `<ErrorMessage>`-Tags haben keine
`Restriction` und sind nach der Mechanik aus Issue #44 vermutlich wirkungslos: Sender `:155, 160,
178, 183, 188, 207, 259, 293, 343, 352, 377, 400, 409`; Receiver `:139, 171, 180, 199, 204, 209,
264, 318, 323, 332`. Sie versprechen Bereiche („1–51200“, „0–300“, „1–5“, „5–900“), die erst beim
Start geprüft werden (`CpiKafkaPlusConsumer.java:271-279`, `CpiKafkaPlusProducer.java:357-379`).
Zu klären: Welche davon sind als einfache Regex ausdrückbar? `TENANT-TEST NÖTIG`: Wirkt ein
`ErrorMessage` ohne `Restriction` wirklich nicht?

**V2 – Retry-Defaults sind gegeneinander unbrauchbar (Hoch).**
Defaults: `deliveryTimeoutSeconds=120`, `producerRetryTotalBudgetSeconds=30`,
`producerRetryDelaySeconds=2`. Wer nur `producerRetryMaxAttempts=2` setzt, scheitert beim Start:
`CpiKafkaPlusProducer.java:408-423` mit `ProducerRetryPolicy.worstCaseSeconds`
(`ProducerRetryPolicy.java:301-312`). Rechnung: ohne Transaktionen `2×(30+120)+2 = 302 s`, mit
Transaktionen `2×(30+120+30+30+5)+2 = 432 s`; bei 3 Versuchen 454 s bzw. 649 s. Bei Budget 30 s
funktioniert ein zweiter Versuch (ohne Transaktionen) nur, wenn `deliveryTimeoutSeconds` höchstens
etwa 7 s beträgt. Die UI warnt davor nicht; nur der Tooltip (`metadata-receiver-1.3.0.xml:331`)
erwähnt die Start-Prüfung. Zu klären: Budget aus den anderen Werten ableiten statt eingeben lassen?
Sinnvolle Defaults? Dropdown für die Versuchsanzahl (→ Arbeitspaket B.2)?

**V3 – Start-Validierung läuft unbedingt, das Feld ist aber ausgeblendet (Mittel–Hoch).**
- `minBacklogToDrain > maxPollRecords` wird in `CpiKafkaPlusConsumer.java:264-269` geprüft,
  unabhängig von `drainEnabled`; das Feld ist bei `drainEnabled=false` ausgeblendet
  (`metadata-sender-1.3.0.xml:204-212`).
- `retryDelaySeconds` (0–300) wird in `CpiKafkaPlusConsumer.java:275-279` geprüft, unabhängig von
  `dlqEnabled`; das Feld ist ohne DLQ ausgeblendet (`metadata-sender-1.3.0.xml:374-382`).
  Ein früher gesetzter Wert kann das Deployment also über ein unsichtbares Feld verhindern.
  Die Streaming-Sonderbehandlung (`:251-254`) zeigt, dass das Problem bekannt ist. Prüfe alle
  übrigen Felder mit `EditCondition` auf dasselbe Muster.

**V4 – Was `validateConfiguration()` nicht prüft (Hoch).**
`CpiKafkaPlusEndpoint.java:399-437` prüft nur Topic (leer/Leerzeichen), Schema-Registry-URL
(leer), JSON-Schema (leer), SASL-Alias (leer) und `diagnosticsLevel`. Nicht geprüft, soweit
erkennbar: `groupId` (in der UI Pflicht, aber keine Start-Prüfung), Format von `bootstrapServers`,
Gültigkeit der Schema-Registry-URL, alle Enum-Strings (`securityProtocol`, `saslMechanism`,
`autoOffsetReset`, `commitStrategy`, `acks`, `compressionType`, `producerBatchMode`,
`batchOutputFormat`, `avroOutputFormat`, `subjectNameStrategy`), Untergrenzen der Zahlenfelder
(`maxPollRecords`, `batchSize`, `batchTimeout`, `fetchMinBytes`, `fetchMaxWaitMs`, `dlqMaxRetries`,
`autoPause*`, `maxRequestSizeKb`, `producerBatchSizeKb`, `bufferMemoryKb`, `deliveryTimeoutSeconds`),
mögliche `int`-Überläufe in `ProducerConfigFactory.java:103-105` (`*1024` auf `int`). Verifiziere
pro Feld, *wo* ein ungültiger Wert wirklich auffällt.

**V5 – Lazy Init verschiebt Verbindungs- und Credential-Fehler (Hoch).**
`CpiKafkaPlusConsumer.java:289`, `CpiKafkaPlusProducer.java:264`. Ein fehlender Credential-Alias
wird nur geloggt, der Start läuft weiter (`SecurityConfigHelper.java:58-82`: „Kafka will fail to
connect“). Gleiches gilt für fehlenden Keystore-Alias und `SSL` ohne Alias
(`SecurityConfigHelper.java:84-88` kehrt still zurück, obwohl die UI `SSL` als „certificate
authentication“ beschriftet, `metadata-sender-1.3.0.xml:474`). Der Producer prüft das Topic nur im
Hintergrund und warnt nur (`CpiKafkaPlusProducer.java:342-345`); Warnungen erreichen den Trace in
Produktion nicht (`docs/troubleshooting.md:15`). Zu klären: Welche dieser Prüfungen brauchen kein
Netzwerk und könnten beim Deployment fehlschlagen? Wie sieht ein Betreiber den Fehler heute?

**V6 – Uneinheitliche Normalisierung von `securityProtocol` (Niedrig–Mittel).**
`validateConfiguration` nutzt `toUpperCase()` (`CpiKafkaPlusEndpoint.java:422-424`),
`SecurityConfigHelper.java:49,53` nutzt dagegen `contains("SASL")`/`contains("SSL")` ohne
Normalisierung. Prüfe Verhalten bei `sasl_ssl`, bei Leerzeichen und bei externalisierten Werten.

## V-B: Semantik und Verständlichkeit der Parameter

**V7 – `jsonSchemaReportError` bedeutet je Richtung etwas anderes (Mittel).**
Receiver: Bei einem Schema-Verstoß wirft der Producer **immer**
(`CpiKafkaPlusProducer.java:1028-1032`); der Schalter steuert nur, ob die Payload zusätzlich
getraced wird. Doku und Java-Beschreibung behaupten das Gegenteil („otherwise invalid messages are
dropped“: `docs/configuration.md:197`, `CpiKafkaPlusEndpoint.java:244-247`). Sender: Bei `false`
wird nur `LOG.warn` geschrieben (`RecordProcessor.java:189-196`, `:447-451`), der MPL-Eintrag
entfällt; ohne DLQ könnte der Datensatz **still verworfen** werden. Prüfe, ob dabei der Offset
committed wird (Datenverlust) und ob der Default `false` vertretbar ist.

**V8 – Veraltete oder irreführende Texte (Niedrig–Mittel).**
- `batchOutputFormat`-Tooltip nennt noch „Individual Exchanges“ (`metadata-sender-1.3.0.xml:267`),
  obwohl der Wert seit 1.2.0 aus dem Dropdown entfernt ist.
- `batchTimeout` heißt in der UI „Poll Timeout (ms)“ (`metadata-sender-1.3.0.xml:631`), die
  Java-Beschreibung sagt „Maximum wait time in ms to fill a batch“
  (`CpiKafkaPlusEndpoint.java:150-152`); Parametername und Bedeutung passen nicht zusammen.
- Die Sender-Metadaten führen Producer-`AttributeMetadata` mit abweichenden Defaults
  (`maxRequestSizeKb` 1024, `producerBatchSizeKb` 249; `metadata-sender-1.3.0.xml:722-739`)
  gegenüber Receiver-Metadaten und Java (5120 bzw. 1024; `metadata-receiver-1.3.0.xml:625-640`,
  `CpiKafkaPlusEndpoint.java:175-181`). Vermutlich folgenlos, weil nicht referenziert – aber eine
  Falle für jede spätere Änderung.
- `docs/troubleshooting.md:54-58`: doppelte/verrutschte Tabellenzeilen (`KP-SR-001`, `KP-GEN-001`).
- `docs/configuration.md` hat keine Abhängigkeits- oder Kombinationsübersicht, nur verstreute
  Hinweise.

**V9 – Sichtbarkeitslogik der UI (Mittel).**
- `saslMechanism` hat keine `EditCondition` und erscheint auch bei `SSL`/`PLAINTEXT`
  (`metadata-sender-1.3.0.xml:71-74`, `metadata-receiver-1.3.0.xml:66-69`).
- `producerRetryDelaySeconds`, `producerRetryOnlyTransientErrors` und
  `producerRetryTotalBudgetSeconds` sind auch bei `producerRetryMaxAttempts=1` (Feature aus)
  sichtbar (`metadata-receiver-1.3.0.xml:320-333`).
- `enableTransactions=true` zusammen mit `enableIdempotence=false` ist in der UI wählbar, wird aber
  erst beim Start abgelehnt (`CpiKafkaPlusProducer.java:275-279`).
- Der Receiver zeigt `acks` nur bei ausgeschalteter Idempotenz; sonst wird der Wert still auf `all`
  gezwungen (`ProducerConfigFactory.java:95-100`). Sinnvoll, aber undokumentiert in der UI?
- `EditCondition` kennt nur Gleichheit auf einen Literalwert; „`maxAttempts > 1`“ ist so nur über
  Dropdown-Werte ausdrückbar.

**V10 – Gefährliche oder unklare Defaults (Mittel).**
`allowedHeaders=*` (`CpiKafkaPlusEndpoint.java:218-220`; `HeaderFilterStrategy.java:45-47` kennt
keine Sperrliste): Prüfe in `addRecordHeaders`, ob `Authorization`, `Cookie`, `SAP_*` und
Camel-interne Header nach Kafka gelangen. `commitStrategy=AUTO` wird ohne Warnung angeboten
(`metadata-sender-1.3.0.xml:587-589`). `autoOffsetReset=latest` überspringt bei neuer Consumer-Group
alle vorhandenen Daten.

## V-C: Betrieb, Fehlersichtbarkeit, Lieferkette

**V11 – `ERROR`-Log als Workaround (Niedrig–Mittel).**
Nicht-Fehler werden auf `ERROR` geloggt, etwa `CpiKafkaPlusEndpoint.java:446`,
`CpiKafkaPlusConsumer.java:286`. Begründung und Gegenargumente (Issue #133: Heartbeat flutet den
Trace) bewerten; gibt es einen sauberen Weg (eigener Logger/Kategorie)?

**V12 – Lizenz- und Lieferketten-Konsistenz (Niedrig–Mittel).**
`README.md:99` nennt die Confluent Community License für den Schema-Registry-Client;
`pom.xml:196-209` bettet `io.confluent`-Artefakte ein; ADR 0002 (Ersatz durch JDK-Client) hat den
Status „Proposed“. Prüfe, ob Dokumentation, `NOTICE` und tatsächliche Auslieferung übereinstimmen
und ob die Kombination mit AGPL-3.0 sauber dokumentiert ist (keine Rechtsberatung, nur
Konsistenzprüfung).

**V13 – Testlücke (Mittel).**
Es scheint keinen Test zu geben, der Metadaten (Defaults, `Restriction`, `ErrorMessage`,
`EditCondition`) mit Java-Defaults und Start-Validierung abgleicht; `CpiKafkaPlusMetadataVersionTest`
prüft laut Stichprobe nur Versionskonsistenz. Bestätige das und skizziere den Test.

**V14 – Toter Code und Technik-Schulden (Niedrig).**
`transactionV2Enabled` ist als `@Deprecated` ohne Wirkung im Endpoint geblieben
(`CpiKafkaPlusEndpoint.java:203-216`); ein auskommentierter `retries`-Parameter
(`:222-227`). Bewerte, ob und wann das entfernt werden sollte (Major-Grenze).

# Ausgabeformat

Liefere **einen** Bericht in Markdown mit genau diesen Abschnitten:

1. **Kurzfazit** (max. 10 Zeilen): die fünf wichtigsten Risiken, Gesamteinschätzung der
   Validierungslücke (D0 vs. D1 vs. D2 vs. D3 als Zahlen: wie viele Regeln je Stufe).
2. **Befundliste**, nach Schwere sortiert (Kritisch / Hoch / Mittel / Niedrig). Pro Befund:
   `ID | Titel | Datei:Zeile | Beleg (Zitat) | Szenario mit konkreten Parameterwerten | sichtbar in
   D0/D1/D2/D3 | Fix (1–3 Sätze) | Aufwand S/M/L | Versionsklasse Micro/Minor/Major/nur Code |
   Status BESTÄTIGT/WIDERLEGT/TEILWEISE/UNVERIFIZIERT`.
3. **Parameter-Matrix** (Arbeitspaket A) als Markdown-Tabelle.
4. **Validierungskonzept:** pro Regel die Ziel-Stufe und die konkrete Umsetzung – mit
   XML-Snippets für `Restriction`, `FixedValues`, `EditCondition` und Java-Snippets für
   `validateConfiguration()`; getrennt nach „sofort (Micro)“ und „nächstes Minor“.
5. **Usability-Vorschläge** inkl. Golden-Path-Konfigurationen und überarbeiteter Tab-Struktur.
6. **Testplan:** konkrete neue Tests (z. B. Metadaten-Lint: jeder `ErrorMessage` hat eine
   `Restriction` oder ist `Usage=true`; Default-Parität Java ↔ Metadaten ↔ Doku; Start-Validierung
   je Regel; „jede Ein-Schalter-Änderung ausgehend von den Defaults ist deploybar“).
7. **Roadmap:** Reihenfolge der Umsetzung, zugeordnet zu Micro/Minor/Major nach `VERSIONING.md`.
8. **Offene Punkte / TENANT-TEST NÖTIG:** exakte Testschritte für alles, was nur im Tenant
   prüfbar ist.

**Qualitätskriterien:** keine Füllsätze, keine Befunde ohne Beleg, keine Wiederholung von
Vorab-Befunden ohne Nachprüfung, keine Empfehlungen, die frozen Metadaten ändern oder gegen
`VERSIONING.md` verstoßen. Wenn du etwas nicht prüfen konntest, sag es ausdrücklich.
