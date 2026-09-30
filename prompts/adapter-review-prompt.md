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

## V-D: Sender (Kafka → CPI) – Fehlerpfad, Commit, DLQ

Herkunft: Teilreview per Code-Lektüre (kein Broker-Lauf). Status pro Befund:
`VORGEPRÜFT` = Kernaussage vor Aufnahme am Code nachgelesen; `GEMELDET` = nicht nachgeprüft, du
musst sie verifizieren. Zeilenangaben sind Stand 1.3.6.

**V15 – Fehlgeschlagene Datensätze werden übersprungen statt erneut geliefert (Kritisch,
VORGEPRÜFT).** Ohne DLQ ist `maxRetries = 0` (`RecordProcessor.java:364`). Schlägt die Route fehl,
gibt `processRecordWithRetry` `0` zurück, ohne zu committen (`:562-571`); `processSingleRecords`
läuft mit dem nächsten Datensatz weiter (`:232-234`). Es gibt keinen `seek()` zurück auf den
fehlgeschlagenen Offset (`seek` nur in `CpiKafkaPlusConsumer.java:773` und `:895`), und
`OffsetCommitTracker.markProcessed` speichert nur den höchsten Offset (`OffsetCommitTracker.java`,
Methode `markProcessed`). Der nächste erfolgreiche Datensatz committet damit über den
fehlgeschlagenen hinweg. Beispiel: Offsets 100–109, Datensatz 103 schlägt fehl, 104 gelingt →
Commit 105, Datensatz 103 ist verloren. Dasselbe gilt für eine fehlgeschlagene DLQ-Zustellung
(`RecordProcessor.java:541-561`, der Kommentar dort behauptet „polled again on the next cycle“).
**Dokumentation widerspricht dem Code:** `docs/features/dead-letter-queue.md:142-145` („offsets are
not committed … re-delivered on the next poll cycle“, „poison pill will block the consumer“),
`docs/faq.md:66-72`. `AutoPauseIT` bildet das Überspringen ab (prüfen). Zu klären: tatsächliche
Semantik je Modus (Batch/Einzeln, mit/ohne DLQ, Drain), Fix (`seek` + Partition anhalten, Consumer
neu aufbauen, explizite „Überspringen“-Option, DLQ-Pflicht) und Korrektur der Doku.

**V16 – Auto-Pause löst bei gewöhnlichen Fehlern nie aus (Hoch, VORGEPRÜFT).**
`recordFailure()` wird nur im `catch` in `CpiKafkaPlusConsumer.java:722-733` aufgerufen.
`RecordProcessor` fängt Route-Exceptions selbst (`:407`) und kehrt normal zurück; danach ruft
`CpiKafkaPlusConsumer.java:719-721` `recordSuccess()` auf und setzt den Zähler zurück.
`AutoPauseIT` funktioniert nur, weil der Test einen `Error` wirft
(`DownstreamProcessingError extends Error`, `AutoPauseIT.java:310`). Die UI verspricht Schutz bei
ausgefallenem Backend (`metadata-sender-1.3.0.xml:395`). Prüfe die Wirkung mit echten
Route-Fehlern.

**V17 – JSON-Schema-Verstoß: Commit auch bei gescheitertem DLQ-Write; Commit vor der
Verarbeitung (Hoch, VORGEPRÜFT).** `filterInvalidRecords` fängt einen DLQ-Fehler, loggt ihn und
committet den Offset trotzdem (`RecordProcessor.java:196-216`); dasselbe Muster in
`handleSchemaValidationFailure` (`:447-467`, GEMELDET). Im Batch-Modus läuft der Filter über den
ganzen Poll, bevor gültige Datensätze verarbeitet werden: ein ungültiger Datensatz bei Offset 105
committet 106, bevor 100–104 verarbeitet sind (GEMELDET; Reihenfolge in `processBatchRecords`
prüfen). Bei ausgefallener DLQ gehen Datensätze verloren, obwohl DLQ konfiguriert ist.

**V18 – Commit-Fehler werden wie Verarbeitungsfehler behandelt (Hoch, GEMELDET).**
`commitSingleOffset` (`RecordProcessor.java:403-405`) und `commitOffsets` (`:324-325`) laufen im
selben `try/catch` wie die Route; nur `CommitFailedException`/`RebalanceInProgressException` werden
abgefangen (`:905`). `doStop` ruft `wakeup()` (`CpiKafkaPlusConsumer.java:305`): ein laufender
`commitSync` kann `WakeupException` werfen → fälschlich Route-Retry/DLQ-Eintrag für einen
erfolgreich verarbeiteten Datensatz. Verarbeitungsschleifen prüfen `shutdownRequested` nicht
(nur `:672`).

**V19 – DLQ-Producer wird bei jedem Consumer-Reconnect neu gebaut, der alte nicht geschlossen
(Hoch, VORGEPRÜFT).** `createConsumerHelpers` überschreibt `dlqHelper` ohne vorheriges
`close()` (`CpiKafkaPlusConsumer.java:520-521`); `closeConsumerQuietly` schließt nur den
`KafkaConsumer` (`:1474-1484`); `dlqHelper.close()` gibt es nur in `doStop` (`:354-360`) und bei
Helper-Initfehlern (`:533-535`). Gleiches für `avroHelper`. Bei Ausfällen mit Reconnect-Zyklen
sammeln sich Producer samt Threads und Sockets an. `UNVERIFIZIERT`: ob der `KafkaProducer` schon im
Konstruktor von `DlqProducerHelper` entsteht.

**V20 – Transiente Fehler werden als dauerhaft eingestuft (Hoch, GEMELDET).** `isRetryable`
(`RecordProcessor.java:246-263`) kennt nur `java.net.*`, `TimeoutException` und Kafka-
`RetriableException`. HTTP 503/429 aus einem Receiver oder `SchemaRegistryException` zählen als
„permanent“ und gehen mit dem Default `retryOnlyTransientErrors=true` ohne Retry direkt in die DLQ.
Avro-Fehler laufen über `handleDeserializationFailure` (`:354-357`, `:594-635`) ohne Retry. Eine
Schema-Registry-Störung bei kaltem Cache kann so gültige Datensätze massenhaft in die DLQ schieben.
Ein fehlender `schemaRegistryCredentialAlias` liefert still keine Auth (`AvroDeserializerHelper`
Zeilen ~56-63), der erste Datensatz erhält HTTP 401 und gilt ebenfalls als „Poison“.

**V21 – DLQ-Konfiguration wird kaum validiert (Hoch, teils VORGEPRÜFT).**
- `dlqTopic` gleich einem Quell-Topic wird akzeptiert; geprüft wird nur „nicht leer“
  (`CpiKafkaPlusConsumer.java:244-250`, VORGEPRÜFT) → Endlosschleife aus Fehlschlag und
  Wiederverarbeitung möglich.
- Der DLQ-Producer setzt kein `max.request.size` (`DlqProducerHelper.java:530-551`), der Consumer
  darf bis zu 50 MB holen → große Datensätze scheitern an der DLQ (GEMELDET).
- `dlqCredentialAlias` ist als „für einen anderen Cluster“ beschrieben
  (`metadata-sender-1.3.0.xml:360`), der DLQ-Producer nutzt aber immer `endpoint.getBootstrapServers()`
  (`DlqProducerHelper.java:532`) (GEMELDET). Prüfe, ob ein Cluster-Wechsel überhaupt möglich ist,
  sonst Tooltip korrigieren.
- Existenz des DLQ-Topics wird erst beim ersten Dead-Letter geprüft; ein Send kann pro Datensatz bis
  zu 60 s (`max.block.ms`-Default) blockieren (GEMELDET).

**V22 – Zahlenparameter ohne Bereichsprüfung (Hoch, teils VORGEPRÜFT).** Geprüft wird beim Start nur
`pollingIntervalSeconds`, `retryDelaySeconds`, `maxPartitionFetchSizeKb`, `minBacklogToDrain`
(`CpiKafkaPlusConsumer.java:254-282`). Alle Felder sind `isparameterized=true`, Werte können also
ungeprüft aus externalisierten Parametern kommen. Gemeldete Folgen:
- `batchSize = 0`: `i += batchSize` (`RecordProcessor.java:151`) macht keinen Fortschritt (VORGEPRÜFT);
  prüfe, ob daraus Endlosschleife oder sofort eine Exception wird (leere Sub-Liste in
  `processOneBatch`, `batch.get(0)`); negativ → `IndexOutOfBoundsException`.
- `dlqMaxRetries < 0`: Schleife `for (attempt <= maxRetries)` (`:383`) läuft nie, die Route wird nie
  aufgerufen, danach NPE/Verlust beim DLQ-Send.
- `maxPollRecords <= 0`, `fetchMinBytes`/`fetchMaxWaitMs < 0`: Kafka-`ConfigException` erst beim
  ersten Poll. `batchTimeout < 0`: Verhalten `UNVERIFIZIERT`.
- `autoPauseErrorThreshold <= 0`: pausiert beim ersten Fehler; `autoPauseCooldownSeconds <= 0`:
  faktisch keine Pause, aber ERROR-Status.

**V23 – Lazy Init im Detail (Mittel–Hoch, GEMELDET).** Erst beim ersten Poll (ca. 5 s nach
Deployment) fallen auf: `bootstrapServers`/`groupId` leer (NPE bei `Properties.put(key, null)`,
`CpiKafkaPlusConsumer.java:1344-1345`), fehlender Credential- oder Keystore-Alias (Konstruktor des
`KafkaConsumer` bzw. `CpiKafkaPlusSslEngineFactory.configure`), ungültige JSON-Schema-Syntax
(`:517`), unbekannte Werte für `securityProtocol`/`autoOffsetReset` (`ConfigException`).
Still falsch statt Fehler:
- Unbekannter `saslMechanism` (z. B. `OAUTHBEARER`) bekommt `PlainLoginModule`
  (`SecurityConfigHelper.java:97-101`, VORGEPRÜFT).
- Unbekannter `commitStrategy` (Tippfehler über externalisierten Parameter) ist weder `AUTO` noch
  `BATCH_COMPLETE` (`CpiKafkaPlusConsumer.java:657`, `:1356`, VORGEPRÜFT): dann gilt
  `enable.auto.commit=false` *und* kein Commit nach Erfolg → Offsets werden nie committet
  (GEMELDET, herleiten).
- Unbekannte `batchOutputFormat`/`avroOutputFormat` fallen still auf JSON (GEMELDET).
- Fehlendes Topic meldet keinen Fehler; `allow.auto.create.topics` bleibt auf dem Default `true`
  (GEMELDET) – auf Brokern mit Auto-Create legt ein Tippfehler ein leeres Topic an.
- Der Betreiber sieht: Status „Started“, keinen MPL-Eintrag (es gibt keine Exchange), aber bei
  jedem Tick eine ERROR-Zeile (bei 5 s Intervall etwa 17.000 pro Tag, bis 8 KB je Zeile), ohne
  Backoff und ohne dauerhaften Stopp bei deterministischen Konfigurationsfehlern
  (`KafkaErrorHelper.INIT_FAILURE_ESCALATION_THRESHOLD`, `:42`, laut Teilreview ungenutzt).

**V24 – JSON-Verarbeitung verändert Payloads (Mittel, GEMELDET, vom Teilreview lokal mit Jackson
nachgestellt).** `BatchFormatter:69,79` und `JsonSchemaValidator:81` nutzen `readTree(String)` mit
Defaults: `true story` → Boolean `true`, `null pointer` → `null`; `{"a":1} {"b":2}` und
`{"a":1}garbage` behalten nur den ersten Wert, und der Validator lässt solche Nachrichten durch;
`1.10` → `1.1`, `12345678901234567.89` → `1.2345678901234568E16`; leerer Wert → `null`.
Prüfe `FAIL_ON_TRAILING_TOKENS`, `USE_BIG_DECIMAL_FOR_FLOATS` und „nur einbetten, wenn der ganze
String ein Objekt/Array ist“.

**V25 – Avro-Ausgabe (Mittel, GEMELDET).** Avros `JsonEncoder` erzeugt für nullable Unions
`{"name":{"string":"Bob"}}` (`AvroDeserializerHelper.java:134`) – undokumentiert und unvereinbar mit
einem JSON-Schema für „normales“ JSON; nicht-Record-Top-Level-Schema → `ClassCastException`
(`:110`); XML-Ausgabe nutzt `toString()` für verschachtelte Strukturen; Avro gilt für *alle*
abonnierten Topics, ein Nicht-Avro-Topic in der Liste scheitert am Magic Byte.

**V26 – `isolation.level` nicht gesetzt (Mittel, GEMELDET).** Default `read_uncommitted`: der Sender
liest abgebrochene Transaktionen mit. Der transaktionale Receiver dieses Adapters setzt auf
`read_committed` (`docs/features/producer-retry.md:21`), ein Parameter fehlt. Prüfe, ob das für
Kafka→Kafka-Szenarien mit diesem Adapter eine Falle ist.

**V27 – `max.poll.interval.ms` passt nicht zu Retry-Einstellungen (Mittel, GEMELDET).**
`max.poll.interval.ms = pollingInterval + 10 min` (`CpiKafkaPlusConsumer.java:1285-1290`), die
Backoff-Schlafzeit ist pro Schlaf auf 300 s gedeckelt (`RecordProcessor.java:476-490`). Mit
`retryDelaySeconds=30` und `dlqMaxRetries=5` schläft ein fehlschlagender Datensatz
30+60+120+240+300 = 750 s; der Consumer wird aus der Gruppe geworfen, Commits scheitern mit
`CommitFailedException` (wird verschluckt) → Duplikate. Kein Start-Check, nur ein Doku-Hinweis
(`docs/features/dead-letter-queue.md:90`). Dieselbe Klasse von Regel wie V2 (Cross-Field).

**V28 – `group.instance.id` kollidiert zwischen iFlows/Stages (Mittel, GEMELDET).**
`groupId + "-" + CF_INSTANCE_INDEX` (`CpiKafkaPlusConsumer.java:1420-1423`) enthält weder Topic
noch iFlow. Zwei iFlows oder DEV/QA/PROD mit gleicher `groupId` erhalten auf Index 0 dieselbe ID →
`FencedInstanceIdException`-Schleifen (`:1111-1132`). Die UI warnt nur im Tooltip
(`metadata-sender-1.3.0.xml:58`). `UNVERIFIZIERT`: ob `close()` bei Static Membership ein
LeaveGroup sendet (der Kommentar `:331-335` behauptet es) und ob in einem CPI-Cluster nur der
Cluster-Lock-Halter pollt (Annahme nur im Kommentar `:419-423`).

**V29 – `jsonSchemaReportError` im Batch- vs. Einzelpfad uneinheitlich (Mittel, GEMELDET).** Im
Batch-Pfad läuft `callback.handleException` unabhängig vom Flag (`RecordProcessor.java:210-213`,
VORGEPRÜFT), im Einzelpfad nicht. `reportValidationErrorToMpl` (`:946`) wirft absichtlich und fängt
selbst, geloggt als `consumer.mpl.report.failed` – ein irreführender ERROR im Normalfall.
Ohne DLQ wird der Datensatz verworfen und committet (die Doku nennt das so:
`docs/features/dead-letter-queue.md:136-140`), aber nur per `WARN` – in Produktion unsichtbar.

**V30 – Weitere Niedrig-Befunde (GEMELDET).**
- Keep-Alive-Poll-Fehler nur als `WARN` (`CpiKafkaPlusConsumer.java:822`), zählen nicht zum
  Reconnect; eine tote Verbindung fällt erst beim nächsten Emit-Zyklus auf (bis
  `pollingIntervalSeconds`, maximal 6 h).
- `TlsListenerProbe.java:107,131` cached ein „INCONCLUSIVE“ dauerhaft; lief die erste Probe bei
  ausgefallenem Broker, bleibt der Schutz gegen Node-Crash für die JVM-Lebenszeit aus.
- `CpiKafkaPlusTopic`-Header im Multi-Topic-Batch ist die Komma-Liste
  (`RecordProcessor.java:692-695`), obwohl jeder Batch nur ein Topic hat; `batchSize` ist als
  „sammelt bis zu …“ beschrieben, Batches überspannen aber keine Polls (effektiv höchstens
  Datensätze je Partition und Poll).
- Speicher: Der ganze Poll wird deserialisiert und zusätzlich als String, Jackson-Baum und
  UTF-8-Kopie gehalten (`:317`), kein Größenlimit für den Body; Auswirkung auf den Node-Heap
  `UNVERIFIZIERT`.
- `LOG.error` für Nicht-Fehler (Konsequenz: Fehlalarme beim Alerting; drei ERROR-Ereignisse je
  fehlgeschlagenem Datensatz) – Zeilen `CpiKafkaPlusConsumer.java:286, 446, 641, 1379, 1424, 1427`
  (letztere Liste nicht vollständig nachgeprüft).
- Felder `kafkaConsumer`, `recordProcessor`, `circuitBreaker` (`:99, 123-124`) sind nicht
  `volatile`, werden aber vom Poll- und vom Stop-Thread benutzt (praktische Wirkung
  `UNVERIFIZIERT`).
- `AUTO` wird beim Start nur zusammen mit Drain im SCHEDULED-Modus abgelehnt
  (`CpiKafkaPlusConsumer.java:255`), nicht mit DLQ, Batch oder Schema-Validierung; unter `AUTO` ist
  jeder Fehler per Definition höchstens einmal zugestellt.
- `batchSize > maxPollRecords` wird nicht validiert (GEMELDET; das wirkt nur als Obergrenze).
- Doku ↔ UI (VORGEPRÜFT): `docs/configuration.md:97-98` listet `autoRegisterSchemas` und
  `subjectNameStrategy` in der **Sender**-Tabelle, die Sender-Variante verweist aber auf keines der
  beiden (`metadata-sender-1.3.0.xml:286-322`; nur als `AttributeMetadata` definiert, `:810-832`).
  Laut Teilreview nutzt der Consumer `subjectNameStrategy` ohnehin nicht. Doku bereinigen oder
  Parameter erklären.

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
