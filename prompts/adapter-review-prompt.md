# Review-Prompt: Kafka Adapter Plus (SAP Cloud Integration)

**Zweck:** Dieser Prompt lässt ein KI-Modell (z. B. Claude Code im Repo-Root) den Adapter
systematisch prüfen: Code-Robustheit, Parameter-Usability und – der Schwerpunkt – die Lücke
zwischen *„die UI akzeptiert es“* und *„das Deployment schlägt fehl“*.

**Benutzung:** Alles unterhalb der Linie `=== PROMPT ===` in eine neue Session im Repo-Root
einfügen. Der Prompt ist bewusst read-only (kein Edit, kein Commit, kein PR). Die Session braucht lesenden
Git-Zugriff auf `github.com/SAP-docs/btp-integration-suite` (SAP-Dokumentation, siehe
Arbeitspaket E); ohne ihn sind die SAP-Abgleiche als `UNVERIFIZIERT` zu kennzeichnen.
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
  dafür sind wertlos. SAP selbst (`versioning-rules-for-custom-adapters-61a988b.md`): Micro =
  „label and tooltip updates“ (gleiche Datei, Version auf Komponenten- *und* Variantenebene),
  Minor = neues Feature (Pflichtfelder „not recommended“, Laufzeit muss abwärtskompatibel sein),
  Major = „not supported“. Ob das Hinzufügen einer `Restriction` oder `EditCondition` noch ein Micro
  ist, ist ein Grenzfall – ordne es begründet ein und nenne das Risiko (bereits gespeicherte Werte
  können beim nächsten Öffnen des Channels ungültig sein).
- **Quellen.** Die SAP-Dokumentation zur Adapter-Entwicklung liegt als Markdown im öffentlichen
  Repo `SAP-docs/btp-integration-suite`, Verzeichnis `docs/ISuite_Integrations_APIs/` (identisch
  unter `docs/ci/Development/`). Die Einstiegsseite `developing-custom-adapters-7392cc4.md` ist
  leer; der Inhalt steht in den Geschwisterdateien (Liste unter „Arbeitspaket E“). Hole das Repo
  flach (`GIT_LFS_SKIP_SMUDGE=1 git clone --depth 1 https://github.com/SAP-docs/btp-integration-suite`)
  und lies die Dateien selbst; verlasse dich nicht auf Zusammenfassungen. Andere externe Quellen
  nennst du mit URL und Abrufstatus. Ist eine Seite (z. B. `community.sap.com`, `help.sap.com`)
  nicht erreichbar, schreibe das hin und markiere die Aussage als `UNVERIFIZIERT` – ersetze sie
  nicht durch Erinnerung.

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

**Belegte Mechanik der UI-Validierung** (SAP-Doku `component-metadata-550b19e.md`, Issue #44,
Kommentar des Maintainers). Lies die SAP-Seite selbst; Kern:
- Validiert wird ein `AttributeReference`-Feld nur über `<Restriction>Constraint.<Name>(…)</Restriction>`.
  Unterstützt laut SAP: `isValidRegex` (Java-Regex), `isAlphaNumeric`, `isStartsWithLetter`,
  `isValidURIString` (akzeptiert http(s), ftp, file, ldap), `isValidXMLString`, `isValidNCName`,
  `isValidXpath`. Einen Zahlenbereichs- oder Cross-Field-Constraint gibt es nicht.
- `<ErrorMessage>` gehört zur `Restriction`: „If the constraint specified in the restriction tag
  fails, the system shows the error message set in this tag.“ Ohne `Restriction` wird es nie
  angezeigt. Nur zwei Stellen nutzen heute eine `Restriction`: `pollingIntervalSeconds` (Sender)
  und `credentialAlias` (beide Richtungen); alle anderen `ErrorMessage`-Tags sind Totext.
- `<Usage>true</Usage>` = Pflichtfeld (prüft nur „nicht leer“). `<Length>` = maximale,
  `<Minlength>` = minimale Zeichenzahl. `FixedValues` = Dropdown/Combo; `isEditable` macht eine
  Combo frei editierbar.
- `EditCondition` steuert Sichtbarkeit/Editierbarkeit und lässt sich mit `AndCondition`,
  `OrCondition` und `NotCondition` verschachteln. Der Adapter nutzt bisher nur `OrCondition` und
  einfache `EditCondition`. SAP beschreibt die `EditCondition` zusätzlich als „Constraint to be
  executed, this is on top of base constraint defined at Attribute Level“ → offen, ob eine
  `Restriction` bei ausgeblendetem Feld noch greift (`TENANT-TEST NÖTIG`).
- Weitere, vom Adapter ungenutzte Bausteine: `AttributeBehavior=SecureAlias` (Feld verweist auf
  den Alias eines Security-Material-Artefakts), `HelpService` (Browse-Dialog für Ressourcen bzw.
  Zertifikate), `xsd:id`/`xsd:idref` (Ressourcenverweis mit Existenzprüfung zur Designzeit).
- Der Maven-Build führt das ADK-`check`-Goal aus
  (`mvn com.sap.cloud.adk:com.sap.cloud.adk.build.archive:check`, laut SAP „used explicitly for
  validation“). Prüfe, was es an den Metadaten tatsächlich validiert.

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
   (XML-escaped) samt Testfällen und prüfe, wo ein anderer SAP-Constraint besser passt (z. B.
   `isValidURIString` für `schemaRegistryUrl`; er lässt aber auch ftp/file/ldap zu, eine eigene
   Regex wie `^https?://.+` kann strenger sein). Nutze `<Minlength>`/`<Length>` für Textfelder.
   Kläre: Verträgt die Regex leere optionale Felder? Wird sie nur erzwungen, wenn das Feld
   sichtbar/editierbar ist? Wie verhält sie sich bei externalisierten Werten (`{{param}}`; die
   Felder sind `isparameterized=true`)? → ggf. `TENANT-TEST NÖTIG`.
2. **Kleine Wertebereiche → `FixedValues` (Dropdown) statt freiem Zahlenfeld** (z. B.
   `producerRetryMaxAttempts` 1–5). Vorteil: ungültige Werte unmöglich. Für Sichtbarkeitslogik
   „Wert ≠ 1“ genügt auch eine `NotCondition`, für „A und nicht B“ eine `AndCondition` (beide
   bisher ungenutzt); vergleiche, was für Nutzer verständlicher ist. Eine editierbare Combo
   (`isEditable`) mit Vorschlagswerten wäre für Zahlen wie `pollingIntervalSeconds` denkbar.
   Prüfe außerdem `AttributeBehavior=SecureAlias` für die Alias-Felder (`credentialAlias`,
   `schemaRegistryCredentialAlias`, `dlqCredentialAlias`, `sslKeystoreAlias`) und
   `xsd:idref`/`HelpService` als Alternative zum 50.000-Zeichen-Inline-`jsonSchema`
   (`TENANT-TEST NÖTIG`: was bewirkt `SecureAlias` zur Designzeit, beim Transport, beim Deployment?).
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

- **SAP-Adapter-Entwicklungsdoku** (Repo `SAP-docs/btp-integration-suite`, Verzeichnis
  `docs/ISuite_Integrations_APIs/`). Lies mindestens diese Dateien vollständig und gleiche den
  Adapter damit ab:
  `component-metadata-550b19e.md` (Metadaten, Constraints, Conditions, `Usage`, `AttributeBehavior`),
  `versioning-rules-for-custom-adapters-61a988b.md`,
  `adapter-development-prerequisites-5638d4a.md` (Header-Regeln, Logging, Manifest/Allowlisting),
  `managing-cluster-lock-in-custom-adapters-b0106a1.md`,
  `enabling-scheduler-support-for-adk-sender-adapter-d423a4b.md`,
  `enabling-connection-status-for-integration-flow-3972bf8.md`,
  `enabling-tracing-for-custom-adapter-a7cafa6.md`,
  `enabling-adk-persistency-958480b.md`,
  `develop-adapters-using-archetype-0a84b13.md` (Build, `check`-Goal, Java/Camel-Stand),
  `importing-custom-integration-adapter-482286e.md`,
  `accessing-user-credentials-e4e4edc.md`, `accessing-trust-and-key-managers-8518837.md`,
  `blueprint-metadata-ab38cc8.md`, `sdk-api-c5c7933.md`,
  `additional-metadata-to-support-edge-integration-cell-f87349e.md`.
  Jede Abweichung des Adapters von diesen Vorgaben ist ein eigener Befund (siehe V47–V52 als
  Startpunkt).
- **SAP-Standard-Kafka-Adapter:** Dokumentation und bekannte Fehlerbilder (Sender/Receiver-
  Konfiguration, Deployment-Fehler, Einschränkungen wie Consumer-Group, Keystore, SCRAM) als
  Vergleichsmaßstab für die Usability. Quellen dafür waren aus der Umgebung nur teilweise
  erreichbar; kennzeichne Nicht-Geprüftes.
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

**Prüfreihenfolge:** zuerst die kritischen und hohen Befunde (V1–V5, V15–V21, V31–V37, V47), dann
mittlere, zuletzt niedrige. Wird dein Kontext knapp, prüfe lieber weniger Befunde gründlich als
alle oberflächlich, und sage im Bericht, welche du nicht mehr geschafft hast. Doppelte Befunde
(z. B. V2/V38, V10/V37, V24/V43, V30/V46) führst du zu einem zusammen.

## V-A: Validierungslücke und Parameter-Abhängigkeiten

**V1 – Nur zwei Felder haben eine `Restriction` (Hoch).**
`metadata-sender-1.3.0.xml:78` (`credentialAlias`), `:145` (`pollingIntervalSeconds`),
`metadata-receiver-1.3.0.xml:73` (`credentialAlias`). Alle anderen `<ErrorMessage>`-Tags haben keine
`Restriction` und sind damit wirkungslos: Issue #44 und die SAP-Doku
(`component-metadata-550b19e.md`: die `ErrorMessage` erscheint, wenn der Constraint der
`Restriction` fehlschlägt) stützen das. Betroffen: Sender `:155, 160, 178, 183, 188, 207, 259, 293,
343, 352, 377, 400, 409`; Receiver `:139, 171, 180, 199, 204, 209, 264, 318, 323, 332`. Sie
versprechen Bereiche („1–51200“, „0–300“, „1–5“, „5–900“), die erst beim Start geprüft werden
(`CpiKafkaPlusConsumer.java:271-279`, `CpiKafkaPlusProducer.java:357-379`). Zu klären: Welche
davon sind als einfache Regex ausdrückbar? `TENANT-TEST NÖTIG` nur noch für die Randfälle (leeres
optionales Feld, ausgeblendetes Feld, externalisierter Wert).

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
- Die Sichtbarkeit „Wert ≠ 1“ ist mit `NotCondition`, „A und nicht B“ mit `AndCondition`
  ausdrückbar (SAP-Doku `component-metadata-550b19e.md`); der Adapter nutzt beides nicht. Damit
  lassen sich z. B. die Retry-Felder bei `producerRetryMaxAttempts=1` ausblenden und
  `enableIdempotence` bei `enableTransactions=true` sperren. (Korrigiert eine frühere Annahme, dass
  nur Gleichheit auf einen Literalwert möglich sei; ein Vergleich wie „> 1“ geht weiterhin nur über
  aufgezählte Werte.)

**V10 – Gefährliche oder unklare Defaults (Mittel).**
`allowedHeaders=*` (`CpiKafkaPlusEndpoint.java:218-220`; `HeaderFilterStrategy.java:45-47` kennt
keine Sperrliste): Prüfe in `addRecordHeaders`, ob `Authorization`, `Cookie`, `SAP_*` und
Camel-interne Header nach Kafka gelangen. `commitStrategy=AUTO` wird ohne Warnung angeboten
(`metadata-sender-1.3.0.xml:587-589`). `autoOffsetReset=latest` überspringt bei neuer Consumer-Group
alle vorhandenen Daten. SAP-Vorgabe (`adapter-development-prerequisites-5638d4a.md`): nur
protokollkonforme oder vom Protokoll benötigte Header propagieren, eine Konfiguration zum Zulassen
weiterer Header anbieten und propagierte Header dokumentieren – ein Default `*` widerspricht dem.

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

## V-E: Receiver (CPI → Kafka) und gemeinsame Sicherheits-/Schema-Registry-Bausteine

Herkunft und Statuslogik wie in V-D (`VORGEPRÜFT` / `GEMELDET`). `UNVERIFIZIERT` = hängt an
Kafka-Client-Interna, die ohne `kafka-clients`-JAR nicht prüfbar waren. Dateien unter
`src/main/java/com/finkeflo/cpi/kafka/`, „Producer“ = `CpiKafkaPlusProducer.java`.

**V31 – Avro-Serialisierung schaltet sich nach einem Producer-Rebuild still ab (Kritisch,
VORGEPRÜFT).** `helpersInitialized` wird nur in `doStop` zurückgesetzt (`Producer:430`).
`closeProducerQuietly` setzt `avroHelper = null` (`:2170`), `ensureHelpersInitializedLocked`
kehrt wegen `helpersInitialized == true` sofort zurück (`:486-488`), und `serializeValue` fällt bei
`avroHelper == null` auf den Roh-Body durch (`:1215-1220`; Batch-Pfad `:1005-1016`). Folge (Herleitung
prüfen, Aufrufpfad `handleSendFailure` → `triggerReconnect` → `closeProducerQuietly`): Nach einem
einzigen Fehler, der einen Rebuild auslöst (z. B. Authentifizierungs- oder Autorisierungsfehler,
unklassifizierter Fehler), landen alle folgenden Nachrichten als **Klartext-JSON ohne Magic Byte und
Schema-ID** im Avro-Topic, mit `CpiKafkaPlusStatus=OK` und ohne Log-Zeile. Zweiter Einstieg:
`ensureHelpersInitialized()` ignoriert sein Ergebnis (`:611`); der Fehlerpfad (`:539-548`) nullt
`avroHelper`, die transaktionale Batch-Variante sendet dann ebenfalls Roh-JSON (GEMELDET). Kein Test
referenziert `avroHelper`/`helpersInitialized` (GEMELDET). Fix-Richtung: Flag gemeinsam mit
`avroHelper` zurücksetzen und bei konfiguriertem Avro ohne Helper hart fehlschlagen.

**V32 – Topic-Probe verliert den Keystore-Alias (Hoch, VORGEPRÜFT).** `buildTopicCheckProperties`
kopiert nur Schlüssel mit `ssl.`, `sasl.` oder `security.protocol` (`Producer:2130-2136`). Der Alias
steckt in `cpi.kafka.ssl.keystore.alias` (`CpiKafkaPlusSslEngineFactory.java:57`), wird also nicht
kopiert, während `ssl.engine.factory.class` durchkommt. `CpiKafkaPlusSslEngineFactory.configure`
wirft dann `IllegalArgumentException("… requires config …")` (`:77-81`). Folge für **jeden** Channel
mit `sslKeystoreAlias` (GEMELDET, herleiten): `AdminClient.create` scheitert, die Probe bleibt
immer INCONCLUSIVE, das Fail-fast bei fehlendem Topic/Auth/TLS greift nie, und der Probe-Fehler wird
jeder Sendefehler-Meldung angehängt (`:1840-1849`), auch bei `RecordTooLarge`. Kein Test für
Alias + Probe gefunden (GEMELDET).

**V33 – Nicht-transaktionaler Batch wird bei Serialisierungsfehler teilweise geschrieben (Hoch,
VORGEPRÜFT).** `ProducerBatchHelper.java:214-222` serialisiert Datensatz *i* innerhalb der
Sende-Schleife; das `try/catch` ab `:243` deckt nur `producer.send`. Wirft der Value-Serializer
(z. B. Avro passt nicht zum Schema) bei Datensatz 5 von 100, sind 0–4 bereits unterwegs, der
Exchange schlägt fehl und ein Wiederholungsversuch des Aufrufers dupliziert sie. Über
`handleSendFailure` wird das außerdem als `UNKNOWN_FATAL` gewertet und löst einen Rebuild aus
(→ V31, GEMELDET). Fix-Richtung: erst alle serialisieren, dann senden.

**V34 – Retry-Duplikatgarantie und Sichtbarkeit abgebrochener Transaktionen (Hoch, GEMELDET).**
- Einzelpfad: `Producer:1066-1069`, `:1078-1117` und `ProducerRetryPolicy.java:56-58, 247-256`
  begründen „kein Duplikat“ mit der Broker-Deduplizierung auf (PID, Sequenz). Die greift nur für
  Kafka-interne Wiederholungen; ein neues `send()` bekommt neue Sequenznummern. Nach einer
  `TimeoutException` (als `RETRIABLE` klassifiziert), bei der der Datensatz doch geschrieben wurde,
  dupliziert der Retry (`UNVERIFIZIERT` gegen das JAR).
- Transaktionaler Pfad: Abgebrochene Versuche sind nur für `read_committed`-Leser unsichtbar. Der
  eigene Sender setzt `isolation.level` nie (→ V26).
- Doku (VORGEPRÜFT): `docs/features/producer-retry.md:50` nennt für das Budget `5–300` (Code:
  5–900, `Producer:90`); `:122` nennt `~970 s` für 120 s / 2 Versuche (Formel ergibt 302 s bzw.
  432 s, → V2/V38); laut Teilreview behauptet die Datei MPL-Statustext, `KafkaAdapterError`-Anhang
  und `retryAttempts` für Receiver-Fehler, der Producer ruft aber `reportFailure` nie auf (Grep im
  Producer: keine Treffer, VORGEPRÜFT; Behauptung in der Doku selbst prüfen).

**V35 – `CamelKafkaTopic` leckt und überschreibt das Ziel-Topic (Hoch, VORGEPRÜFT).**
`ProducerBatchHelper.java:275` setzt `CamelKafkaTopic` nach jedem Batch-Send; `Producer:630` liest
diesen Header als Topic-Override. Ein zweiter Kafka-Receiver im selben Exchange schreibt dadurch in
das vorige Topic (Herleitung prüfen). `resolveTopic` (`:1183-1199`) wertet laut Teilreview außerdem
Simple-Ausdrücke im Header aus; Erreichbarkeit durch nicht vertrauenswürdige Aufrufer
`UNVERIFIZIERT`.

**V36 – `transactional.id` enthält weder iFlow noch Endpoint noch Tenant (Hoch, teils VORGEPRÜFT).**
Form: `prefix-<sha256(topic)[:8]>-<CF_INSTANCE_INDEX|HOSTNAME>-<slot>` (`Producer:319-325`,
`:738-739`). Zwei iFlows oder DEV/TEST-Tenants am selben Cluster mit gleichem Prefix und Topic
kollidieren auf Node 0, Slot 0 und fencen sich gegenseitig (`ProducerFencedException`, standardmäßig
nicht wiederholt; GEMELDET). Der Hash nutzt den *konfigurierten* Topic-Text (`:319`), ein
Ausdruck-Topic hasht den Ausdruck (GEMELDET). Die UI sagt nur „unique prefix“. Prüfe, ob ein Hash
aus `adapterInstanceID`/Endpoint-ID ergänzt werden kann, ohne laufende `transactional.id`s
unkontrolliert zu ändern (Versionsklasse!).

**V37 – `allowedHeaders='*'` ohne Sperrliste (Hoch, GEMELDET; klärt V10).** Übersprungen werden nur
`Camel*`, `org.apache.camel*`, `kafka.*`, `CpiKafkaPlus*` (`Producer:1238-1243`,
`HeaderFilterStrategy.java:45-47`). Weitergegeben werden u. a. `SAP_MessageProcessingLogID`,
`SAP_ApplicationID`, allgemein `SAP_*` sowie jeder `Authorization`-/`Cookie`-/`X-*`-Header auf dem
Exchange (ob CPI-Sender-Adapter `Authorization`/`Cookie` auf dem Exchange belassen:
`UNVERIFIZIERT`). Jeder Topic-Leser sieht sie. Werte gehen per `toString()` (`:1249`), Binärwerte
werden verfälscht. Header aus der Batch-Payload umgehen den Filter und ersetzen gleichnamige
(`ProducerBatchHelper.java:234-241`, in der UI dokumentiert). Der `Camel`-Filter ist
case-sensitiv, Camels Header-Zugriff nicht.

**V38 – Retry-Start-Check im Detail (Mittel–Hoch, teils VORGEPRÜFT; ergänzt V2).**
- `producerRetryMaxAttempts=5` mit Transaktionen und `deliveryTimeoutSeconds=120`: `5×215+4×2 = 1083 s`
  > 900 s Obergrenze → mit den Defaults unmöglich; Lieferzeit müsste ≤ 83 s sein (GEMELDET). Für zwei
  Versuche in 30 s: ohne Transaktionen ≤ 7 s, mit Transaktionen ≤ 2 s (nachgerechnet).
- Auch ein nicht-transaktionaler **Batch**-Channel scheitert: `Producer:391-397` warnt „no effect“,
  `:408-423` wirft trotzdem (VORGEPRÜFT).
- Die Bereichsprüfungen `Producer:359-379` laufen **auch bei ausgeschaltetem Retry**
  (`maxAttempts == 1` kehrt erst danach zurück, `:381-383`). Ein Wert wie `producerRetryDelaySeconds=0`
  blockiert das Deployment, obwohl das Feature aus ist (VORGEPRÜFT; gleiche Klasse wie V3).
- Die in der Doku empfohlenen `deliveryTimeoutSeconds=2` setzen `max.block.ms` und
  `request.timeout.ms` auf 2 s (`ProducerConfigFactory.java:115-125`); für einen kalten
  Wegwerf-Producer mit Verbindungsaufbau, TLS, SASL und `InitProducerId` sehr knapp
  (`UNVERIFIZIERT`).

**V39 – Blockierung von Worker-Threads (Mittel, GEMELDET).** `txnSlotSemaphore.acquire()`
(`Producer:714`) ist unbegrenzt; bei Ausfall warten Threads auf 5 Slots × bis zu 215 s × Versuche,
das Retry-Budget schließt die Slot-Wartezeit nicht ein. Im Shared-Pfad kann eine Nachricht vor jedem
Retry bis rund 70 s blockieren (Topic-Probe 5 s, `close()` bis ~10 s `UNVERIFIZIERT`,
Prewarm-`partitionsFor` 30 s bei jeder Nachricht während eines Fehlers `:1298`, Send 30 s plus Acks)
– mehr als ein typischer Aufrufer-Timeout von 60 s. Der Default `deliveryTimeoutSeconds=120` hat für
nicht-transaktionale Channels keine Obergrenze.

**V40 – Receiver-spezifische Fehlkonfigurationen, die erst beim ersten Send auffallen (Mittel–Hoch,
teils VORGEPRÜFT).**
- `bootstrapServers` leer: NPE bei `Properties.put` (`ProducerConfigFactory.java:91`, VORGEPRÜFT);
  leer oder nur Host: `ConfigException` beim Producer-Bau.
- Credential-Alias fehlt im Secure Store: Start läuft weiter (`SecurityConfigHelper.java:62-81`,
  VORGEPRÜFT), jede Nachricht scheitert mit „Could not find a 'KafkaClient' entry in the JAAS
  configuration“ ohne den Alias-Namen (GEMELDET).
- `securityProtocol` klein geschrieben (nur über externalisierte Parameter): `contains("SASL")`/
  `contains("SSL")` sind case-sensitiv (`SecurityConfigHelper.java:49,53`, VORGEPRÜFT), Kafka
  akzeptiert aber Kleinschreibung → kein JAAS, keine Keystore-Factory. `saslMechanism` null → NPE
  (`:59`).
- `subjectNameStrategy` `RecordNameStrategy`/`TopicRecordNameStrategy` werden von der UI angeboten,
  `AvroSerializerHelper.java:178-186` wirft laut Teilreview bei jeder Nachricht (GEMELDET; die Doku
  nennt nur `TopicNameStrategy` als unterstützt, `docs/configuration.md:209`). Fehlender
  Schema-Registry-Alias → keine Auth → 401 bei der ersten Nachricht.
- `producerBatchMode` unbekannt/leer: Exception erst bei der ersten Nachricht (`Producer:995-1003`).
  `acks="ALL"` wird nur bei aktiver Idempotenz still zu `all` korrigiert
  (`ProducerConfigFactory.java:96`), sonst `ConfigException`.
- Zahlen: `deliveryTimeoutSeconds=0` → `max.block.ms=0`, jeder Send scheitert; `bufferMemoryKb=0`
  und `maxRequestSizeKb=0` scheitern beim Senden. Int-Überlauf bei `*1024`
  (`ProducerConfigFactory.java:103,105`; VORGEPRÜFT, Typ `int`): `5242880` (Bytes statt KB
  eingegeben) ergibt 1 GiB, `4194304` ergibt 0; `deliveryTimeoutSeconds ≥ 2.147.484` läuft bei
  `*1000` (`:111`) negativ.
- Kombinationen: Default `maxRequestSizeKb=5120` liegt über dem `message.max.bytes`-Default eines
  Standard-Brokers (~1 MB; Confluent Cloud abweichend, prüfen) → 1–5-MB-Datensätze scheitern am
  Broker. `bufferMemoryKb` kleiner als `maxRequestSizeKb`/`producerBatchSizeKb` scheitert beim Senden.

**V41 – `enableTransactions=true` mit `producerBatchMode=NONE` ist still nicht-transaktional
(Mittel, VORGEPRÜFT).** Einzelnachrichten laufen über den geteilten, nicht-transaktionalen Producer
(`Producer:604-611`, `:642-647`; `transactionalOnlyPath` schließt `NONE` aus), der Start verlangt
trotzdem Prefix, Slots und rechnet das falsche Worst-Case-Modell. Zusätzlich setzt
`ProducerConfigFactory.java:163-165` `transaction.timeout.ms` anhand des Endpoint-Flags, also auch
für den geteilten Producer; der Code-Kommentar dort sagt, Kafka lehne die Option bei einem
nicht-transaktionalen Producer ab – dann würde diese Kombination jeden Send scheitern lassen
(`UNVERIFIZIERT`, am JAR/Broker testen). Die UI verhindert die Kombination nicht.

**V42 – Schema-Registry-Client (Mittel, GEMELDET).** `SchemaRegistryHttpClient.java`: Basic-Auth geht
an jede konfigurierte URL inklusive `http://`, ohne Schema-/Host-Prüfung und ohne
Redirect-Kontrolle (`:199-209`; ob das JDK `Authorization` bei Redirects weiterreicht,
`UNVERIFIZIERT`); nur JVM-Default-TLS, `sslKeystoreAlias` wird nicht genutzt → Registry mit privater
CA unbenutzbar oder Nutzer weichen auf `http` aus; handgeschriebene JSON-Verarbeitung erwartet die
exakte Form `"schema":"` und dekodiert `\uXXXX` falsch (`:245, 257-264`), eine formatierende
Registry scheitert; „latest“ wird ewig gecacht (`:121-127`), neue Schemaversionen greifen erst nach
Redeploy; Auto-Register leitet das Schema aus der ersten Nachricht ab
(`AvroSerializerHelper.java:157-164`), Feldnamen mit Bindestrich werfen (`:209-211`); der
Avro-JSON-Decoder (`:111`) erwartet Avros JSON-Kodierung (Unions als `{"string":"x"}`), Plain-JSON
für nullable Felder scheitert (`UNVERIFIZIERT`); die URL wird auf INFO mit evtl. Userinfo geloggt
(`:81-83`).

**V43 – Batch-Parser und Re-Serialisierung (Mittel, GEMELDET).** XML_LIST: `BatchParser.java:210,
222, 228` nutzen tiefes `getElementsByTagName`; ein `<record>` in einem `<value>` wird zum
zusätzlichen oder fehlschlagenden Datensatz, ein `<key>` im Payload zum Message-Key. JSON_ARRAY
(`BatchParser.java:52, 106-108`): Standard-`ObjectMapper`, `99.90` → `99.9`, hohe Dezimalpräzision
geht vor Avro/JSON verloren (→ V24, gleiche Ursache auf der Receiver-Seite).

**V44 – Leerer/fehlender Body wird zum Tombstone (Mittel, GEMELDET).** `Producer:1203-1212` liefert
`null` für fehlenden Body, `AvroSerializerHelper.java:101-103` für einen leeren String; der Record
wird trotzdem gebaut (`:1050`). Auf einem Compacted Topic löscht das den Key. Nur der Batch-Modus
dokumentiert Tombstones.

**V45 – Hängende Transaktion nach fehlgeschlagenem Versuch (Mittel, GEMELDET).** `Producer:765-770`
ruft bewusst kein `abort` auf; `close(5s)` (`:871-876`) kann bei unerreichbarem Broker nichts
abbrechen. Das abgeleitete `transaction.timeout.ms` (Standard 150 s, bis 900 s) hält die LSO für
`read_committed`-Leser offen, bis die `transactional.id` wiederverwendet wird oder der Timeout
abläuft.

**V46 – Weitere Niedrig-Befunde (GEMELDET, sofern nicht anders vermerkt).**
- Deploy-Diagnose nur als `WARN` (Topic-Probe `:2020, :2039`), obwohl das Projekt selbst sagt, dass
  `WARN` den Trace nicht erreicht (`:554-556`).
- `docs/features/producer-batch.md:155` sagt „Avro serialization is not supported in batch mode
  (v1)“, der Code wendet Avro im Batch an (`Producer:1005-1016`; VORGEPRÜFT); dort steht außerdem
  „async send + flush“, ein `flush` wurde nicht gefunden.
- `MAX_CONSECUTIVE_SEND_FAILURES` (`Producer:70`) ist laut Teilreview tot, obwohl das Javadoc
  (`:1415`) behauptet, sie steuere den Reconnect.
- `LOG.error` für Nicht-Fehler (Heartbeat `:1397`, `producer.retry.effect` `:985`, `send.recovered`
  `:1359`, SLOW-Prewarm `:1310`, `Endpoint:446`) – Fehlalarme bei ERROR-basiertem Alerting.
- Der transaktionale Pfad ruft nie `publishConnectionStatus`; das „Producer READY“-OK (`:464`)
  bedeutet nur, dass das Client-Objekt existiert.
- `kafkaProducer` ist nicht `volatile`, `triggerReconnect` (`:2146`) nicht synchronisiert; es kann
  einen gerade neu gebauten Producer schließen. Wiederverwendung des Semaphors nach Stop/Start
  `UNVERIFIZIERT`.
- Pro transaktionalem Producer-Bau: Secure-Store-Lookup, Keystore-`SSLContext` und INFO-Logs;
  gleiche `client.id` paralleler Wegwerf-Producer erzeugen vermutlich JMX-AppInfo-WARNs
  (`UNVERIFIZIERT`).
- `CredentialHelper.java:131,163` nullt das `char[]` aus `getPassword()`; liefert CPI ein internes
  Array, könnte ein späterer Lookup Leerwerte liefern (`UNVERIFIZIERT`); `:183-187` prüft Key
  Manager nicht auf `null`.
- Jackson-Parse-Fehlertexte enthalten einen Payload-Ausschnitt in MPL und Logs
  (`BatchParser.java:71-73`, für Jackson 2.22 `UNVERIFIZIERT`).
- `TlsListenerProbe` cached auch `INCONCLUSIVE` dauerhaft (→ V30) und läuft innerhalb von
  `computeIfAbsent` und `synchronized(this)` (`Producer:446, :503`): parallele Erstsends stauen sich
  hinter der Probe (bis 3 s + 3 s je Server, DNS unbegrenzt).

**Vom Teilreview geprüfte Nicht-Probleme (nur bei Gegenindiz neu aufrollen, GEMELDET):**
Hostname-Verifikation bleibt aktiv (`ssl.endpoint.identification.algorithm` wird nie gesetzt, die
Factory wendet Kafkas `https`-Default an, `CpiKafkaPlusSslEngineFactory.java:161-167`); kein
Trust-All (Trust Manager kommen aus dem CPI-`KeystoreService`); JAAS-Escaping von `\` und `"` ist
korrekt (`SecurityConfigHelper.java:103-104`, VORGEPRÜFT); es werden nur Aliase geloggt, nie
Geheimnisse; Slot-Semaphor und Flag-Array sind innerhalb einer Producer-Lebenszeit korrekt; der
Retry-Entscheidungsbaum ist in sich konsistent (stoppt bei `COMMIT`/`COMMITTED`, Fehler wird einmal
gemeldet); Wegwerf-Producer werden im `finally` mit 5 s Timeout geschlossen; XXE ist durch
`disallow-doctype-decl` blockiert.

## V-F: Abweichungen von der SAP-Adapter-Entwicklungsdoku

Quelle: `SAP-docs/btp-integration-suite`, `docs/ISuite_Integrations_APIs/` (Dateinamen je Befund).
Der Abgleich Adapter ↔ Doku ist `VORGEPRÜFT` (Suche im Repo), die *Wirkung* auf der Plattform ist es
meist nicht und braucht einen Tenant-Test oder SAP-Auskunft.

**V47 – Cluster-Lock: Code nimmt ihn an, die Metadaten fordern ihn nicht an (Mittel–Hoch).**
SAP (`managing-cluster-lock-in-custom-adapters-b0106a1.md`): Ein Custom Adapter muss Cluster-Locks
selbst über den `LockManager` holen und dafür in den Metadaten
`<AdditionalMetadata><Name>requiredIntegrationFlowProperties</Name><Value>adapterInstanceID</Value>…`
deklarieren. Alternativ liefert die referenzierte Scheduler-Komponente einen „in-built locking
feature“ (`enabling-scheduler-support-for-adk-sender-adapter-d423a4b.md`: `ReferencedComponents`
mit `sap:Scheduler`, Consumer „extending ScheduledPollingConsumer“, `useDefaultScheduler=false`).
Der Adapter: `useDefaultScheduler="false"` (`metadata-sender-1.3.0.xml:22`), aber **kein**
`ReferencedComponent`, **kein** `AdditionalMetadata`/`requiredIntegrationFlowProperties`, **kein**
`AttributeBehavior` in irgendeiner der acht Metadaten-Dateien (Grep: 0 Treffer) und kein
`LockManager` im Code; der Consumer erbt von Camels `ScheduledPollConsumer`
(`CpiKafkaPlusConsumer.java:55`; ob SAPs „ScheduledPollingConsumer“ derselbe Typ ist:
`UNVERIFIZIERT`). Dennoch behaupten Kommentare, nur der Knoten mit Cluster-Lock pollt und erzeugt
den `KafkaConsumer` (`CpiKafkaPlusConsumer.java:419-423`, `CpiKafkaPlusEndpoint.java:45-49`). Zu
klären mit Belegen, nicht per Annahme: Läuft der Sender auf **allen** Worker-Knoten (dann teilen
sich N Consumer eine Gruppe, und V28 trifft nur unter den Knoten desselben iFlows zu), oder gibt es
einen Lock? Was sagen die E2E-Tests (`tests/e2e/`) dazu? `TENANT-TEST NÖTIG`.
Folgefrage: `adapterInstanceID` wird per `getGlobalOption("adapterInstanceID")` gelesen
(`CpiKafkaPlusConsumer.java:1437`, `ProducerConfigFactory.java:144`, jeweils mit Null-Prüfung), ist
aber nach SAP nur verfügbar, wenn die Metadaten es anfordern. Ist der Wert im Betrieb also leer?
Wäre er (iFlow-eindeutig) eine Lösung für V28 (`group.instance.id`) und V36 (`transactional.id`)?

**V48 – Versionierungspraxis gegen SAPs Regeln (Niedrig–Mittel).**
`versioning-rules-for-custom-adapters-61a988b.md`: Micro nur für Labels/Tooltips, Minor = neues
Feature ohne neue Pflichtfelder, Major = „not supported“. `VERSIONING.md` beschreibt Major als
„Löschen und Neuanlegen, letztes Mittel“. Prüfe, ob frühere Micro-Änderungen SAPs Definition
überschritten haben (z. B. Design-Time-Validierung in 1.0.7, `CHANGELOG.md`), und ob ein künftiger
Bruch (Entfernen von `transactionV2Enabled`, V14) damit überhaupt vorgesehen ist. Laut SAP-Doku
(`component-metadata-550b19e.md`) erfordert das Umbenennen oder Löschen einer Variante vorher ein
Undeploy des Adapters; vgl. `required-metadata-lines.txt`.

**V49 – Java-Stand der Laufzeit (Mittel, UNVERIFIZIERT).** SAP nennt an mehreren Stellen Java 8
(`adapter-development-prerequisites-5638d4a.md`: „The Java standard libraries of Java 8 can be
used“; `sdk-api-c5c7933.md`; Archetype-Voraussetzungen „Java 8“, Camel 3.14 in
`develop-adapters-using-archetype-0a84b13.md`). Der Adapter kompiliert mit `--release 11` und
bettet `kafka-clients` 4.3.1 (Class-File 55) ein; `pom.xml:445-451` begründet das mit „CPI runtime is
>= Java 11“. Prüfe, worauf diese Annahme beruht (SAP-Aussage? nur E2E-Lauf?) und ob ein
Regressionstest sie absichert. Die Doku kann veraltet sein; sage, was du belegen kannst.

**V50 – Tracing gegenüber SAPs Empfehlungen (Niedrig–Mittel).**
`enabling-tracing-for-custom-adapter-a7cafa6.md`: Inbound mit ursprünglicher Payload **und
Headern** tracen, „Security-relevant header values must be obfuscated“, Outbound nach der
Transformation tracen, Trace nur bei aktivem Adapter-Trace, Zeichenkodierung setzen. Der
`AdapterTracingHelper` enthält laut Grep keine Header-Behandlung (nur `addCustomHeaderProperty`
für MPL-Felder, `AdapterTracingHelper.java:189,220`); der Receiver traced den Batch „as received,
before Avro or Schema Registry serialization“ (`CHANGELOG.md`, Abschnitt Unreleased). Bewerte die
Abweichungen und ob sie bewusst sind; Schnittstelle zu V37 (welche Header fließen wohin).

**V51 – Connection-Status-API als Hebel für Konfigurationsfehler (Mittel).**
`enabling-connection-status-for-integration-flow-3972bf8.md`: `IFlowMonitorService.publishEvent`
meldet den Verbindungsstatus an das Integration-Flow-Monitoring. Der Adapter ruft die API
reflektiv auf (`AdapterTracingHelper.java:680-720`); der Consumer veröffentlicht ERROR bei
Init-Fehlern (`CpiKafkaPlusConsumer.java:552`) und OK nach erfolgreichem Poll, der **Producer ruft
`publishConnectionStatus` gar nicht auf** (Grep im Producer: keine Treffer). Frage: Lassen sich die
D2-Fehler aus V5/V23/V40 (fehlender Alias, ungültige Bootstrap-Liste, unbekannter Enum-Wert) beim
Start als rote Verbindung im Monitoring sichtbar machen, statt nur als ERROR-Zeile im Trace?
`TENANT-TEST NÖTIG`: Was genau sieht der Betreiber bei `EventStatus.ERROR` für einen Sender bzw.
Receiver?

**V52 – Laufzeit-Importe und Allowlisting (Niedrig–Mittel).**
`adapter-development-prerequisites-5638d4a.md`: Zur Laufzeit sind nur ADK-APIs, Camel Core und
Logging zugänglich; weitere Pakete müssen gebündelt und importiert werden. Der Adapter importiert
`com.sap.it.api.*` optional (`pom.xml:367`) und setzt `<DynamicImport-Package>*</DynamicImport-Package>`
(`pom.xml:395`) – das ist sehr breit und steht in Spannung zur Allowlist-Regel; die
Class-Space-Probleme #148/#154 gehören in dieselbe Klasse. Der Adapter hängt typisiert an
`com.sap.cloud.adk:generic.api`/`adapter.api` 3.21.0 (`provided`, `pom.xml:70-82`; `CredentialHelper`
importiert `com.sap.it.api.*` direkt), ruft `IFlowMonitorService` und `MessageLog` aber reflektiv
auf (`AdapterTracingHelper.java:189,220,694,719`). Prüfe, warum (Off-Platform-Tests? fehlender
Zugriff im Bundle? Plattform-Versionsunterschiede?), ob ein typisierter Zugriff möglich ist, ob
`DynamicImport-Package: *` eingeschränkt werden kann, und ob die Warmup-Lösung (ADR 0005) robust
gegen künftige Plattformänderungen ist. (Die Stubs unter `src/stubs/` betreffen nur `org.ietf.jgss`
für SASL/GSSAPI, nicht die ADK.)

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
