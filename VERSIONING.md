# Versioning — Kafka Adapter Plus

The adapter version lives **manually** in `config.adk` (`Adapter-Version=MAJOR.MINOR.MICRO`).
It is the single source of truth; the Maven build keeps `pom.xml` in sync with it.
It is **not** computed from git/commits, and the metadata is **not** stamped.

## Which number when — and what it does

| You change … | Bump | Example | Effect on existing iFlows |
|---|---|---|---|
| **Bugfix / runtime code / label** | **MICRO** | 1.0.0 → 1.0.**1** | **Seamless** — every iFlow on the line picks it up automatically (no click, no recreate). |
| **New optional feature / parameter** | **MINOR** | 1.**0**.0 → 1.**1**.0 | Old iFlows keep running; adopt via **"Update Version"** (one click) or leave them on the old minor. |
| **Incompatible / breaking change** | **MAJOR** | **1**.x → **2**.0 | **No auto-update**: delete + recreate the channel. Last resort only. |

**Rule of thumb:** the **MAJOR version is the compatibility boundary.**
Within the same major everything is compatible (micro automatic, minor one-click) —
a **major jump breaks** auto-migration.

## Version format — three integer parts only

The adapter version **must** be exactly `MAJOR.MINOR.MICRO` (three integers, e.g. `1.0.3`).
Do **not** try to append a fourth segment or a qualifier such as `1.0.3.6`,
`1.0.3-6`, or `1.0.3.8-g2d51fd3` — even though the raw OSGi spec would allow a
`major.minor.micro.qualifier` form.

- **Why it fails:** the SAP adapter-build plugin's `VersionComparator`
  (`com.sap.cloud.adk.checks.VersionComparator.formatVersion`, reached via
  `AdapterProjectChecks.checkForDuplicateMetadataVariants`) assumes a strict
  three-part version. Given a 4th segment it spins in an **endless loop**
  (100 % CPU, no `.esa` ever produced) — the build hangs instead of erroring out.
  Verified empirically: setting `version::1.0.3.6` in the metadata files reproduces
  the hang; the dash vs. dot in the qualifier is irrelevant — the 4th segment alone
  triggers it.
- **Consequence:** the version shown as **"Version" in the CPI UI cannot carry a
  build/commit suffix.** It is the OSGi `Subsystem-Version`, which the ADK derives
  from the metadata adapter-variant `version::` (kept in sync with
  `Adapter-Version` in `config.adk`). It can only ever be a plain `X.Y.Z`.
- **To distinguish preview/CI builds** use the **`.esa` file name** instead (the ESA
  preview workflow stamps it via `git describe`, e.g. `…-1.0.3-8-g2d51fd3-<branch>.esa`).
  The file name has no OSGi/ADK constraints and does not affect channel compatibility,
  so the CPI-visible version stays a stable `X.Y.Z`.

## Iron rules

1. **Bugfixes = MICRO.** Never put a fix into a new minor — existing iFlows would **not** receive it.
2. **Never edit or delete a released metadata file.** A new version = a **new** file (one variant per file); old files stay frozen in the bundle → backward compatibility.
3. **Transport order:** per tenant deploy the **adapter first**, then the iFlows. Otherwise "Route has no inputs" / "Not supported yet".
4. **Transporting the adapter via CTS+? Upload it through the Integration Suite UI (update-in-place), not the API.** UI path (keeps the workspace `reg_id` stable): package → adapter → *Actions* → *View metadata* → *Edit* → upload the new ESA → *Save* → *Deploy*. The API import path (delete + import) regenerates the `reg_id` on every deploy, which then makes CTS+ transports fail on the target with `UniquenessViolationException` (see SAP KBA 3003834). If the `reg_id`s are already out of sync, a one-time fix is to delete the adapter on the **target** design-time (runtime untouched) and let the transport re-import it.

## How

- **Micro:** bump `version::` in the **same** variant file + set `config.adk`.
- **Minor:** copy the variant file → `metadata-<sender|receiver>-<new>.xml` (one variant), set its `version::`, leave the old file **untouched**, set `config.adk`.
- Then run `mvn test` — the build's consistency guard verifies the version is aligned across `config.adk`, `pom.xml`, and the metadata files.
