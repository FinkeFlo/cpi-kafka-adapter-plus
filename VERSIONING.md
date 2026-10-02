# Versioning — Kafka Adapter Plus

The adapter version lives **manually** in `config.adk` (`Adapter-Version=MAJOR.MINOR.MICRO`).
It is the single source of truth; the Maven build keeps `pom.xml` in sync with it.
It is **not** computed from git/commits, and the metadata is **not** stamped.

## Which number when — and what it does

| You change … | Bump | Example | Effect on existing iFlows |
|---|---|---|---|
| **Bugfix / runtime code / label** | **MICRO** | 1.0.0 → 1.0.**1** | **Seamless** — every iFlow on the line picks it up automatically (no click, no recreate). |
| **New optional feature / parameter** | **MINOR** | 1.**0**.0 → 1.**1**.0 | Old iFlows keep running; adopt via **"Update Version"** (one click) or leave them on the old minor. The click only works if the new line keeps every fixed value of the old one (iron rule 3). |
| **Incompatible / breaking change** | **MAJOR** | **1**.x → **2**.0 | **Not supported by SAP** for custom adapters ("Incompatible Changes — This is not supported", SAP Help: *Versioning Rules for Custom Adapters*). There is no migration path: avoid it. |

**Rule of thumb:** stay **compatible within major version 1**: micro automatic, minor one-click.
SAP offers no major-version mechanism for custom adapters, so an incompatible change has no
supported way to reach existing iFlows — design every change so that it does not need one.

## Version format & preview builds

The version is always `MAJOR.MINOR.MICRO` — three integers, no fourth segment or
qualifier (`1.0.3`, never `1.0.3.6` or `1.0.3-g2d51fd3`). This is exactly what the
CPI UI shows: the OSGi `Subsystem-Version`, derived from the metadata
adapter-variant `version::`, kept in sync with `Adapter-Version` in `config.adk`.

**Preview / CI builds keep the released version in the CPI UI and are told apart by
the ESA file name.** The ESA-preview workflow stamps that name with `git describe`
(e.g. `cpi-kafka-adapter-plus-1.0.3-8-g2d51fd3-<branch>.esa`), which has no OSGi/ADK
constraints and does not affect channel compatibility. So the deployed file is always
identifiable, while the CPI-visible version stays a clean `X.Y.Z`.

We deliberately do **not** bump the CPI version for previews. There is no integer
`MICRO` between a release and its successor — nothing sits between `1.0.3` and
`1.0.4` — so any *distinct* preview number would be **≥ the next release**. Because
micro versions auto-migrate onto existing iFlows within the same major, a stale
preview like `1.0.36` would then outrank a later real `1.0.4`. Keeping previews on the
released version avoids that trap entirely.

## Iron rules

1. **Bugfixes = MICRO.** Never put a fix into a new minor — existing iFlows would **not** receive it.
2. **A change in place must not invalidate a saved value.** A micro edits the variant file that
   every iFlow on the line already uses, without an "Update Version" click. It may change labels,
   tooltips and the visibility of fields (hide a field only if the runtime ignores it while hidden),
   but nothing that turns a value an iFlow has already saved
   into an invalid one: no new `Restriction` that rejects a value the runtime accepted, no removed
   `FixedValue`, no new mandatory field. Such a change needs a new minor line — and until that line
   is released, its new files are free to change.
3. **A new minor line must keep every `FixedValue` of the lines before it.** "Update Version" compares
   the variant *definitions*, not the values a channel stores: if an older line offers a fixed value
   that the new line no longer has, CPI refuses the update for **every** channel on that older line
   ("cannot be updated from 1.0 to 1.4 as it contains incompatible changes. Delete and recreate
   manually"), even for channels that never selected that value. A single `FixedValue` cannot be
   hidden, so to retire an option, keep it in the list, relabel it as legacy, and keep the runtime
   accepting it. This happened with `SPLIT_EXCHANGES`, which 1.2.0 removed from `batchOutputFormat`:
   sender channels on 1.0 and 1.1 can never use "Update Version" and have to be recreated (#219).
4. **Never edit or delete a released metadata file of a *superseded* line.** A new **significant** version (`MAJOR.MINOR`) = a **new** file (one variant per file); old files stay frozen in the bundle → backward compatibility. A **micro** bump is the exception and *must* edit its file in place — see below.
5. **Transport order:** per tenant deploy the **adapter first**, then the iFlows. Otherwise "Route has no inputs" / "Not supported yet".
6. **Transporting the adapter via CTS+? Upload it through the Integration Suite UI (update-in-place), not the API.** UI path (keeps the workspace `reg_id` stable): package → adapter → *Actions* → *View metadata* → *Edit* → upload the new ESA → *Save* → *Deploy*. The API import path (delete + import) regenerates the `reg_id` on every deploy, which then makes CTS+ transports fail on the target with `UniquenessViolationException` (see SAP KBA 3003834). If the `reg_id`s are already out of sync, a one-time fix is to delete the adapter on the **target** design-time (runtime untouched) and let the transport re-import it.

## How to version

- **Micro:** bump `version::` in the **same** variant file — at **both** the `ComponentId` (line 2) and the `VariantId` (inside `<Variant>`) — + set `config.adk`. Never create a second file for a micro: the ADK check rejects it with *"All Receivers should have different significant version (X.X. Micro version ignored)"*, because the micro is not part of a variant's identity. That is also why editing in place is safe — deployed iFlows are pinned to the significant version, so they pick the change up without an "Update Version" click. This matches SAP's own rule ("micro version updates are intended for small changes such as label or tooltip updates … the existing metadata variant must be updated").
  - If the direction's line lags behind `config.adk` (e.g. receiver on `1.1.1` while the adapter is `1.2.0`, because the last minor was sender-only), the frozen-file guard will flag the in-place edit. Re-record that file's SHA-256 in `src/test/resources/released-metadata-checksums.txt` — the significant version is unchanged, so nothing breaks.
- **Minor:** copy the variant file → `metadata-<sender|receiver>-<new>.xml` (one variant), set its `version::`, leave the old file **untouched**, set `config.adk`, and add the new `MAJOR.MINOR` to `src/test/resources/required-metadata-lines.txt`.
  - **Naming convention (new files only): name the file after the line's *baseline*** — the first micro of that line, e.g. `metadata-sender-1.3.0.xml` for the `1.3` line. The filename records where the line *started*; the running micro lives in `version::` inside the file and is never reflected in the name. Do not rename existing files to match: the frozen-file guard keys its SHA-256 records by filename, so a retroactive rename buys cosmetics at the price of touching the very guard that protects released metadata. `metadata-*-1.2.8.xml` predates this convention and stays as it is.
  - The release workflow enforces this: it only rewrites metadata in place when `MAJOR.MINOR` is unchanged, and aborts a minor bump for which no new metadata file declares the new version — rewriting the outgoing file instead would silently drop its line and leave every iFlow bound to it undeployable (`This component … is not supported in Cloud Integration profile`). Once the new files are in place it registers the new line and freezes the superseded ones itself.
- Then run `mvn test` — the build's consistency guard verifies the version is aligned across `config.adk`, `pom.xml`, and the metadata files, that released files stay byte-identical, and that every line listed in `required-metadata-lines.txt` still ships. `mvn verify` additionally runs the ADK's own `check` goal, which is what catches duplicate variants.

## How to release (GitHub Actions)

The repository features a fully automated release pipeline (`.github/workflows/release.yml`). You never need to build or upload the `.esa` file manually for a release. 

To publish a new release:
1. Complete the version bumps described above (`config.adk`, metadata files) and commit them to a new branch.
2. Ensure you have added a corresponding section in `CHANGELOG.md` with the exact version number (e.g., `## [1.0.13] - YYYY-MM-DD`).
3. Create a Pull Request and merge these changes into `main` (since `main` is a protected branch).
4. After merging, tag the commit on `main` and push the tag to trigger the pipeline:

```bash
git tag v1.0.13
git push origin v1.0.13
```

The GitHub Action will automatically:
- Verify that your tag matches the version in `config.adk`.
- Run the full build and tests (`mvn clean install`).
- Extract the release notes for this specific version from `CHANGELOG.md`.
- Create a GitHub Release and attach the compiled `.esa` file.
