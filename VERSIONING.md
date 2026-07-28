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
2. **Never edit or delete a released metadata file.** A new version = a **new** file (one variant per file); old files stay frozen in the bundle → backward compatibility.
3. **Transport order:** per tenant deploy the **adapter first**, then the iFlows. Otherwise "Route has no inputs" / "Not supported yet".
4. **Transporting the adapter via CTS+? Upload it through the Integration Suite UI (update-in-place), not the API.** UI path (keeps the workspace `reg_id` stable): package → adapter → *Actions* → *View metadata* → *Edit* → upload the new ESA → *Save* → *Deploy*. The API import path (delete + import) regenerates the `reg_id` on every deploy, which then makes CTS+ transports fail on the target with `UniquenessViolationException` (see SAP KBA 3003834). If the `reg_id`s are already out of sync, a one-time fix is to delete the adapter on the **target** design-time (runtime untouched) and let the transport re-import it.

## How to version

- **Micro:** bump `version::` in the **same** variant file + set `config.adk`.
- **Minor:** copy the variant file → `metadata-<sender|receiver>-<new>.xml` (one variant), set its `version::`, leave the old file **untouched**, set `config.adk`.
- Then run `mvn test` — the build's consistency guard verifies the version is aligned across `config.adk`, `pom.xml`, and the metadata files.

## How to release (GitHub Actions)

The repository features a fully automated release pipeline (`.github/workflows/release.yml`). You never need to build or upload the `.esa` file manually, nor do you need to edit version numbers in the code files or changelog heading manually.

To publish a new release:
1. Ensure all changes are merged into the `main` branch. All release notes must be written under the `## [Unreleased]` section of [CHANGELOG.md](file:///Users/fkube/github/sap-cpi-kafka-adapter-plus/CHANGELOG.md).
2. Create and push a git tag on the `main` branch:
   ```bash
   git tag v1.0.17
   git push origin v1.0.17
   ```
3. The GitHub Actions release bot will automatically:
   - Extract the version from the tag (e.g., `1.0.17`).
   - Bump version settings in `config.adk`, `pom.xml`, and the metadata XML files in the repository.
   - Rename the `## [Unreleased]` heading to `## [1.0.17] - YYYY-MM-DD` in `CHANGELOG.md`.
   - Commit and push these changes back to the `main` branch.
   - Re-target the tag to the version-bump commit.
   - Run the build and tests, build the `.esa` archive, and publish the GitHub Release with the correct release notes.
