# Issue #14 Implementation Plan: Real OSGi Resolution Test

Scope: implement a **real OSGi resolve check** for the built adapter artifact and close the gap described in [#14](https://github.com/FinkeFlo/cpi-kafka-adapter-plus/issues/14).

## Goal

Catch CPI deployment blockers that static manifest checks can miss, especially:

- unresolved mandatory `Import-Package`
- unresolved standalone bundles emitted into the `.esa`
- resolver problems similar to the historical `at.yawk.lz4` case

## Work Plan

- [ ] **Step 1: Resolve test placement and artifact lifecycle**
  - Decide where the new test runs so `.esa` is guaranteed to exist.
  - If needed, run it in a CI path that executes `mvn install` (not only `verify`).

- [ ] **Step 2: Resolver strategy spike (Felix vs. bnd)**
  - Implement a short spike and pick one resolver approach.
  - Document resolver config and limits (system packages/boot delegation behavior).
  - Validate that the chosen setup can fail on a deliberately unresolvable bundle.

- [ ] **Step 3: Add OSGi resolver IT for ESA standalone bundles**
  - Add a new IT (e.g. `OsgiFrameworkResolveIT`) in `src/test/java/...`.
  - Locate built `.esa`, enumerate included bundles, install them in resolver runtime.
  - Execute resolve and fail with actionable diagnostics for unresolved constraints.

- [ ] **Step 4: Add a deliberate negative guard**
  - Add one controlled failing bundle/input in test scope and assert resolver failure.
  - Ensures the test is not a false-green smoke check.

- [ ] **Step 5: CI integration**
  - Wire the new IT into CI in the job where `.esa` is present.
  - Keep runtime bounded with explicit timeout and stable logging.

- [ ] **Step 6: Documentation and closure**
  - Document what this test guarantees and what it does not.
  - Link implementation PR to issue #14 and close once acceptance checks pass.

## Acceptance Checks

- [ ] With current mainline configuration, resolver IT passes in CI.
- [ ] With the `at.yawk.lz4` exclusion removed (temporary validation branch), resolver IT fails for the expected unresolved constraint.
- [ ] Failure output identifies the unresolved bundle/import clearly enough for triage.
- [ ] Existing functional/unit/integration suites remain green.
