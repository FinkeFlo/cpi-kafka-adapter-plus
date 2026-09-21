# ADR 0005: Pre-load the Bundle Class Space to Survive Adapter Updates

## Status
Accepted (implemented on `fix/148-wiring-warmup`, extended with stage 3 on
`fix/154-warmup-imported-classes`)

## Context
Deploying a new version of the adapter to a CPI tenant is an OSGi subsystem *update*: the new
bundle revision is installed and the old one is purged immediately. The integration flows that
were created from the old revision are not restarted — their Camel routes, `KafkaConsumer` and
`KafkaProducer` objects keep running on the old revision's class loader.

That class loader is now dead for anything it has not loaded yet. The first lazy load of a class
fails with `NoClassDefFoundError`/`ClassNotFoundException … bundle wiring … no longer valid`; the
first lookup of a resource (`getResourceAsStream`) returns `null`. HotSpot caches the failed
resolution per call site, so the failure is permanent for that route. Observed signatures from the
production incidents that led to this work:

- `Lz4Compression$Builder` on the first LZ4-compressed batch after an update;
- `CloseOptions` in `KafkaConsumer.close()` on stop, so the member never sent `LeaveGroup` and the
  group waited out `session.timeout.ms`;
- reproduced on a tenant during this work: `SnappyError FAILED_TO_LOAD_NATIVE_LIBRARY` on the first
  snappy batch — snappy-java, zstd-jni and lz4-java extract their native library from the jar via a
  resource lookup on first use.

The adapter's previous reaction (issue #148 in its original form) detected the "bundle wiring"
text, rebuilt the consumer and restarted the Camel route. Both re-use the very same class loader,
so the loop failed again on the next cold load, every ~6 s, until an operator redeployed the flow.

## Options considered

**Revision anchor (rejected, falsified on a tenant).** Keep the old revision alive by holding an
OSGi reference to it. The framework purges the revision on update regardless of references from
inside the bundle; the experiment showed identical failures.

**Import the codec packages from a separate bundle (rejected, infeasible).** Moving the codecs
out of the fat bundle so that their classes are wired at resolve time. The CPI ADK filters
`Import-Package` through an allowlist (`javax.*`, `org.apache.camel*`, `org.osgi.*`, `com.sap.it.api*`
and a few more); codec packages are silently dropped from `SUBSYSTEM.MF`, so the bundle would not
resolve.

**Restart every integration flow from inside the adapter on update (rejected).** The adapter cannot
redeploy flows; a Camel route restart stays on the dead loader. Only a platform-side redeploy
rebinds a flow.

**Leave nothing to load lazily (chosen).** If the old revision has already loaded every class and
extracted every native library it can ever need, the purge has nothing to break. The residual gap
is accepted knowingly: stage 2 loads without initialising, so a resource lookup from a static
initialiser other than the codecs' would still fail. No such path is known in the shipped code,
and `poll.bundle-wiring-invalid` reports one if it appears.

### Reaching the imported classes (issue #154)

Stage 2 covers the bundle's own content. The classes it borrows from other bundles needed a
separate decision:

**Replace `DynamicImport-Package: *` with an explicit `Import-Package` list and pre-load each
imported package's exports via `BundleWiring.getRequiredWires` → `listResources` (rejected).**
The CPI ADK filters `Import-Package` through an allowlist, so the header we declare is not the
header that reaches `SUBSYSTEM.MF` — the same mechanism that already ruled out shipping the codecs
as separate bundles. It would also warm up whole packages instead of the classes we actually use,
which is both slower and less reviewable.

**Constant-pool scan of our own bytecode (chosen).** What the bundle can make the JVM resolve is
written in its class files. Measured on the shipped bundle: 6,931 referenced types, of which 220
are outside the bundle and not `java.*` — two orders of magnitude smaller than stage 2, so the
scan can simply load all of them. Being a set of concrete class names rather than packages, it is
small enough to assert on in `BundleClassWarmupPackagingIT` and to review in a diff.

**Accept and document only (rejected).** The failure mode is the one #148 exists to remove, and
the fix turned out to be cheap.

## Decision
`BundleClassWarmup.ensureStarted` runs once per class space (a `static` guard is automatically per
bundle revision), triggered from the component constructor and from every consumer/producer start:

1. **Stage 1, synchronous** — all `com.finkeflo.cpi.kafka.*` classes, initialised (~80 classes,
   ~250 ms). Covers the adapter's own stop and error paths.
2. **Codecs, synchronous** — `CodecWarmup` performs one compress/decompress round trip per Kafka
   codec through `Compression.of(type)` (gzip, snappy, lz4, zstd; ~60 ms). This is what extracts and
   links the native libraries; class loading alone does not, because the `.so` is a *resource*.
3. **Stage 2, daemon thread** — every `.class` in the bundle root and in each `Bundle-ClassPath`
   jar, enumerated through the OSGi `Bundle` API (reflectively — the adapter has no compile-time
   dependency on `org.osgi`), loaded with `initialize=false`. Codec, consumer and record packages
   go first. ~6,900 classes in ~2 s on a tenant, finished before the first partition assignment.
   50 classes fail by design (grpc, jose4j, joni/graalvm are not shipped) and are logged by name.
4. **Stage 3, same thread, after stage 2** — the classes the bundle *references* but does not
   *contain* (issue #154). The bundle declares `DynamicImport-Package: *`, so classes owned by
   other bundles (Camel, `com.sap.it.api.*`, SLF4J, `javax.*`) are wired lazily on first reference
   and fail on a purged revision exactly like bundle-internal ones. `ConstantPoolScanner` re-walks
   the bundle content and reads the `CONSTANT_Class` entries and the `CONSTANT_NameAndType` /
   `CONSTANT_MethodType` descriptors of every class — together precisely the set the JVM resolves
   lazily — and everything not contained in the bundle is loaded with `initialize=false`. Currently
   220 classes out of 6,931 referenced types; Camel and the SAP APIs go first. Stage 3 runs *after*
   stage 2 on purpose: stage 2 covers the failures actually observed in production and must not be
   delayed by the ~1 s constant-pool scan.

   `java.*` is excluded: the OSGi core specification requires every bundle class loader to
   delegate it to the parent, so those classes never travel over a bundle wire.

The recovery loop is removed. If a poll still fails on a class, link or native-library load
*and* `OsgiBundleInfo.isClassSpaceStale` reports that the route's loader is no longer the
bundle's current `BundleWiring` loader, the consumer stops by error policy with
`poll.bundle-wiring-invalid … action=redeploy the integration flow`, closes the Kafka client
(`close()` now catches `Throwable`, so a `NoClassDefFoundError` cannot skip the remaining cleanup)
and reports the state to the monitor. Every start line carries the class-space fingerprint
(`bundleId`, `bundleVersion`, `bundleLastModified`, `revisions`, `stale`, `loader`).

All warm-up lines are `ERROR` on purpose (ADR 0004: only `ERROR` reaches the tenant trace).

## Verification
Tenant protocol, adapter updated under a running flow each time (fixed build A → fixed build B):

| Probe | Before the fix | After the fix |
|---|---|---|
| First LZ4 batch after update | `poll: FAILED … Lz4Compression$Builder` | passed (2×) |
| First snappy batch after update | `SnappyError FAILED_TO_LOAD_NATIVE_LIBRARY`, endless loop | passed |
| Undeploy flow on purged revision | `STOP_FAILURE … CloseOptions`, member lingers | clean stop, group empty within seconds (4×) |

Stage 3 (issue #154) is verified by the packaging IT and by the `class-warmup.stage3.completed`
line; the tenant probe — update the adapter under a running flow, then force an error path that
touches a Camel class for the first time — is still outstanding.

## Consequences
- Adapter updates no longer lose messages of running flows; flows do not need to be redeployed
  after an update. Cost: ~3.5 s CPU per bundle revision on a low-priority thread (stage 2 ~2.3 s,
  stage 3 ~1 s of which most is the constant-pool scan), plus ~300 ms of synchronous work when the
  first endpoint of a revision starts.
- **First rollout caveat:** flows started on an adapter *without* the warm-up will still fail once,
  when they are updated to the first warm-up-capable version. Redeploy the Kafka flows once after
  that rollout.
- The 50 tolerated stage-2 failures are asserted in `BundleClassWarmupPackagingIT`; a dependency
  upgrade that changes the set fails the build and forces a review. The same IT asserts the size
  and the allowed namespaces of the stage-3 set and writes both as build artifacts
  (`target/warmup-it-failures.txt`, `target/warmup-it-imported.txt`).
- Remaining gaps, by construction:
  - **Resource lookups** other than the codec natives that a Kafka client might perform after
    construction. None are known; if one appears it now ends in a single
    `poll.bundle-wiring-invalid` line with the revision fingerprint instead of a silent loop.
  - **Reflection and service lookups**: a class named only in a string (`Class.forName` on a
    configured serializer, `ServiceLoader`) is invisible to a constant-pool scan. The adapter's own
    configurable class names are resolved during endpoint start-up, while the revision is live.
  - **Second-order references**: stage 3 loads an imported class but not what *that* class
    references, because those class files are not ours to read. Loading resolves its super types
    and interfaces, which is what breaks first; the rest stays with the owning bundle, whose wiring
    an adapter update does not invalidate.
  - **Uninitialised classes**: stages 2 and 3 load with `initialize=false`, so a static initialiser
    that performs a resource lookup on first *use* is still exposed. Stage 1 initialises the
    adapter's own classes and `CodecWarmup` covers the native libraries, the only such paths known.
