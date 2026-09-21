# ADR 0005: Pre-load the Bundle Class Space to Survive Adapter Updates

## Status
Accepted (implemented on `fix/148-wiring-warmup`), pending review

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
   61 classes fail by design (grpc, jose4j, joni, asm are not shipped) and are logged by name.

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

## Consequences
- Adapter updates no longer lose messages of running flows; flows do not need to be redeployed
  after an update. Cost: ~2.3 s CPU per bundle revision on a low-priority thread, plus ~300 ms of
  synchronous work when the first endpoint of a revision starts.
- **First rollout caveat:** flows started on an adapter *without* the warm-up will still fail once,
  when they are updated to the first warm-up-capable version. Redeploy the Kafka flows once after
  that rollout.
- The 61 tolerated stage-2 failures are asserted in `BundleClassWarmupPackagingIT`; a dependency
  upgrade that changes the set fails the build and forces a review.
- Not covered by construction: resource lookups other than the codec natives that a Kafka client
  might perform after construction. None are known; if one appears it now ends in a single
  `poll.bundle-wiring-invalid` line with the revision fingerprint instead of a silent loop.
