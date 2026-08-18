# ADR 0004: Single, Enforced Diagnostic Logging Contract

## Status
Accepted (implemented on `feat/diagnostic-logging` branch), pending review

## Context
When a Kafka operation failed in production, the incident could not be diagnosed from the tenant trace file. An analysis of a real ~870,000-line trace corpus uncovered the reasons. Each was a verified finding, and each was sufficient by itself to make investigation impossible.

**Only `ERROR` reaches the CPI tenant trace file.** 32,708 lines in the corpus came from this adapter and every one of them was `ERROR`, including a `LOG.warn` statement known to execute at runtime. The practical consequence is that log level cannot be used as a severity signal: anything needed for diagnosis must be `ERROR`, and the absence of a `WARN` in the trace proves nothing at all. Producer initialisation failures were originally logged at `WARN` for the first nine attempts and escalated only on the tenth, so the most common real failure class (wrong credentials, TLS mismatch, unreachable broker) produced no visible output until the tenth attempt.

**The trace appender discards the `Throwable`.** A conventional `LOG.error("...", exception)` loses the stack trace twice: the call site often passed only `getMessage()` to begin with, and the appender drops whatever is left. The cause chain must therefore be serialised into the message text.

**The SLF4J format-string trap.** `log.error(renderedText, throwable)` treats the rendered text as a format string. A `{}` inside an exception message (entirely realistic once payloads carry JSON) silently consumes the `Throwable` as a placeholder argument and drops the stack trace. The only safe pattern is a constant format string: `log.error("{}", renderedLine, throwable)`.

**No single grep could retrieve an incident.** Two markers competed. The original marker appeared on exactly four lines, and they were the adapter start failures and endpoint validation messages — the most valuable lines there are. Everything else used the diagnostic marker, so no single grep covered both. Worse, credential and TLS helpers carried no marker at all, placing that entire failure class outside any marker-based search.

**Failures could not be attributed.** The tenant trace format reserves four correlation fields per record and all four were empty on every adapter line in the corpus. A failure could be observed but not tied to a message, an exchange, or a sender.

**The `#` field delimiter is not safe.** Trace records are `#`-separated, but the thread name contains a `#` (e.g. `... thread #429917`), so a record splits into a variable number of fields. Any structured payload must either parse from the end of the line or use a regular expression; fixed column indices cannot work.

**Dead reflective bindings were invisible.** The ADK Message Processing Log binding could not possibly work (it was looked up as a Camel bean rather than through `ITApiFactory`, which is the only way to obtain it), but the failure was reported at `DEBUG` and never reached the tenant trace file. Three additional reflection lookups contained the same class of defect: mismatched parameter types that cause `getMethod` to throw `NoSuchMethodException`, or methods resolved on implementation classes rather than interfaces, which fail at invoke-time with `IllegalAccessException`. Every one was swallowed silently.

**Retries were not provably bounded.** Retrying a JVM-level fault (KAFKA-10902, an `IllegalMonitorStateException` thrown from the Kafka client's metadata wait) is only defensible if the retry terminates under all circumstances. Without explicit bounds and logging, a stuck loop would be invisible.

## Decision
The adapter adopts a single, enforced diagnostic logging contract implemented in `AdapterDiagnostics`. All failure paths use it; bare `LOG.error` calls are prohibited.

### The contract

**One marker, one grep.** Every line carries `[CPI-KAFKA-PLUS-DIAG]`. There is no second marker. Runbooks and alerts match on exactly that string.

**Everything at `ERROR`.** Because the CPI trace appender drops lower levels, anything needed for diagnosis must be `ERROR`. Successes are not logged except after a retry that recovered from a fault; that case is rare enough that a line is justified.

**Flat `key=value` structure.** Values containing spaces are wrapped in single quotes. No nested structures. Newlines and tabs inside values are flattened to spaces so that the diagnostic remains a single physical line.

**The `Throwable` is serialised into `error=`.** The call is `log.error("{}", renderedLine, throwable)` so that braces in the rendered text cannot affect the logging call. The serialisation renders the exception class, message (truncated at 300 characters), the top 12 stack frames, then `CAUSED_BY` for each cause level (up to five levels), and `SUPPRESSED` for try-with-resources close failures (up to three per level). The overall line is truncated at 8,000 characters.

**Failures are attributable.** Every failure line carries `mplId`, `applicationId`, and `exchangeId` (when available), plus `thread` and the producer path. Null-safety is strict: extracting a correlation identifier must never throw and replace the diagnostic it was meant to enrich.

**Binding failures are reported loudly and once.** A dead reflective lookup is reported at `ERROR` on first occurrence, not swallowed at `DEBUG`. Subsequent calls to the same endpoint skip the report.

**Bounded mitigation is explicit.** Retries for the KAFKA-10902 fault are bounded three ways: per record (3), per batch (5), and by wall clock (never past the batch deadline). The worst case for a batch is five extra attempts and roughly 250 ms of added latency. The outcome is always logged, including on success, so a recovered fault is visible.

**Heartbeat is throttled.** The emit-cycle heartbeat that keeps a long-running consumer visible is logged at `ERROR` (for reachability), but no more than once per five minutes. Unthrottled it dominated the tenant log while conveying nothing beyond "still alive".

### What it is not

The contract does not replace the troubleshooting runbook. The ADR records the *why*; the runbook in `docs/troubleshooting.md` is the *how*. Duplicating the field reference and grep recipes here would create two sources that drift apart.

## Considered Alternatives

### Relying on `WARN`/`INFO`/`DEBUG` levels
Rejected. The trace corpus proved these levels never reach the tenant trace file. An adapter that logs meaningful information at `INFO` or `DEBUG` logs nothing visible.

### Relying on Message Processing Log tracing alone
Rejected. MPL tracing was never active in the corpus (it is off by default), and the binding that would have enabled it never worked. Even when enabled, an MPL trace is per-message and cannot surface an adapter-level initialisation failure or a configuration defect that affects all messages.

### Emitting JSON log lines
Rejected, but the argument is finer than it first appears. The obvious objection — that a `#` inside a JSON value would corrupt the record — applies just as much to a `key=value` value, so it does not distinguish the two. The real reason is that JSON's only advantage is machine parseability, and that advantage is already forfeited: the enclosing trace record cannot be split reliably (the thread name contains the delimiter), so a consumer must recover the payload with a regular expression regardless of what is inside it. What remains is the cost, and it falls on a human reading a truncated line under time pressure, for whom a minified JSON blob is materially harder to scan than a flat `key=value` sequence. If a machine-readable channel is ever genuinely needed, the right answer is a second explicit export, not a harder-to-read primary log.

### Using an MDC (Mapped Diagnostic Context)
Rejected. SLF4J's MDC is a thread-local store that an appender can access when formatting. The CPI trace appender does not render MDC entries, so they would be lost identically to the `Throwable`. Additionally, the CPI runtime's thread pools do not propagate MDC context, so an identifier placed on the MDC by the Camel exchange lifecycle is not reliably present on the thread that eventually logs. The contract therefore places all context explicitly in the message text.

### Doing nothing and attaching a debugger
Not a real option. The CPI runtime is a managed service. The customer does not have shell access to a running worker node, and the debugger ports are not exposed. The tenant trace file is the only observation channel.

## Consequences

**Longer log lines.** A line with a full cause chain and 12 frames per level is roughly 1,500–2,500 characters. That is intentional: the alternative is no stack trace at all.

**More `ERROR` volume.** Because `WARN` is invisible, events that would naturally be warnings are now errors. The cost is managed: the heartbeat is throttled (one line per five minutes), binding failures are reported once per endpoint, and producer reinitialisation is logged once per failure.

**Changing the marker invalidates existing runbooks.** Any alerting or monitoring that matched on the old marker will stop matching. The troubleshooting documentation names the new marker and must be updated before the adapter is deployed.

**Maintenance obligation.** Every new failure path must use `AdapterDiagnostics`. A bare `LOG.error` that bypasses the contract is a defect. This is now enforced mechanically: `DiagnosticContractTest` scans the adapter sources at build time and fails if any `LOG.error` or `LOG.warn` call lacks the marker or passes a `Throwable` directly to SLF4J instead of serialising it through `AdapterDiagnostics`.

**Dependency-free.** The contract adds no runtime dependency beyond SLF4J and stays within the Java 11 API level. This is required for an ADK adapter bundle that must load in the CPI OSGi runtime.
