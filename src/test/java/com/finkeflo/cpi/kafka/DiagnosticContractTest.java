package com.finkeflo.cpi.kafka;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Enforces the diagnostic logging contract recorded in ADR 0004 by scanning the adapter sources.
 *
 * <p>The contract exists because of properties of the CPI runtime that no ordinary unit test can
 * observe: only {@code ERROR} reaches the tenant trace file, and the trace appender discards the
 * {@code Throwable} argument of a logging call. A failure path that logs at {@code WARN}, or that
 * passes an exception as a logging argument instead of serialising it, therefore compiles, passes
 * its own tests, and still produces nothing usable in production. That is precisely how the
 * incident behind this branch stayed undiagnosable.
 *
 * <p>These are source-level tests on purpose. The defect they guard against is the *absence* of a
 * call, which behavioural tests cannot see.
 */
public class DiagnosticContractTest {

    private static final String MARKER = "[CPI-KAFKA-PLUS-DIAG]";
    private static final Path SOURCE_ROOT = Paths.get("src/main/java/com/finkeflo/cpi/kafka");

    /** {@code AdapterDiagnostics} defines the contract, so it is the one file allowed to bypass it. */
    private static final String CONTRACT_OWNER = "AdapterDiagnostics.java";

    private static List<Path> adapterSources() throws IOException {
        try (Stream<Path> paths = Files.walk(SOURCE_ROOT)) {
            return paths.filter(p -> p.toString().endsWith(".java"))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    private static String read(Path p) throws IOException {
        return new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
    }

    /**
     * Extracts each complete {@code LOG.error(...)} / {@code LOG.warn(...)} statement, following the
     * call across line breaks by balancing parentheses. String literals are skipped while balancing
     * so that a bracket inside a message cannot terminate the statement early.
     */
    private static List<String> logStatements(String source, String level) {
        List<String> found = new ArrayList<>();
        Matcher m = Pattern.compile("LOG\\." + level + "\\(").matcher(source);
        while (m.find()) {
            int depth = 0;
            boolean inString = false;
            for (int i = m.end() - 1; i < source.length(); i++) {
                char c = source.charAt(i);
                if (inString) {
                    if (c == '\\') {
                        i++;
                    } else if (c == '"') {
                        inString = false;
                    }
                    continue;
                }
                if (c == '"') {
                    inString = true;
                } else if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                    if (depth == 0) {
                        found.add(source.substring(m.start(), i + 1));
                        break;
                    }
                }
            }
        }
        return found;
    }

    @Test
    public void everyErrorAndWarnGoesThroughTheDiagnosticContract() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path source : adapterSources()) {
            if (source.getFileName().toString().equals(CONTRACT_OWNER)) {
                continue;
            }
            String text = read(source);
            for (String level : new String[]{"error", "warn"}) {
                for (String statement : logStatements(text, level)) {
                    if (!statement.contains(MARKER)) {
                        violations.add(source.getFileName() + ": " + firstLine(statement));
                    }
                }
            }
        }
        if (!violations.isEmpty()) {
            fail("These log statements bypass the diagnostic contract of ADR 0004. Every ERROR or "
                    + "WARN must carry the marker " + MARKER + ", normally by being emitted through "
                    + "AdapterDiagnostics. A line without it cannot be found by the single grep the "
                    + "troubleshooting runbook is built on:\n  - "
                    + String.join("\n  - ", violations));
        }
    }

    /**
     * The trace appender drops the {@code Throwable} argument, so an exception handed to SLF4J as a
     * parameter is lost. It has to be serialised into the message text, which is what
     * {@code AdapterDiagnostics.error(Logger, Event, Throwable)} does.
     */
    @Test
    public void noFailurePathPassesAnExceptionAsAPlainLoggingArgument() throws IOException {
        List<String> violations = new ArrayList<>();
        Pattern trailingThrowable = Pattern.compile(",\\s*(e|ex|cause|t|error|throwable)\\s*\\)\\s*$");
        for (Path source : adapterSources()) {
            if (source.getFileName().toString().equals(CONTRACT_OWNER)) {
                continue;
            }
            String text = read(source);
            for (String level : new String[]{"error", "warn"}) {
                for (String statement : logStatements(text, level)) {
                    if (statement.contains("AdapterDiagnostics")) {
                        continue;
                    }
                    if (trailingThrowable.matcher(statement.trim()).find()) {
                        violations.add(source.getFileName() + ": " + firstLine(statement));
                    }
                }
            }
        }
        if (!violations.isEmpty()) {
            fail("These statements pass a Throwable to SLF4J directly. The CPI trace appender "
                    + "discards it, so the stack trace never reaches the tenant trace file. Use "
                    + "AdapterDiagnostics.error(LOG, event, throwable), which serialises the cause "
                    + "chain into the message text:\n  - " + String.join("\n  - ", violations));
        }
    }

    /**
     * A second marker is how the most valuable lines became unreachable once before: the adapter
     * start failures carried a different one, so the obvious grep missed exactly them.
     */
    @Test
    public void onlyOneMarkerExistsInTheAdapterSources() throws IOException {
        Pattern legacy = Pattern.compile("\\[CPI-KAFKA-PLUS\\](?!-DIAG)");
        List<String> offenders = new ArrayList<>();
        for (Path source : adapterSources()) {
            if (legacy.matcher(read(source)).find()) {
                offenders.add(source.getFileName().toString());
            }
        }
        assertTrue("A second log marker has reappeared in " + offenders + ". The troubleshooting "
                + "runbook and any tenant alerting match on " + MARKER + " alone, so a competing "
                + "marker silently hides those lines from an investigation.", offenders.isEmpty());
    }

    /**
     * Guards the reason the previous ADK bindings were dead: a {@code Method} resolved on a
     * platform implementation class fails at invoke time with {@code IllegalAccessException}
     * because the class is not public. Interfaces are the only safe lookup target.
     */
    @Test
    public void adkReflectionResolvesOnInterfacesNotImplementationClasses() throws IOException {
        String tracing = read(SOURCE_ROOT.resolve("AdapterTracingHelper.java"));
        assertFalse("An ADK lookup targets a '.impl.' class. Methods resolved on a non-public "
                        + "implementation class throw IllegalAccessException at invoke time, which is "
                        + "how four bindings in this class stayed dead without anyone noticing.",
                tracing.contains(".impl."));
        assertFalse("An ADK lookup targets a class whose name ends in 'Impl', for the same reason.",
                Pattern.compile("Class\\.forName\\(\"[^\"]*Impl\"\\)").matcher(tracing).find());
    }

    /**
     * Every instrumentation entry point must have at least one caller outside the class that
     * declares it.
     *
     * <p>This guards against the failure mode found in the reference adapter surveyed while
     * planning this work: it declares a {@code registerRuntimeStatus} method with an empty body and
     * a {@code fireChannelFailed} method with no callers at all. Both read as instrumentation in
     * review, and both emit nothing. That is worse than no instrumentation, because it removes the
     * incentive to add the real thing.
     *
     * <p>The same risk applies here. Every method listed below writes to a channel that is invisible
     * in local tests — the Message Processing Log, the message status, the iFlow monitor — so if a
     * refactoring drops the last call site, nothing fails and nobody finds out until the next
     * incident produces an empty monitor entry.
     */
    @Test
    public void everyInstrumentationEntryPointHasACallerOnAProductionPath() throws IOException {
        String[] entryPoints = {
                "traceError",
                "reportFailure",
                "publishConnectionStatus",
        };

        List<Path> sources = adapterSources();
        List<String> orphans = new ArrayList<>();

        for (String method : entryPoints) {
            Pattern declaration = Pattern.compile("(?:void|boolean|[A-Za-z<>\\[\\]]+)\\s+"
                    + Pattern.quote(method) + "\\s*\\(");
            Pattern invocation = Pattern.compile("\\." + Pattern.quote(method) + "\\s*\\(");

            String declaringFile = null;
            for (Path p : sources) {
                if (declaration.matcher(read(p)).find()) {
                    declaringFile = p.getFileName().toString();
                    break;
                }
            }

            int externalCallers = 0;
            for (Path p : sources) {
                if (p.getFileName().toString().equals(declaringFile)) {
                    continue;
                }
                if (invocation.matcher(read(p)).find()) {
                    externalCallers++;
                }
            }

            if (externalCallers == 0) {
                orphans.add(method + (declaringFile == null
                        ? " (no declaration found — was it renamed?)"
                        : " (declared in " + declaringFile + ", never called from elsewhere)"));
            }
        }

        assertTrue("Instrumentation exists but nothing invokes it, so it can never emit anything:\n"
                        + String.join("\n", orphans)
                        + "\nEither wire it to a real failure path or delete it. Scaffolding that "
                        + "looks instrumented and is not is the specific trap this test exists for.",
                orphans.isEmpty());
    }

    /**
     * Every ADK method that is resolved reflectively must also be invoked.
     *
     * <p>Reflective calls escape the previous test, because the method name is a string rather than
     * a symbol the compiler can see. They also fail more quietly than direct calls: resolving a
     * {@code Method} and then never invoking it compiles, runs, throws nothing, and writes nothing.
     * Four bindings in this adapter were dead for the entire life of a deployment for a closely
     * related reason, so the resolve-without-invoke shape is worth pinning explicitly.
     *
     * <p>Whether the enclosing method is itself reachable is covered by the test above, which
     * requires {@code reportFailure} and {@code traceError} to have callers.
     */
    @Test
    public void everyReflectivelyResolvedAdkMethodIsAlsoInvoked() throws IOException {
        String tracing = read(SOURCE_ROOT.resolve("AdapterTracingHelper.java"));

        Matcher m = Pattern.compile(
                "Method\\s+(\\w+)\\s*=\\s*[\\w.]+\\s*\\n?\\s*\\.?getMethod\\(\\s*\\n?\\s*\"(\\w+)\"")
                .matcher(tracing);

        List<String> resolvedNever = new ArrayList<>();
        int resolved = 0;
        while (m.find()) {
            resolved++;
            String variable = m.group(1);
            String adkMethod = m.group(2);
            if (!Pattern.compile(Pattern.quote(variable) + "\\s*\\.invoke\\s*\\(").matcher(tracing).find()) {
                resolvedNever.add(adkMethod + " (resolved into '" + variable + "', never invoked)");
            }
        }

        assertTrue("No reflective ADK lookups were found at all. Either the binding was rewritten "
                + "or this test's pattern has drifted; both need looking at.", resolved > 0);

        assertTrue("An ADK method is resolved but never invoked, which writes nothing while looking "
                        + "correct in review:\n" + String.join("\n", resolvedNever),
                resolvedNever.isEmpty());
    }

    private static String firstLine(String statement) {
        int nl = statement.indexOf('\n');
        String head = nl < 0 ? statement : statement.substring(0, nl);
        return head.length() > 120 ? head.substring(0, 120) + "..." : head;
    }
}
