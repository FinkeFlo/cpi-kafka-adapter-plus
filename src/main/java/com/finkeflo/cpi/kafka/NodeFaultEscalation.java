/*-
 * #%L
 * Kafka Adapter Plus
 * %%
 * Copyright (C) 2026 Florian Kube
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */package com.finkeflo.cpi.kafka;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.finkeflo.cpi.kafka.KafkaErrorHelper.Classification;
import com.finkeflo.cpi.kafka.ProducerBatchHelper.ProducerPath;

/**
 * Node-level fault escalation of the receiver (e4b) with JVM state (c6) and an opt-in thread
 * dump (c6b). Moved out of {@link CpiKafkaPlusProducer}; it logs under the producer's logger
 * category, so log levels and trace filters set for the producer still apply.
 */
final class NodeFaultEscalation {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusProducer.class);

    // ─────────────────────────────────────────────────────────────────────────────
    // e4b: Node-level fault escalation — if the same fault (keyed by exception class +
    // error code) recurs more than N times within T minutes despite mitigation, escalate
    // to make the node problem visible.
    // Thresholds: 5 occurrences in 20 minutes (observed incident was 5 in 18 min).
    // ─────────────────────────────────────────────────────────────────────────────
    private static final int NODE_FAULT_ESCALATION_COUNT = 5;
    private static final long NODE_FAULT_ESCALATION_WINDOW_MS = 20 * 60 * 1000L;
    // c6b: Thread dump caps — AdapterDiagnostics truncates at 8K chars; stay well under.
    // Prefer BLOCKED/WAITING threads (informative for monitor issues) over RUNNABLE.
    private static final int THREAD_DUMP_MAX_THREADS = 20;
    private static final int THREAD_DUMP_MAX_FRAMES = 10;
    private static final int THREAD_DUMP_MAX_CHARS = 5000;
    /** Lock for atomic escalation decisions. The incident involved 5 threads — volatile++ would race. */
    private final Object nodeFaultLock = new Object();
    /** Fault identity = exceptionClass + ":" + errorCode. Keyed to make "same fault" claim true. */
    private String nodeFaultIdentity = null;
    private long nodeFaultWindowStartMs = 0L;
    private int nodeFaultCountInWindow = 0;
    private boolean nodeFaultEscalated = false;

    private final Supplier<String> topic;
    private final BooleanSupplier diagnosticsFull;
    private final Supplier<String> clientId;
    private final LongSupplier clock;

    NodeFaultEscalation(Supplier<String> topic, BooleanSupplier diagnosticsFull,
                        Supplier<String> clientId, LongSupplier clock) {
        this.topic = topic;
        this.diagnosticsFull = diagnosticsFull;
        this.clientId = clientId;
        this.clock = clock;
    }

    /**
     * e4b: Escalate loudly when the same fault recurs more than N times within T minutes
     * on this JVM despite mitigation. This indicates a node-level problem that requires
     * operator attention.
     *
     * <p>Fault identity is keyed by relevant root-cause class + error code, so wrapper
     * exceptions (e.g. RuntimeException) do not collapse different faults into one identity.
     * Five different faults will not escalate; five occurrences of the same fault will.
     *
     * <p>Thresholds: 5 occurrences in 20 minutes. The observed incident was 5 in 18 minutes
     * across a whole worker node. We use 5 and 20 rather than 5 and 18 to avoid escalating
     * on a single retryable blip that happens to cross a window boundary.
     *
     * <p>The window reset, increment and escalation decision are synchronized because the
     * incident involved 5 concurrent threads — volatile++ races under exactly that load.
     */
    void checkNodeFaultEscalation(Classification classification, CpiKafkaPlusErrorCode errorCode,
                                   Throwable cause) {
        // Only track producer-unusable or unknown faults (not retriable or data errors)
        if (classification != Classification.FATAL_PRODUCER_UNUSABLE
                && classification != Classification.UNKNOWN_FATAL) {
            return;
        }

        // Fault identity = relevant root-cause class + errorCode
        Throwable relevantCause = resolveNodeFaultIdentityCause(cause);
        String relevantCauseClass = (relevantCause != null) ? relevantCause.getClass().getName() : "unknown";
        String currentFaultIdentity = relevantCauseClass + ":" + errorCode.code();

        boolean shouldEscalate = false;
        int count = 0;

        synchronized (nodeFaultLock) {
            long now = clock.getAsLong();

            // Reset window if expired OR if a different fault is seen (start fresh tracking)
            boolean windowExpired = (now - nodeFaultWindowStartMs) > NODE_FAULT_ESCALATION_WINDOW_MS;
            boolean differentFault = nodeFaultIdentity != null && !nodeFaultIdentity.equals(currentFaultIdentity);

            if (windowExpired || differentFault) {
                nodeFaultWindowStartMs = now;
                nodeFaultCountInWindow = 0;
                nodeFaultEscalated = false;
                nodeFaultIdentity = currentFaultIdentity;
            }

            nodeFaultCountInWindow++;
            count = nodeFaultCountInWindow;

            if (count >= NODE_FAULT_ESCALATION_COUNT && !nodeFaultEscalated) {
                nodeFaultEscalated = true;
                shouldEscalate = true;
            }
        }

        if (shouldEscalate) {
            emitNodeFaultEscalation(relevantCauseClass, errorCode, classification, count);
        }
    }

    /**
     * Resolves the relevant cause for node-fault identity, skipping wrapper exceptions where
     * possible so that escalation groups by actual root cause.
     */
    private Throwable resolveNodeFaultIdentityCause(Throwable cause) {
        if (cause == null) {
            return null;
        }
        if (cause instanceof Exception) {
            return KafkaErrorHelper.extractKafkaCause((Exception) cause);
        }

        Throwable current = cause;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    /**
     * Emits the node-fault escalation diagnostic with JVM state (c6) and optionally a thread
     * dump (c6b) when diagnosticsLevel=FULL.
     *
     * <p>The JVM state is cheap and always-on. The thread dump is expensive and opt-in.
     */
    private void emitNodeFaultEscalation(String exceptionClass, CpiKafkaPlusErrorCode errorCode,
                                          Classification classification, int count) {
        // Guard against endpoint.getEffectiveTopic() throwing on failure path
        String topic;
        try {
            topic = this.topic.get();
        } catch (Exception e) {
            topic = "<unavailable>";
        }

        AdapterDiagnostics.Event event = AdapterDiagnostics.event("producer.node.fault.escalation")
                .with("faultClass", exceptionClass)
                .with("errorCode", errorCode.code())
                .with("classification", classification)
                .with("countInWindow", count)
                .with("windowMinutes", NODE_FAULT_ESCALATION_WINDOW_MS / 60_000)
                .with("producerPath", ProducerPath.SHARED)
                .withOptional("clientId", clientId.get())
                .withOptional("topic", topic)
                .with("thread", Thread.currentThread().getName())
                .with("advice", "The same fault has recurred " + count + " times within "
                        + (NODE_FAULT_ESCALATION_WINDOW_MS / 60_000) + " minutes on this JVM despite "
                        + "mitigation. This indicates a node-level problem. The node address is available "
                        + "in the trace record's second-to-last #-separated field.");

        // c6: Lightweight JVM state — always-on, cheap
        addJvmState(event);

        // c6b: Thread dump — expensive, opt-in at FULL level only
        boolean diagnosticsFull = false;
        try {
            diagnosticsFull = this.diagnosticsFull.getAsBoolean();
        } catch (Exception e) {
            // Ignore - just don't emit dump
        }

        if (diagnosticsFull) {
            addThreadDump(event);
        }

        AdapterDiagnostics.error(LOG, event, null);
    }

    /**
     * c6: Adds lightweight JVM state to the escalation event.
     *
     * <p>Uses java.lang.management APIs which are available in Java 11 without extra dependencies.
     * Fully guarded so a failure here can never displace the diagnostic it is attached to.
     */
    private void addJvmState(AdapterDiagnostics.Event event) {
        try {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            ThreadMXBean threads = ManagementFactory.getThreadMXBean();
            MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
            MemoryUsage heap = memory.getHeapMemoryUsage();

            event.with("jvmUptimeMs", runtime.getUptime())
                 .with("jvmThreadCount", threads.getThreadCount())
                 .with("jvmPeakThreadCount", threads.getPeakThreadCount())
                 .with("jvmHeapUsedMB", heap.getUsed() / (1024 * 1024))
                 .with("jvmHeapMaxMB", heap.getMax() / (1024 * 1024))
                 .with("jvmAvailableProcessors", Runtime.getRuntime().availableProcessors());
        } catch (Exception e) {
            // Management APIs can throw or be restricted in locked-down containers
            event.with("jvmStateUnavailable", true)
                 .with("jvmStateError", e.getClass().getSimpleName());
        }
    }

    /**
     * c6b: Adds a bounded thread dump to the escalation event.
     *
     * <p>Only called when diagnosticsLevel=FULL. The dump includes lock and monitor information
     * (the entire point — this is what settles monitor-ownership questions). Output is bounded
     * hard: max 20 threads, max 10 frames per thread, max 5000 chars total. BLOCKED and WAITING
     * threads are preferred over RUNNABLE (they are the informative ones for deadlock/contention).
     *
     * <p>Fully guarded so a failure here can never displace the diagnostic it is attached to.
     */
    private void addThreadDump(AdapterDiagnostics.Event event) {
        try {
            ThreadMXBean threads = ManagementFactory.getThreadMXBean();

            // Check if monitor/synchronizer info is supported before requesting it
            boolean monitorSupported = threads.isObjectMonitorUsageSupported();
            boolean syncSupported = threads.isSynchronizerUsageSupported();

            ThreadInfo[] allThreads = threads.dumpAllThreads(monitorSupported, syncSupported);
            if (allThreads == null || allThreads.length == 0) {
                event.with("threadDumpUnavailable", true);
                return;
            }

            // Sort: BLOCKED first, then WAITING, then others — these are the informative ones
            java.util.List<ThreadInfo> sorted = new java.util.ArrayList<>(java.util.Arrays.asList(allThreads));
            sorted.sort((a, b) -> {
                int aPriority = threadStatePriority(a.getThreadState());
                int bPriority = threadStatePriority(b.getThreadState());
                return Integer.compare(aPriority, bPriority);
            });

            StringBuilder dump = new StringBuilder();
            int threadCount = 0;
            boolean truncated = false;

            for (ThreadInfo ti : sorted) {
                if (threadCount >= THREAD_DUMP_MAX_THREADS) {
                    truncated = true;
                    break;
                }
                if (dump.length() >= THREAD_DUMP_MAX_CHARS) {
                    truncated = true;
                    break;
                }

                dump.append("\n[").append(ti.getThreadName()).append("] ")
                    .append(ti.getThreadState());

                if (ti.getLockName() != null) {
                    dump.append(" on ").append(ti.getLockName());
                }
                if (ti.getLockOwnerName() != null) {
                    dump.append(" owned by [").append(ti.getLockOwnerName()).append("]");
                }

                StackTraceElement[] stack = ti.getStackTrace();
                int frameCount = Math.min(stack.length, THREAD_DUMP_MAX_FRAMES);
                for (int i = 0; i < frameCount; i++) {
                    dump.append("\n  at ").append(stack[i]);
                    if (dump.length() >= THREAD_DUMP_MAX_CHARS) {
                        truncated = true;
                        break;
                    }
                }
                if (stack.length > THREAD_DUMP_MAX_FRAMES) {
                    dump.append("\n  ... ").append(stack.length - THREAD_DUMP_MAX_FRAMES).append(" more");
                }

                threadCount++;
            }

            event.with("threadDump", dump.toString())
                 .with("threadDumpThreads", threadCount)
                 .with("threadDumpTruncated", truncated)
                 .with("threadDumpTotalThreads", allThreads.length)
                 .with("threadDumpMonitorInfoSupported", monitorSupported)
                 .with("threadDumpSyncInfoSupported", syncSupported);

        } catch (Exception e) {
            // Thread dump can fail in restricted environments
            event.with("threadDumpUnavailable", true)
                 .with("threadDumpError", e.getClass().getSimpleName());
        }
    }

    /** Returns priority for thread state sorting: lower = more informative for contention analysis. */
    private int threadStatePriority(Thread.State state) {
        switch (state) {
            case BLOCKED: return 0;      // Highest priority - waiting for monitor
            case WAITING: return 1;      // High - in Object.wait() or similar
            case TIMED_WAITING: return 2; // Medium - in sleep/wait with timeout
            default: return 3;           // RUNNABLE, NEW, TERMINATED
        }
    }
}
