package com.finkeflo.cpi.kafka;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounds how long a worker thread may wait for a Kafka send to complete.
 *
 * <p>Kafka enforces {@code delivery.timeout.ms} from inside its own sender thread. When that thread
 * dies, nothing expires the pending record futures any more and an unbounded
 * {@link Future#get()} blocks the calling worker thread forever — which CPI eventually reports as
 * {@code Node Crashed} instead of a usable error.
 *
 * <p>The practical trigger is a TLS mismatch: with a plaintext Security Protocol against a
 * TLS-only broker, Kafka reads the broker's SSL handshake bytes as a Kafka frame header and tries
 * to allocate a frame of several hundred megabytes. The resulting {@link OutOfMemoryError} kills
 * the sender thread silently.
 *
 * <p>This guard therefore waits at most {@code delivery.timeout.ms} plus a margin. Kafka's own
 * error reporting still wins whenever it works; the guard only converts the "sender thread is gone"
 * case into a normal, catchable exception that names the likely cause.
 */
final class ProducerSendGuard {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerSendGuard.class);

    /**
     * Added on top of {@code delivery.timeout.ms} so Kafka always gets the chance to report a
     * delivery failure itself. Only a sender thread that stopped working altogether runs into it.
     */
    static final long GUARD_MARGIN_MS = 30_000L;

    private final long budgetMs;
    private final String securityProtocol;

    private ProducerSendGuard(long budgetMs, String securityProtocol) {
        this.budgetMs = budgetMs;
        this.securityProtocol = securityProtocol;
    }

    static ProducerSendGuard forEndpoint(CpiKafkaPlusEndpoint endpoint) {
        long deliveryMs = Math.max(0L, (long) endpoint.getDeliveryTimeoutSeconds() * 1000L);
        return new ProducerSendGuard(deliveryMs + GUARD_MARGIN_MS, endpoint.getSecurityProtocol());
    }

    /** Visible for testing. */
    static ProducerSendGuard of(long budgetMs, String securityProtocol) {
        return new ProducerSendGuard(budgetMs, securityProtocol);
    }

    long getBudgetMs() {
        return budgetMs;
    }

    /**
     * @return the wall-clock deadline shared by all sends of one exchange, so a batch cannot
     *         multiply the budget by its record count.
     */
    long newDeadline() {
        return System.currentTimeMillis() + budgetMs;
    }

    /**
     * Waits for one send result until {@code deadlineMs}.
     *
     * @throws SendStalledException if the deadline passes without Kafka completing the future
     */
    RecordMetadata await(Future<RecordMetadata> future, long deadlineMs, String description)
            throws Exception {
        long remaining = deadlineMs - System.currentTimeMillis();
        if (remaining <= 0L) {
            remaining = 1L;
        }
        try {
            return future.get(remaining, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new SendStalledException(stalledMessage(description), e);
        }
    }

    /**
     * Waits for all futures against a single shared deadline, logging instead of throwing. Used on
     * abort paths, where the send outcome is only needed for diagnostics and must never replace the
     * original failure. Kafka's own send futures cannot be cancelled reliably, so a stalled future
     * stops the diagnostic drain without claiming that buffered records were released.
     *
     * <p>b4: Failures are now logged at ERROR with full stack traces via {@link AdapterDiagnostics},
     * because only ERROR reaches the CPI tenant trace file. A swallowed send failure is exactly how
     * the original production incident stayed invisible.
     */
    void awaitAllQuietly(List<Future<RecordMetadata>> futures, long deadlineMs) {
        for (int i = 0; i < futures.size(); i++) {
            try {
                await(futures.get(i), deadlineMs, "Buffered record " + i);
            } catch (SendStalledException e) {
                // b4: Upgrade to ERROR with full stack trace — swallowed send failures stayed invisible
                AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.send.stalled")
                        .with("recordIndex", i)
                        .with("budgetMs", budgetMs)
                        .with("securityProtocol", securityProtocol), e);
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // b4: Upgrade to ERROR with full stack trace — swallowed failures stayed invisible
                AdapterDiagnostics.error(LOG, AdapterDiagnostics.event("producer.send.abort.failed")
                        .with("recordIndex", i)
                        .with("phase", "awaitAllQuietly"), e);
            }
        }
    }

    String stalledMessage(String description) {
        StringBuilder sb = new StringBuilder();
        sb.append(description)
                .append(" was not completed by Kafka within ").append(budgetMs).append(" ms. ")
                .append("The producer's sender thread stopped reporting progress, so the send can ")
                .append("neither succeed nor time out on its own. ");
        if (isPlaintext(securityProtocol)) {
            sb.append("The usual cause is a TLS mismatch: Security Protocol '")
                    .append(securityProtocol)
                    .append("' sends unencrypted traffic, but the broker expects TLS. ")
                    .append("Use SASL_SSL (or SSL) instead.");
        } else {
            sb.append("Check broker availability and the Security Protocol '")
                    .append(securityProtocol).append("' configuration.");
        }
        return sb.toString();
    }

    private static boolean isPlaintext(String securityProtocol) {
        return securityProtocol == null
                || !securityProtocol.toUpperCase(Locale.ROOT).contains("SSL");
    }

    /** Signals that Kafka never completed a send, as opposed to reporting it as failed. */
    static final class SendStalledException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        SendStalledException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
