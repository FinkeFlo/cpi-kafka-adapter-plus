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
 */
package com.finkeflo.cpi.kafka;

import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/**
 * Decides what happens to a consumed record, or a batch, that failed: retry it within the poll,
 * write it to the dead letter queue, skip it, block its partition, or process a failed batch record
 * by record.
 *
 * <p>{@link RecordProcessor} carries the decisions out: the DLQ write, tracing, the MPL, the commit
 * and the pause between two retries. Kept apart so the rules can be read in one place and tested
 * without a broker.
 *
 * <h2>The rules</h2>
 * <ul>
 *   <li><b>Route failure of a record</b> (also a record of a failed batch, processed on its own):
 *       with a DLQ the record is retried within the poll up to {@code dlqMaxRetries} times, then
 *       dead-lettered. With {@code retryOnlyTransientErrors} a failure that is not transient skips
 *       the retries left. Without a DLQ it is not retried within the poll, {@code errorHandling}
 *       decides.</li>
 *   <li><b>Deserialization failure:</b> dead-lettered at once, or {@code errorHandling}.</li>
 *   <li><b>JSON Schema violation:</b> dead-lettered at once. Without a DLQ the record is dropped
 *       whatever {@code errorHandling} says: it does not become valid by retrying.</li>
 *   <li><b>A batch that cannot be formatted:</b> with a DLQ its records are processed one by one.
 *       Without a DLQ an {@link Error} escapes the poll; any other failure takes
 *       {@code errorHandling} for the whole batch.</li>
 *   <li><b>Route failure of a batch:</b> with a DLQ its records are processed one by one, without
 *       one {@code errorHandling} applies to the whole batch.</li>
 * </ul>
 * {@code errorHandling} is Retry Failed Message ({@link Action#BLOCK}, the default) or Skip Failed
 * Message ({@link Action#SKIP}). A failed DLQ write always blocks: the record stays unresolved and
 * is retried after a backoff. A stop request or an interrupted pause between two retries leaves the
 * record unresolved as well, without dead-lettering it; the caller checks for that, because it owns
 * the pause.
 */
final class RecordFailurePolicy {

    /** The error type of a route failure that was not retried because it is not transient. */
    static final String PERMANENT = "PERMANENT";
    /** The error type of a transient route failure whose retries are used up. */
    static final String TRANSIENT = "TRANSIENT";

    /** What happens to a failed record, or to a failed batch. */
    enum Action {
        /** Write the record to the dead letter queue. If the write fails, {@link #BLOCK}. */
        DEAD_LETTER,
        /**
         * Resolve the record (a batch: all its records) without processing it, so the next commit
         * may pass it: at most once.
         */
        SKIP,
        /**
         * Block the partition at the record (a batch: its first record). No commit passes it, the
         * poll rewinds to it, and it is retried after a backoff.
         */
        BLOCK,
        /** Process the records of the failed batch one by one, each like a single record. */
        PROCESS_INDIVIDUALLY,
        /** Let the failure escape the poll, which blocks the partition at the current record. */
        ESCAPE
    }

    /**
     * What happens to a record once its route call has failed for the last time: the action, and the
     * error type the DLQ record and the trace carry.
     */
    static final class Decision {

        private final Action action;
        private final String errorType;

        private Decision(Action action, String errorType) {
            this.action = action;
            this.errorType = errorType;
        }

        Action action() {
            return action;
        }

        /**
         * {@link #PERMANENT} or {@link #TRANSIENT}; {@code null} with {@code retryOnlyTransientErrors}
         * off, which does not classify failures.
         */
        String errorType() {
            return errorType;
        }

        @Override
        public String toString() {
            return errorType != null ? action + "(" + errorType + ")" : String.valueOf(action);
        }
    }

    private final boolean deadLetterQueue;
    private final IntSupplier dlqMaxRetries;
    private final BooleanSupplier retryOnlyTransientErrors;
    private final BooleanSupplier skipFailedMessages;

    /**
     * The settings are read when a decision is made, not here.
     *
     * @param deadLetterQueue          whether failed records can be written to a dead letter queue
     * @param dlqMaxRetries            Max Retries before DLQ
     * @param retryOnlyTransientErrors whether only a transient route failure is retried
     * @param skipFailedMessages       whether {@code errorHandling} is Skip Failed Message
     */
    RecordFailurePolicy(boolean deadLetterQueue, IntSupplier dlqMaxRetries,
                        BooleanSupplier retryOnlyTransientErrors, BooleanSupplier skipFailedMessages) {
        this.deadLetterQueue = deadLetterQueue;
        this.dlqMaxRetries = dlqMaxRetries;
        this.retryOnlyTransientErrors = retryOnlyTransientErrors;
        this.skipFailedMessages = skipFailedMessages;
    }

    /**
     * How often the route is called again for a failed record within the poll. Max Retries before
     * DLQ applies only with a DLQ: without one, the blocked partition retries the record on a later
     * poll, after a backoff.
     */
    int maxRetries() {
        return deadLetterQueue ? dlqMaxRetries.getAsInt() : 0;
    }

    /**
     * Whether a failed route call skips the retries left: with {@code retryOnlyTransientErrors}, a
     * failure that {@link KafkaErrorHelper#isTransientForRedelivery} does not call transient would
     * fail the same way on every retry.
     */
    boolean skipsRetries(Exception error) {
        return retryOnlyTransientErrors.getAsBoolean() && !KafkaErrorHelper.isTransientForRedelivery(error);
    }

    /**
     * Decides about a record whose route call failed for the last time.
     *
     * @param permanent whether {@link #skipsRetries} skipped the retries left, rather than the
     *                  retries being used up
     */
    Decision afterRetries(boolean permanent) {
        String errorType = null;
        if (retryOnlyTransientErrors.getAsBoolean()) {
            errorType = permanent ? PERMANENT : TRANSIENT;
        }
        return new Decision(deadLetterQueue ? Action.DEAD_LETTER : errorHandling(), errorType);
    }

    /** A record that cannot be deserialized is not retried within the poll. */
    Action forDeserializationFailure() {
        return deadLetterQueue ? Action.DEAD_LETTER : errorHandling();
    }

    /**
     * A record that violates the JSON Schema is not retried within the poll. Without a DLQ it is
     * dropped whatever {@code errorHandling} says, as documented for JSON Schema validation.
     */
    Action forSchemaViolation() {
        return deadLetterQueue ? Action.DEAD_LETTER : Action.SKIP;
    }

    /**
     * A batch that cannot be formatted. With a DLQ, processing its records one by one dead-letters
     * only the failing one; an {@link Error} then escapes from that record. Without a DLQ an
     * {@code Error} escapes at once: it is not a data problem, and the poll retries the batch.
     */
    Action forBatchFormatFailure(Throwable error) {
        if (deadLetterQueue) {
            return Action.PROCESS_INDIVIDUALLY;
        }
        if (!(error instanceof Exception)) {
            return Action.ESCAPE;
        }
        return errorHandling();
    }

    /** A batch the route failed for. Without a DLQ {@code errorHandling} applies to the whole batch. */
    Action forBatchRouteFailure() {
        return deadLetterQueue ? Action.PROCESS_INDIVIDUALLY : errorHandling();
    }

    /** {@code errorHandling}, for a failure without a DLQ. */
    private Action errorHandling() {
        return skipFailedMessages.getAsBoolean() ? Action.SKIP : Action.BLOCK;
    }
}
