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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The cause chain of a throwable, from the throwable itself to its deepest cause, walked so that
 * it always ends (issue #241).
 *
 * <p>{@link Throwable#initCause(Throwable)} allows a cycle such as A → B → A, and a subclass can
 * override {@link Throwable#getCause()} to return itself. A plain
 * {@code while (c != null) c = c.getCause()} loop walks either forever, on the poll thread, with no
 * log line and deaf to an interrupt. The walk here stops at a cause it has already returned and
 * after {@link #MAX_DEPTH} links, whichever comes first.
 */
final class CauseChain implements Iterable<Throwable> {

    /**
     * Far above any real chain. A cycle ends the walk much earlier; this only bounds a chain whose
     * {@code getCause()} keeps returning new objects.
     */
    static final int MAX_DEPTH = 64;

    private final Throwable head;

    private CauseChain(Throwable head) {
        this.head = head;
    }

    /** The chain starting at {@code t} itself; empty for {@code null}. */
    static CauseChain of(Throwable t) {
        return new CauseChain(t);
    }

    /**
     * The last link of the chain: the deepest cause, {@code t} itself when it has no cause, the
     * link before the repeat for a cyclic chain, or {@code null} for {@code null}.
     */
    static Throwable root(Throwable t) {
        Throwable last = null;
        for (Throwable link : of(t)) {
            last = link;
        }
        return last;
    }

    @Override
    public Iterator<Throwable> iterator() {
        return new Iterator<Throwable>() {
            private final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            private Throwable next = head;

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Throwable next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                Throwable current = next;
                seen.add(current);
                Throwable cause = current.getCause();
                next = cause == null || seen.contains(cause) || seen.size() >= MAX_DEPTH ? null : cause;
                return current;
            }
        };
    }
}
