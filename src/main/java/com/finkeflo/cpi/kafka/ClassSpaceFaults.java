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

/**
 * Recognises failures caused by a dead bundle class space (issue #148): an adapter update has
 * replaced the bundle revision this route was created from, and any first-time class, link or
 * native-library load over the old loader now fails. Shared by consumer and producer so both
 * report the same diagnosis and the same remedy — redeploy the integration flow.
 */
final class ClassSpaceFaults {

    private static final String INVALID_BUNDLE_WIRING_SNIPPET = "bundle wiring";
    private static final String INVALID_BUNDLE_WIRING_SUFFIX = "no longer valid";

    private ClassSpaceFaults() {
    }

    /**
     * First signature: a class-loading fault whose message carries the OSGi
     * "bundle wiring ... no longer valid" text. Unambiguous on its own.
     */
    static boolean isWiringInvalid(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            boolean classLoadingFault = current instanceof NoClassDefFoundError
                    || current instanceof ClassNotFoundException
                    || current instanceof LinkageError;
            String msg = current.getMessage();
            boolean wiringSignature = msg != null
                    && msg.contains(INVALID_BUNDLE_WIRING_SNIPPET)
                    && msg.contains(INVALID_BUNDLE_WIRING_SUFFIX);
            if (classLoadingFault && wiringSignature) {
                return true;
            }
            Throwable next = current.getCause();
            if (next == current) {
                break;
            }
            current = next;
        }
        return false;
    }

    /**
     * Second signature: any class, link or native-library fault anywhere in the chain. Not
     * conclusive alone — combine with {@link OsgiBundleInfo#isClassSpaceStale(Class)}. Typical:
     * {@code SnappyError FAILED_TO_LOAD_NATIVE_LIBRARY} on the first snappy batch after an update,
     * later re-thrown by Kafka as {@code KafkaException → NoClassDefFoundError: Could not initialize
     * class}.
     */
    static boolean hasFaultSignature(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof LinkageError
                    || current instanceof ClassNotFoundException
                    || (current instanceof Error
                        && !(current instanceof VirtualMachineError)
                        && !(current instanceof ThreadDeath))) {
                return true;
            }
            Throwable next = current.getCause();
            if (next == current) {
                break;
            }
            current = next;
        }
        return false;
    }

    /**
     * {@code true} when {@code failure} is attributable to the dead class space of the bundle
     * revision {@code anchor} was loaded from: either the wiring text is present, or the failure has
     * a class-space fault signature and the anchor's loader is known to be stale.
     */
    static boolean isOnStaleClassSpace(Throwable failure, Class<?> anchor) {
        if (isWiringInvalid(failure)) {
            return true;
        }
        return hasFaultSignature(failure) && Boolean.TRUE.equals(OsgiBundleInfo.isClassSpaceStale(anchor));
    }
}
