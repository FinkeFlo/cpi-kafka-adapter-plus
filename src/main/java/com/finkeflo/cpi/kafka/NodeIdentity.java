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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.UnaryOperator;

/**
 * Identity segments of the ids a worker node registers with the broker: the member suffix in
 * {@code group.instance.id} (sender) and {@code transactional.id} (receiver), and the topic hash in
 * {@code transactional.id}. Pure functions without logging; what happens when no member suffix is
 * available (dynamic membership on the sender, a random suffix on the receiver) stays with the
 * caller. Package-private.
 */
final class NodeIdentity {

    private NodeIdentity() {
    }

    /**
     * Resolves a stable per-pod identifier for {@code group.instance.id} (KIP-345
     * Static Membership). Returns the first non-empty value of:
     * <ol>
     *   <li>{@code CF_INSTANCE_INDEX} — Cloud Foundry instance number, stable
     *       across restarts of the same slot (this is what runs in CPI BTP).</li>
     *   <li>{@code HOSTNAME} — fallback for non-CF runtimes (local Camel test,
     *       k8s where CF_INSTANCE_INDEX is unset).</li>
     * </ol>
     * Returns {@code null} if neither is available, in which case the consumer
     * falls back to dynamic membership.
     *
     * <p>Package-private and static so unit tests can verify the call site
     * behavior using whichever env vars are present in the test JVM.
     */
    static String resolveStaticMemberSuffix() {
        return resolveStaticMemberSuffix(System::getenv);
    }

    /**
     * As {@link #resolveStaticMemberSuffix()}, reading the variables through {@code env} so tests
     * can supply them.
     */
    static String resolveStaticMemberSuffix(UnaryOperator<String> env) {
        String cfInstance = env.apply("CF_INSTANCE_INDEX");
        if (cfInstance != null && !cfInstance.isEmpty()) {
            return cfInstance;
        }
        String hostname = env.apply("HOSTNAME");
        return (hostname != null && !hostname.isEmpty()) ? hostname : null;
    }

    /**
     * Returns the first 8 hex characters of the SHA-256 digest of the given topic name.
     * Used as a stable, short segment in {@code transactional.id} to ensure that two producers
     * with the same {@code transactionalIdPrefix} but different target topics never share an id.
     *
     * <p>SHA-256 is used rather than {@link String#hashCode()} because the latter is not
     * guaranteed to be stable across JVM versions or implementations.
     */
    static String computeTopicHash(String topic) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(topic.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(8);
            for (int i = 0; i < 4; i++) {
                sb.append(String.format("%02x", hash[i]));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the Java SE spec — this branch is unreachable in practice.
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
