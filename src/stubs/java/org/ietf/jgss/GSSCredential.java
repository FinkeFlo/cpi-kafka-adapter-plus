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
/**
 * JGSS Stub — CPI's OSGi runtime does not export org.ietf.jgss as a system package.
 * Kafka's SaslChannelBuilder statically references these classes even for PLAIN auth.
 * Without these stubs, the adapter bundle fails to resolve on CPI.
 */
package org.ietf.jgss;

/** Stub - required for Kafka SaslChannelBuilder classloading on CPI OSGi. */
public interface GSSCredential {
    int INITIATE_AND_ACCEPT = 0;
    int INITIATE_ONLY = 1;
    int ACCEPT_ONLY = 2;
    int DEFAULT_LIFETIME = 0;
    int INDEFINITE_LIFETIME = Integer.MAX_VALUE;
}
