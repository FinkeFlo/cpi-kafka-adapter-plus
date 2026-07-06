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

/**
 * Stub for org.ietf.jgss.GSSException.
 * Required because Kafka's SaslChannelBuilder statically references this class,
 * but CPI's OSGi framework does not export the java.security.jgss module.
 * This stub is never actually invoked at runtime (we use SASL PLAIN, not Kerberos/GSSAPI).
 */
public class GSSException extends Exception {
    private static final long serialVersionUID = 1L;

    public static final int BAD_MECH = 1;
    public static final int BAD_NAME = 2;
    public static final int BAD_NAMETYPE = 3;
    public static final int BAD_BINDINGS = 4;
    public static final int BAD_STATUS = 5;
    public static final int BAD_MIC = 6;
    public static final int CONTEXT_EXPIRED = 7;
    public static final int CREDENTIALS_EXPIRED = 8;
    public static final int DEFECTIVE_CREDENTIAL = 9;
    public static final int DEFECTIVE_TOKEN = 10;
    public static final int FAILURE = 11;
    public static final int NO_CONTEXT = 12;
    public static final int NO_CRED = 13;
    public static final int UNAUTHORIZED = 15;
    public static final int UNAVAILABLE = 16;
    public static final int DUPLICATE_ELEMENT = 17;
    public static final int NAME_NOT_MN = 18;
    public static final int DUPLICATE_TOKEN = 19;
    public static final int OLD_TOKEN = 20;
    public static final int GAP_TOKEN = 21;

    private int major;

    public GSSException(int majorCode) {
        this.major = majorCode;
    }

    public GSSException(int majorCode, int minorCode, String minorString) {
        super(minorString);
        this.major = majorCode;
    }

    public int getMajor() { return major; }
    public String getMajorString() { return "GSS-API error (stub): " + major; }
    public String getMessage() { return getMajorString(); }
}
