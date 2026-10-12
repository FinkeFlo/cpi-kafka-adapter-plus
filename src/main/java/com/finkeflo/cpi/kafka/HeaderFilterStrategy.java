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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Strategy for filtering headers based on allowed patterns.
 */
public final class HeaderFilterStrategy {

    /**
     * Headers that carry credentials. A wildcard pattern, the default {@code *} included, never lets
     * them through: an access token in a record header can be read by everyone who may read the topic
     * (#195). A pattern that names one of them exactly still does.
     */
    private static final Set<String> CREDENTIAL_HEADERS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList("authorization", "proxy-authorization", "cookie", "set-cookie")));

    private HeaderFilterStrategy() {
        // static utility class
    }

    /**
     * Checks if a given header name matches the allowed patterns.
     * Patterns can be separated by a pipe '|' character.
     * Supports exact match, or prefix/suffix matching using '*', in any letter case. A credential
     * header ({@code Authorization}, {@code Proxy-Authorization}, {@code Cookie},
     * {@code Set-Cookie}) is allowed only by a pattern that names it exactly, never by a wildcard.
     *
     * @param headerName        The name of the header to check.
     * @param allowedPatternStr The pipe-separated pattern string.
     * @return true if the header is allowed, false otherwise.
     */
    public static boolean isHeaderAllowed(String headerName, String allowedPatternStr) {
        if (allowedPatternStr == null || allowedPatternStr.trim().isEmpty()) {
            return false;
        }
        String headerNameLower = headerName.toLowerCase(Locale.ROOT);
        boolean credential = CREDENTIAL_HEADERS.contains(headerNameLower);
        if ("*".equals(allowedPatternStr.trim())) {
            return !credential;
        }
        
        String[] patterns = allowedPatternStr.split("\\|");
        boolean wildcardMatch = false;
        
        for (String pattern : patterns) {
            pattern = pattern.trim().toLowerCase(Locale.ROOT);
            if (pattern.isEmpty()) {
                continue;
            }
            if (pattern.endsWith("*") && pattern.startsWith("*") && pattern.length() > 2) {
                wildcardMatch |= headerNameLower.contains(pattern.substring(1, pattern.length() - 1));
            } else if (pattern.endsWith("*")) {
                wildcardMatch |= headerNameLower.startsWith(pattern.substring(0, pattern.length() - 1));
            } else if (pattern.startsWith("*")) {
                wildcardMatch |= headerNameLower.endsWith(pattern.substring(1));
            } else if (headerNameLower.equals(pattern)) {
                return true;
            }
        }
        return wildcardMatch && !credential;
    }
}
