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
 * Strategy for filtering headers based on allowed patterns.
 */
public final class HeaderFilterStrategy {

    private HeaderFilterStrategy() {
        // static utility class
    }

    /**
     * Checks if a given header name matches the allowed patterns.
     * Patterns can be separated by a pipe '|' character.
     * Supports exact match, or prefix/suffix matching using '*'.
     *
     * @param headerName        The name of the header to check.
     * @param allowedPatternStr The pipe-separated pattern string.
     * @return true if the header is allowed, false otherwise.
     */
    public static boolean isHeaderAllowed(String headerName, String allowedPatternStr) {
        if (allowedPatternStr == null || allowedPatternStr.trim().isEmpty()) {
            return false;
        }
        if ("*".equals(allowedPatternStr.trim())) {
            return true;
        }
        String[] patterns = allowedPatternStr.split("\\|");
        for (String pattern : patterns) {
            pattern = pattern.trim();
            if (pattern.isEmpty()) {
                continue;
            }
            if (pattern.endsWith("*") && pattern.startsWith("*") && pattern.length() > 2) {
                if (headerName.contains(pattern.substring(1, pattern.length() - 1))) {
                    return true;
                }
            } else if (pattern.endsWith("*")) {
                if (headerName.startsWith(pattern.substring(0, pattern.length() - 1))) {
                    return true;
                }
            } else if (pattern.startsWith("*")) {
                if (headerName.endsWith(pattern.substring(1))) {
                    return true;
                }
            } else {
                if (headerName.equals(pattern)) {
                    return true;
                }
            }
        }
        return false;
    }
}
