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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;

/**
 * The ObjectMapper for record payloads the adapter parses and writes back out. A payload is data passed
 * through, so the defaults that normalise it are wrong here: a double turns {@code 100.00} into
 * {@code 100.0} and loses digits beyond 17, the tree model strips trailing zeros of a BigDecimal, and
 * {@code readTree()} stops after the first value and silently drops whatever follows it.
 *
 * <p>Numbers are written in canonical {@code BigDecimal} form: exponent input and decimals below
 * {@code 0.000001} come out in E-notation ({@code 1e3} becomes {@code 1E+3}, {@code 0.0000001} becomes
 * {@code 1E-7}); the value is unchanged.
 */
final class PayloadJson {

    private PayloadJson() {
    }

    static ObjectMapper newMapper() {
        return new ObjectMapper()
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);
    }
}
