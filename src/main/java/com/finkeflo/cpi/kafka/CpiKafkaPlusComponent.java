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

import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.support.DefaultComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CpiKafkaPlusComponent extends DefaultComponent {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusComponent.class);

    @Override
    public Endpoint createEndpoint(String uri) throws Exception {
        // Trim whitespace from all query parameter values before Camel parses the URI.
        // SAP CPI does not URL-encode adapter field values, so a leading/trailing space
        // in e.g. the topic field produces an illegal URI character and a cryptic
        // "Illegal character in opaque part" error. Trimming here gives a clean
        // startup error or transparent fix for accidental spaces.
        return super.createEndpoint(trimQueryParamValues(uri));
    }

    static String trimQueryParamValues(String uri) {
        int queryStart = uri.indexOf('?');
        if (queryStart < 0) return uri;
        String base  = uri.substring(0, queryStart + 1);
        String query = uri.substring(queryStart + 1);
        String[] pairs = query.split("&", -1);
        StringBuilder sb = new StringBuilder(base);
        for (int i = 0; i < pairs.length; i++) {
            if (i > 0) sb.append('&');
            int eq = pairs[i].indexOf('=');
            if (eq >= 0) {
                sb.append(pairs[i], 0, eq + 1);
                sb.append(pairs[i].substring(eq + 1).trim());
            } else {
                sb.append(pairs[i]);
            }
        }
        return sb.toString();
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        LOG.info("Creating CPI Kafka Plus endpoint: {}", uri);
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint(uri, remaining, this);
        setProperties(endpoint, parameters);
        return endpoint;
    }
}
