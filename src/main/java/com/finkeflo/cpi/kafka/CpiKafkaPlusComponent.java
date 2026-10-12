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

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.camel.Endpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.support.DefaultComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CpiKafkaPlusComponent extends DefaultComponent {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusComponent.class);

    /**
     * The options whose value is a number or a switch, matched in any case as Camel binds them. A
     * space around such a value can never be meant, and Camel failed to bind it with a message that
     * names no UI label (#261).
     */
    private static final Set<String> NUMBER_AND_SWITCH_OPTIONS = numberAndSwitchOptions();

    public CpiKafkaPlusComponent() {
        // Earliest hook this bundle gets: pre-load the whole class space before a later adapter
        // update can purge this revision underneath running routes (issue #148).
        BundleClassWarmup.ensureStarted(CpiKafkaPlusComponent.class, "component.init");
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        BundleClassWarmup.ensureStarted(CpiKafkaPlusComponent.class, "component.start");
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        LOG.info("Creating CPI Kafka Plus endpoint: {}", uri);
        CpiKafkaPlusEndpoint endpoint = new CpiKafkaPlusEndpoint(uri, remaining, this);
        trimNumbersAndSwitches(parameters);
        setProperties(endpoint, parameters);
        return endpoint;
    }

    /**
     * Trims the value of every number and switch option before Camel binds it. Text values are bound
     * as they are: the endpoint's setters trim those where a space can never be meant, and leave the
     * others, such as the consumer group ID, exactly as configured.
     */
    static void trimNumbersAndSwitches(Map<String, Object> parameters) {
        for (Map.Entry<String, Object> parameter : parameters.entrySet()) {
            if (parameter.getValue() instanceof String && NUMBER_AND_SWITCH_OPTIONS.contains(parameter.getKey())) {
                parameter.setValue(((String) parameter.getValue()).trim());
            }
        }
    }

    private static Set<String> numberAndSwitchOptions() {
        Set<String> options = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Field field : CpiKafkaPlusEndpoint.class.getDeclaredFields()) {
            Class<?> type = field.getType();
            if (field.isAnnotationPresent(UriParam.class)
                    && (type.isPrimitive() || Number.class.isAssignableFrom(type) || type == Boolean.class)) {
                options.add(field.getName());
            }
        }
        return options;
    }
}
