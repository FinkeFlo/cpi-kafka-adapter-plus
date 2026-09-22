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
import java.util.List;

import com.sap.it.api.adapter.monitoring.AdapterEndpointInformation;
import com.sap.it.api.adapter.monitoring.AdapterEndpointInformationService;
import com.sap.it.api.adapter.monitoring.AdapterEndpointInstance;
import com.sap.it.api.adapter.monitoring.EndpointCategory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spike / feasibility probe for SAP CPI's endpoint-visualization SPI
 * ({@link AdapterEndpointInformationService}).
 *
 * <p><b>This is a deliberately minimal, static-only implementation.</b> It returns one hard-coded
 * placeholder entry so we can verify, on a real tenant, whether CPI's monitoring UI surfaces any
 * data at all for a polling-style, non-HTTP adapter like this one (see the endpoint-visualization
 * feature plan for the background: the SPI's {@link EndpointCategory} and
 * {@code com.sap.it.api.adapter.monitoring.Protocol} enums only cover entry-point/API-style
 * adapters such as REST, SOAP, OData, AS2/AS4 — there is no Kafka/messaging category). No registry,
 * no live values, no per-iFlow differentiation yet; that only gets built once this spike confirms
 * CPI actually renders the data somewhere.
 *
 * <p>Registered as an OSGi Blueprint service, see
 * {@code OSGI-INF/blueprint/endpoint-information-service.xml}.
 */
public class CpiKafkaPlusEndpointInformationService implements AdapterEndpointInformationService {

    private static final Logger LOG = LoggerFactory.getLogger(CpiKafkaPlusEndpointInformationService.class);

    /** Placeholder text; real values (broker, topic, group id, ...) come only after the spike. */
    private static final String SPIKE_PLACEHOLDER = "cpi-kafka-plus adapter (spike placeholder - no live data yet)";

    @Override
    public List<AdapterEndpointInformation> getAdapterEndpointInformation() {
        LOG.info("[CPI-KAFKA-PLUS-ENDPOINT-INFO-SPIKE] getAdapterEndpointInformation() called");
        return Collections.singletonList(buildPlaceholderInformation(null));
    }

    @Override
    public List<AdapterEndpointInformation> getAdapterEndpointInformationByIFlow(String integrationFlowId) {
        LOG.info("[CPI-KAFKA-PLUS-ENDPOINT-INFO-SPIKE] getAdapterEndpointInformationByIFlow(integrationFlowId={}) called",
                integrationFlowId);
        return Collections.singletonList(buildPlaceholderInformation(integrationFlowId));
    }

    private AdapterEndpointInformation buildPlaceholderInformation(String integrationFlowId) {
        AdapterEndpointInstance instance = new AdapterEndpointInstance(
                EndpointCategory.ENTRY_POINT, SPIKE_PLACEHOLDER, SPIKE_PLACEHOLDER);
        AdapterEndpointInformation information = new AdapterEndpointInformation(
                Collections.singletonList(instance), integrationFlowId);
        return information;
    }
}
