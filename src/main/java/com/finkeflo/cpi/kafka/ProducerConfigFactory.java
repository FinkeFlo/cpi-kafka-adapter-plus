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

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating Kafka producer configuration properties from an endpoint.
 */
public final class ProducerConfigFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerConfigFactory.class);

    private ProducerConfigFactory() {
        // static utility class
    }

    /**
     * Builds and returns the Properties map for a KafkaProducer based on the CPI Endpoint configuration.
     */
    public static Properties buildProducerProperties(CpiKafkaPlusEndpoint endpoint) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, endpoint.getBootstrapServers());
        // Serializers are passed as instances to the KafkaProducer constructor
        // to avoid OSGi classloading issues with Class.forName()

        String acks = endpoint.getAcks();
        if (endpoint.isEnableIdempotence() && !"all".equals(acks) && !"-1".equals(acks)) {
            LOG.warn("[CPI-KAFKA-PLUS-DIAG] buildProducerProperties: enableIdempotence=true requires acks=all, "
                    + "overriding configured acks='{}' to 'all'", acks);
            acks = "all";
        }
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, endpoint.getCompressionType());
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, endpoint.getMaxRequestSizeKb() * 1024);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0L);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, endpoint.getProducerBatchSizeKb() * 1024);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, endpoint.getBufferMemoryKb() * 1024L);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, endpoint.isEnableIdempotence());
        
        // retries: not configurable — Kafka uses Integer.MAX_VALUE with idempotence,
        // deliveryTimeoutSeconds is the effective limit
        int deliveryMs = endpoint.getDeliveryTimeoutSeconds() * 1000;
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryMs);
        
        // request.timeout.ms must be <= delivery.timeout.ms; cap it accordingly
        int requestTimeoutMs = Math.min(30000, deliveryMs);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);

        // client.id — auto-generated from adapter instance ID
        String adapterInstanceId = endpoint.getCamelContext() != null
                ? endpoint.getCamelContext().getGlobalOption("adapterInstanceID") : null;
        if (adapterInstanceId != null && !adapterInstanceId.isEmpty()) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG,
                    "cpi-kafka-plus-producer-" + adapterInstanceId);
        }

        // Security - reuse same logic as consumer
        SecurityConfigHelper.configureSecurityProperties(props, endpoint);

        return props;
    }
}
