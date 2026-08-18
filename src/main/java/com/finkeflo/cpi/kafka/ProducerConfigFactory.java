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

    /**
     * How long the client keeps the metadata of an idle topic, in milliseconds. One hour, well above
     * the interval of any realistic integration flow, so that producing to a topic never has to
     * block on a metadata fetch just because the flow was quiet for a while. See the reasoning at
     * the usage site.
     */
    static final long METADATA_MAX_IDLE_MS = 3_600_000L;

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

        // max.block.ms caps how long send()/flush()/initTransactions() will block waiting for
        // broker metadata or buffer space. The Kafka client default is 60 s, which is already a
        // hard bound — but it is a long time to wait for an answer that will not change, e.g. for
        // a topic that does not exist. We deliberately keep it at or below request.timeout.ms
        // (never at delivery.timeout.ms, which would *raise* it to 120 s by default and double the
        // time such a send needs to fail). This is the fallback bound for the cases where the
        // AdminClient topic probe in CpiKafkaPlusProducer cannot give a definitive answer.
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (long) Math.min(30000, deliveryMs));

        // metadata.max.idle.ms — the client default of 5 minutes forgets the metadata of a topic
        // that has not been produced to for that long, and the next send then has to fetch it
        // again. That fetch is not a background refresh: KafkaProducer.waitOnMetadata() blocks the
        // calling thread in ProducerMetadata.awaitUpdate(), which is the method carrying the
        // KAFKA-10902 monitor defect (see MonitorFaultRetry). An integration flow that produces
        // less often than every five minutes would therefore enter that vulnerable path on
        // *every* message.
        //
        // Raising the idle window keeps the topic in the cache, so the send path finds the metadata
        // present and returns without ever calling awaitUpdate(). Freshness is unaffected: it is
        // governed by metadata.max.age.ms (5 min by default), whose refresh happens on the client's
        // own network thread and does not block senders. The cost is holding on to the metadata of
        // an idle topic, which is a few entries.
        props.put(ProducerConfig.METADATA_MAX_IDLE_CONFIG, METADATA_MAX_IDLE_MS);

        // client.id — auto-generated from adapter instance ID
        String adapterInstanceId = endpoint.getCamelContext() != null
                ? endpoint.getCamelContext().getGlobalOption("adapterInstanceID") : null;
        if (adapterInstanceId != null && !adapterInstanceId.isEmpty()) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG,
                    "cpi-kafka-plus-producer-" + adapterInstanceId);
        }

        // Security - reuse same logic as consumer
        SecurityConfigHelper.configureSecurityProperties(props, endpoint);

        // Note: transaction.two.phase.commit.enable is set in CpiKafkaPlusProducer.sendTransactionalBatch
        // where the transactional producer is created, not here. See e6 comment there for details.

        return props;
    }
}
