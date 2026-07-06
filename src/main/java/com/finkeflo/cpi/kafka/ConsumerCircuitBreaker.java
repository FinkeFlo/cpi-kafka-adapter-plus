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

import java.time.Duration;
import java.util.Set;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the auto-pause / circuit breaker state machine for the Kafka consumer.
 * Tracks consecutive processing failures and pauses consumption with exponential backoff
 * cooldown to avoid overwhelming downstream systems.
 *
 * <p>Package-private — used exclusively by {@link CpiKafkaPlusConsumer}.</p>
 */
final class ConsumerCircuitBreaker {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerCircuitBreaker.class);
    private static final long MAX_COOLDOWN_SECONDS = 900; // 15 minutes

    private final CpiKafkaPlusEndpoint endpoint;
    private final AdapterTracingHelper tracingHelper;

    private int consecutiveProcessingFailures = 0;
    private long pausedUntil = 0;
    private int pauseCount = 0;

    ConsumerCircuitBreaker(CpiKafkaPlusEndpoint endpoint, AdapterTracingHelper tracingHelper) {
        this.endpoint = endpoint;
        this.tracingHelper = tracingHelper;
    }

    /**
     * Checks whether the consumer is currently in auto-pause state. If paused and the
     * cooldown has not yet expired, performs a keepalive poll (with partitions paused)
     * to maintain group membership and returns {@code true} — the caller should return 0
     * from poll(). If the cooldown has expired, resumes partitions for a probe poll
     * and returns {@code false}.
     *
     * @param kafkaConsumer the Kafka consumer instance
     * @return {@code true} if the consumer is still paused and poll() should return 0
     */
    boolean handlePausedState(KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        if (!endpoint.isAutoPauseEnabled() || pausedUntil <= 0) {
            return false;
        }

        long now = System.currentTimeMillis();
        if (now < pausedUntil) {
            // Still in cooldown — pause partitions and poll for keepalive
            setPartitionsPaused(kafkaConsumer, true);
            try {
                BundleBackedClassLoader.withBundleClassLoader(getClass(),
                        () -> kafkaConsumer.poll(Duration.ofMillis(100)));
            } catch (WakeupException e) {
                LOG.info("[CPI-KAFKA-PLUS-DIAG] poll: received wakeup signal during auto-pause");
            } catch (Exception e) {
                LOG.debug("[CPI-KAFKA-PLUS-DIAG] poll: keepalive poll failed during auto-pause: {}", e.getMessage());
            }
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] poll: auto-paused, resuming in {}s", (pausedUntil - now) / 1000);
            return true;
        }

        // Cooldown expired — resume for probe poll
        setPartitionsPaused(kafkaConsumer, false);
        LOG.info("[CPI-KAFKA-PLUS-DIAG] poll: auto-pause cooldown expired, attempting probe poll");
        return false;
    }

    /**
     * Records a successful processing cycle. Resets all auto-pause state if any
     * failures or pauses had been accumulated.
     */
    void recordSuccess() {
        if (!endpoint.isAutoPauseEnabled()) {
            return;
        }
        if (consecutiveProcessingFailures > 0 || pauseCount > 0) {
            LOG.info("[CPI-KAFKA-PLUS-DIAG] auto-pause: RESET after successful processing (was pause #{}, topic='{}' group='{}')",
                    pauseCount, endpoint.getEffectiveTopic(), endpoint.getGroupId());
            consecutiveProcessingFailures = 0;
            pausedUntil = 0;
            pauseCount = 0;
            tracingHelper.publishConnectionStatus(true, null);
        }
    }

    /**
     * Records a processing failure. Increments the consecutive failure counter and,
     * if the auto-pause error threshold is reached, triggers auto-pause.
     *
     * @return {@code true} if auto-pause was triggered (caller should break out of drain loop)
     */
    boolean recordFailure() {
        if (!endpoint.isAutoPauseEnabled()) {
            return false;
        }
        consecutiveProcessingFailures++;
        if (consecutiveProcessingFailures >= endpoint.getAutoPauseErrorThreshold()) {
            triggerAutoPause();
            return true;
        }
        return false;
    }

    /**
     * Resets all circuit breaker state. Called when the consumer is stopped.
     */
    void reset() {
        consecutiveProcessingFailures = 0;
        pausedUntil = 0;
        pauseCount = 0;
    }

    private void triggerAutoPause() {
        pauseCount++;
        int clampedShift = Math.min(pauseCount - 1, 30);
        long cooldownSeconds = Math.min(
                endpoint.getAutoPauseCooldownSeconds() * (1L << clampedShift),
                MAX_COOLDOWN_SECONDS);
        pausedUntil = System.currentTimeMillis() + (cooldownSeconds * 1000L);
        LOG.warn("[CPI-KAFKA-PLUS-DIAG] auto-pause: ACTIVATED after {} consecutive failures. " +
                        "Pausing for {}s (pause #{}, topic='{}' group='{}')",
                consecutiveProcessingFailures, cooldownSeconds, pauseCount,
                endpoint.getEffectiveTopic(), endpoint.getGroupId());
        tracingHelper.publishConnectionStatus(false,
                new RuntimeException("Auto-paused after " + consecutiveProcessingFailures + " consecutive processing failures"));
    }

    private void setPartitionsPaused(KafkaConsumer<byte[], byte[]> kafkaConsumer, boolean paused) {
        try {
            Set<TopicPartition> assignment = kafkaConsumer.assignment();
            if (!assignment.isEmpty()) {
                if (paused) {
                    kafkaConsumer.pause(assignment);
                } else {
                    kafkaConsumer.resume(assignment);
                }
            }
        } catch (Exception e) {
            LOG.debug("[CPI-KAFKA-PLUS-DIAG] auto-pause: error {} partitions: {}",
                    paused ? "pausing" : "resuming", e.getMessage());
        }
    }
}
