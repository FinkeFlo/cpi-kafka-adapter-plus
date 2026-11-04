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

import java.lang.reflect.Constructor;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.RecordMetadata;

/** Futures as kafka-clients returns them, for tests of the send paths. */
final class KafkaFutures {

    private KafkaFutures() {
    }

    /** The future {@code KafkaProducer.send()} returns when the record never reached the buffer. */
    @SuppressWarnings("unchecked")
    static Future<RecordMetadata> failedBeforeBuffering(Exception e) throws Exception {
        Constructor<?> ctor = Class.forName(ProducerRetryPolicy.KAFKA_FUTURE_FAILURE_CLASS)
                .getDeclaredConstructor(Exception.class);
        ctor.setAccessible(true);
        return (Future<RecordMetadata>) ctor.newInstance(e);
    }
}
