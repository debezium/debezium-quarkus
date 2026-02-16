/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.runtime.CapturingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDebeziumCacheInvalidator implements DebeziumCacheInvalidator {

    private final DebeziumEvictionStrategy evictionStrategy;
    private final DebeziumFilterStrategy filterStrategy;
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDebeziumCacheInvalidator.class);

    public DefaultDebeziumCacheInvalidator(DebeziumEvictionStrategy evictionStrategy,
                                           DebeziumFilterStrategy filterStrategy) {
        this.evictionStrategy = evictionStrategy;
        this.filterStrategy = filterStrategy;
    }

    @Override
    public void evaluate(CapturingEvent<SourceRecord> event) {
        if (filterStrategy.filter(event)) {
            LOGGER.debug("CDC event candidate to invalidation discarded, {}", event);
            return;
        }

        evictionStrategy.evict(InvalidationEvent.from(
                event.engine(),
                (Struct) event.record().key(),
                ((Struct) event
                        .record()
                        .value())
                        .getStruct(Envelope.FieldName.SOURCE)));
    }

}
