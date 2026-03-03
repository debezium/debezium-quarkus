/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.runtime.CapturingEvent;

/**
 * Defines the events that can be evaluated for cache invalidation
 *
 *  @author Giovanni Panice
 */
public interface DebeziumFilterStrategy {

    /**
     *
     * @param event {@link SourceRecord} captured by Debezium
     * @return true if the event should be discarded
     */
    boolean filter(CapturingEvent<SourceRecord, SourceRecord> event);
}
