/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.runtime.CapturingEvent;

/**
 *  Invalidates cached data in response to {@link CapturingEvent} events emitted by the Debezium Engine.
 */
public interface DebeziumCacheInvalidator {
    /**
     *
     * @param event {@link CapturingEvent} for invalidation
     */
    void evaluate(CapturingEvent<SourceRecord, SourceRecord> event);
}
