/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;

public class HibernateCacheHandler {

    private final DebeziumCacheInvalidator invalidator;

    @Inject
    public HibernateCacheHandler(DebeziumCacheInvalidator invalidator) {
        this.invalidator = invalidator;
    }

    @Capturing()
    public void capturing(CapturingEvent<SourceRecord, SourceRecord> event) {
        invalidator.evaluate(event);
    }
}
