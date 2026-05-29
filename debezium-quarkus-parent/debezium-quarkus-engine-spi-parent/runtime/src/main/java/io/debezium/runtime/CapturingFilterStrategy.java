/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.common.annotation.Incubating;

@Incubating
public interface CapturingFilterStrategy {

    boolean shouldCapture(CapturingEvent<SourceRecord, SourceRecord> event);

    class DefaultCapturingFilterStrategy implements CapturingFilterStrategy {
        @Override
        public boolean shouldCapture(CapturingEvent<SourceRecord, SourceRecord> event) {
            return Capturing.ALL.equals(event.destination()) && Capturing.DEFAULT.equals(event.engine());
        }
    }
}
