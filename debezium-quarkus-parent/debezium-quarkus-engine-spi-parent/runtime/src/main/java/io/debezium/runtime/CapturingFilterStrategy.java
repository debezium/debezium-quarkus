/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.common.annotation.Incubating;

/**
 * Strategy for filtering {@link CapturingEvent} event before they are delivered to a
 * {@link Capturing}-annotated handler method.
 * Implementations are referenced via {@link Capturing#filter()} and must be CDI beans.
 *
 */
@Incubating
public interface CapturingFilterStrategy {

    /**
     * Determines whether the given event should be captured.
     *
     * @param event the change data capture event to evaluate
     * @return {@code true} if the event should be delivered to the handler, {@code false} to skip it
     */
    boolean shouldCapture(CapturingEvent<SourceRecord, SourceRecord> event);

    /**
     * Default filter that accepts all events with wildcard destination and default engine.
     */
    class DefaultCapturingFilterStrategy implements CapturingFilterStrategy {
        @Override
        public boolean shouldCapture(CapturingEvent<SourceRecord, SourceRecord> event) {
            return Capturing.ALL.equals(event.destination()) && Capturing.DEFAULT.equals(event.engine());
        }
    }
}
