/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.CapturingEvents;

/**
 *
 * Invoker assigned to any annotated class with method {@link Capturing}
 *
 */
public interface CapturingInvoker<T> {

    /**
     *
     * @return the destination that triggers the handler
     */
    String destination();

    /**
     * @param event captured by Debezium
     */
    void capture(T event);

    /**
     *
     * @return the engine id assigned
     */
    String engine();

    /**
     * Determines whether the given event should be captured by this invoker.
     * Delegates to the {@link io.debezium.runtime.CapturingFilterStrategy} when a custom filter
     * is configured via {@link io.debezium.runtime.Capturing#filter()}.
     *
     * @param event the captured event to evaluate
     * @return {@code true} if the event should be captured, {@code false} to skip it
     */
    default boolean shouldCapture(CapturingEvent<SourceRecord, SourceRecord> event) {
        return true;
    }

    /**
     * Indicates whether this invoker has a custom {@link io.debezium.runtime.CapturingFilterStrategy} configured
     *
     * @return {@code true} if a custom filter is configured, {@code false} otherwise
     */
    default boolean hasFilter() {
        return false;
    }

    static CapturingInvokerKey generateKey(CapturingInvoker invoker) {
        return new CapturingInvokerKey(invoker.engine(), invoker.destination());
    }

    static CapturingInvokerKey getKey(CapturingEvent event) {
        return new CapturingInvokerKey(event.engine(), event.destination());
    }

    static CapturingInvokerKey getKey(CapturingEvents<Object> event) {
        return new CapturingInvokerKey(event.engine(), event.destination());
    }

    static CapturingInvokerKey getAllDestinations(CapturingEvent event) {
        return new CapturingInvokerKey(event.engine(), Capturing.ALL);
    }

    static CapturingInvokerKey getAllDestinations(CapturingEvents events) {
        return new CapturingInvokerKey(events.engine(), Capturing.ALL);
    }

    record CapturingInvokerKey(
            String engine,
            String destination) {
    }
}
