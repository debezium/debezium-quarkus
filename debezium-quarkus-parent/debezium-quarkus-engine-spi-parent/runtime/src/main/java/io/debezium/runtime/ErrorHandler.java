/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * Strategy interface to handle exceptions occurring when sending captured events to destinations.
 * <p>
 * Implementations can wrap consumer execution to provide retry loops, dead-letter routing,
 * or custom fallback behavior.
 * <p>
 * <b>Note on batch retry semantics:</b> When a batch execution is retried, all destination consumers
 * configured for that batch are re-executed. If a destination succeeded prior to a failure in another
 * destination within the same batch, the successful destination will re-receive its events upon retry.
 * Handlers and downstream consumers should be designed with at-least-once delivery semantics in mind.
 */
public interface ErrorHandler {

    @FunctionalInterface
    interface BatchConsumer {
        void accept(CapturingEvents<BatchEvent> events) throws Exception;
    }

    @FunctionalInterface
    interface EventConsumer {
        void accept(CapturingEvent<SourceRecord, SourceRecord> event) throws Exception;
    }

    default void handle(CapturingEvents<BatchEvent> records, BatchConsumer consumer) throws Exception {
        consumer.accept(records);
    }

    default void handle(CapturingEvent<SourceRecord, SourceRecord> event, EventConsumer consumer) throws Exception {
        consumer.accept(event);
    }
}
