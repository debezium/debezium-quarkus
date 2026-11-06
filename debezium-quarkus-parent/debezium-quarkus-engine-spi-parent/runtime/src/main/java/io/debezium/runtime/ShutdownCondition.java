/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.debezium.common.annotation.Incubating;

/**
 * Stops the Debezium engine when the annotated method returns {@code true}.
 * <p>
 * The method must be declared by a bean, must not be static or private, must return {@code boolean}
 * and must take a single {@code org.apache.kafka.connect.source.SourceRecord} parameter:
 *
 * <pre>
 * {@code @ShutdownCondition(Phase.AFTER)}
 * {@code public boolean stopAfterLastOrder(SourceRecord record) { ... }}
 * </pre>
 * <p>
 * The method is evaluated for every record: {@link Phase#BEFORE} before the record is handed to the consumer,
 * {@link Phase#AFTER} once the consumer has handled the batch that contains it. When the method returns
 * {@code true} the engine stops, and the method is no longer called. At most one method per phase is allowed; the build
 * fails otherwise.
 * <p>
 * An exception thrown by the method is not treated as {@code false}: it fails the engine, which reports an unsuccessful
 * {@link io.debezium.runtime.events.DebeziumCompletionEvent}.
 * <p>
 * The method can be called concurrently, by the tasks of an engine and by several engines, so it must be thread-safe.
 * <p>
 * This annotation is not scoped to an engine yet: the method applies to every engine configured by the extension, and
 * returning {@code true} stops the engine that delivered the record. Engine qualifiers can be added later.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Incubating
public @interface ShutdownCondition {

    /**
     * the moment, relative to the consumer, in which the shutdown condition is evaluated
     */
    Phase value();

    /**
     * when the shutdown condition is evaluated for a record
     */
    enum Phase {
        /**
         * evaluated for each record before the batch is handed to the consumer; a match stops the engine before the consumer runs
         */
        BEFORE,
        /**
         * evaluated for each record after the consumer handled the batch; a match marks the batch as finished and then stops the engine
         */
        AFTER
    }
}
