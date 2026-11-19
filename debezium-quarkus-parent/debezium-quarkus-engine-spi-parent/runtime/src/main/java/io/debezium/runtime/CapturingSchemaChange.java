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
 * Delivers the schema change events of an engine to the annotated method.
 * <p>
 * The method must be declared by a bean, must not be static or private, must return {@code void}
 * and must take a single {@link SchemaChange} parameter:
 *
 * <pre>
 * {@code @CapturingSchemaChange}
 * {@code public void onSchemaChange(SchemaChange change) { ... }}
 * </pre>
 * <p>
 * Schema change events are emitted by the connectors that support the {@code include.schema.changes} option, and only
 * when that option is enabled. The method is called on the thread that delivers the change events, in the position
 * that the schema change has in the stream: the events that precede it have already been handed to the
 * {@link Capturing} methods, the ones that follow it have not.
 * <p>
 * When an engine has at least one method with this annotation, its schema change events are no longer delivered to the
 * {@link Capturing} methods. Several methods can be declared for the same engine: each one receives every schema change,
 * in no guaranteed order.
 * <p>
 * An exception thrown by the method fails the engine, which reports an unsuccessful
 * {@link io.debezium.runtime.events.DebeziumCompletionEvent}.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Incubating
public @interface CapturingSchemaChange {

    /**
     * the engine assigned
     */
    String engine() default Capturing.DEFAULT;
}
