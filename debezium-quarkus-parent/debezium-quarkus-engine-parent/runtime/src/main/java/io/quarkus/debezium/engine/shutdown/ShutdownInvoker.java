/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.shutdown;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.runtime.DebeziumShutdown.Phase;

/**
 * Invokes a method annotated with {@link io.debezium.runtime.DebeziumShutdown}.
 * Implementations are generated at build time.
 */
public interface ShutdownInvoker {

    /**
     * @return the phase in which the annotated method is evaluated
     */
    Phase phase();

    /**
     * @param record the record to evaluate
     * @return {@code true} when the engine has to stop
     */
    boolean shouldShutdown(SourceRecord record);
}
