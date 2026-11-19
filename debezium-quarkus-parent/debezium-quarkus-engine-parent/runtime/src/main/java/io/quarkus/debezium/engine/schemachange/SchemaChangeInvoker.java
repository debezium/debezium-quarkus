/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.schemachange;

import io.debezium.runtime.SchemaChange;

/**
 * Invoker assigned to a method annotated with {@link io.debezium.runtime.CapturingSchemaChange}.
 */
public interface SchemaChangeInvoker {

    /**
     * @return the engine id assigned
     */
    String engine();

    /**
     * @param change the schema change emitted by the engine
     */
    void capture(SchemaChange change);
}
