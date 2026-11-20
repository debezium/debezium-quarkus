/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime;

import java.time.Instant;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.common.annotation.Incubating;

/**
 * A schema change event emitted by a connector when {@code include.schema.changes} is enabled, delivered to the
 * methods annotated with {@link CapturingSchemaChange}.
 *
 * @param engine the engine that emitted the event
 * @param destination the logical destination for which the event is intended
 * @param database the database that contains the change, {@code null} when the connector does not report it
 * @param schema the schema that contains the change, {@code null} when the connector does not report it
 * @param ddl the DDL statements that caused the change, {@code null} when the connector does not report them
 * @param timestamp the time at which the connector processed the change
 * @param source the source metadata of the event
 * @param tableChanges the structure of the tables affected by the change
 * @param record the original record emitted by the connector
 */
@Incubating
public record SchemaChange(
        String engine,
        String destination,
        String database,
        String schema,
        String ddl,
        Instant timestamp,
        Struct source,
        List<Struct> tableChanges,
        SourceRecord record) {
}
