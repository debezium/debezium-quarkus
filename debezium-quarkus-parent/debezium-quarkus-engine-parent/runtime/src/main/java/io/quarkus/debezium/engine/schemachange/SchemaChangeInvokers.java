/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.schemachange;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.HistoryRecord.Fields;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.SchemaChange;
import io.debezium.schema.SchemaFactory;
import io.quarkus.debezium.engine.capture.consumer.QuarkusChangeConsumer;
import io.quarkus.debezium.engine.capture.consumer.SourceRecordEventConsumer;

/**
 * Connects the {@link io.debezium.runtime.CapturingSchemaChange} methods to the consumers of an engine.
 * <p>
 * The schema change events of an engine with at least one method are delivered to those methods and withheld from the
 * consumer, so that they never reach the {@link io.debezium.runtime.Capturing} methods. The consumer of an engine
 * without methods is returned untouched.
 */
public final class SchemaChangeInvokers {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaChangeInvokers.class);

    private SchemaChangeInvokers() {
    }

    /**
     * @param manifest the engine
     * @param configuration the configuration of the engine
     * @param invokers the invokers of every annotated method
     * @return the invokers assigned to the engine
     */
    public static List<SchemaChangeInvoker> select(EngineManifest manifest, Map<String, String> configuration, List<SchemaChangeInvoker> invokers) {
        final List<SchemaChangeInvoker> selected = invokers.stream()
                .filter(invoker -> invoker.engine().equals(manifest.id()))
                .toList();

        final String includeSchemaChanges = RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_CHANGES.name();
        if (!selected.isEmpty() && "false".equalsIgnoreCase(configuration.get(includeSchemaChanges))) {
            LOGGER.warn("Engine '{}' declares @CapturingSchemaChange methods but '{}' is disabled: they will never be called",
                    manifest.id(), includeSchemaChanges);
        }

        return selected;
    }

    /**
     * The batch is split at every schema change, so that the methods are called after the consumer has handled the
     * events that precede the schema change and before it receives the ones that follow it.
     */
    public static QuarkusChangeConsumer apply(QuarkusChangeConsumer delegate, EngineManifest manifest, List<SchemaChangeInvoker> invokers) {
        if (invokers.isEmpty()) {
            return delegate;
        }

        return new QuarkusChangeConsumer() {
            @Override
            public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
                    throws InterruptedException {
                if (records.stream().noneMatch(record -> isSchemaChange(toSourceRecord(record)))) {
                    delegate.handleBatch(records, committer);
                    return;
                }

                List<ChangeEvent<Object, Object>> pending = new ArrayList<>();
                for (ChangeEvent<Object, Object> record : records) {
                    final SourceRecord sourceRecord = toSourceRecord(record);
                    if (!isSchemaChange(sourceRecord)) {
                        pending.add(record);
                        continue;
                    }

                    if (!pending.isEmpty()) {
                        delegate.handleBatch(pending, committer);
                        pending = new ArrayList<>();
                    }

                    capture(invokers, manifest, record.destination(), sourceRecord);
                    committer.markProcessed(record);
                }

                if (pending.isEmpty()) {
                    committer.markBatchFinished();
                }
                else {
                    delegate.handleBatch(pending, committer);
                }
            }

            @Override
            public boolean supportsTombstoneEvents() {
                return delegate.supportsTombstoneEvents();
            }
        };
    }

    public static SourceRecordEventConsumer apply(SourceRecordEventConsumer delegate, EngineManifest manifest, List<SchemaChangeInvoker> invokers) {
        if (invokers.isEmpty()) {
            return delegate;
        }

        return event -> {
            final SourceRecord sourceRecord = toSourceRecord(event);
            if (isSchemaChange(sourceRecord)) {
                capture(invokers, manifest, event.destination(), sourceRecord);
                return;
            }

            delegate.accept(event);
        };
    }

    /**
     * The engine event wraps the {@link SourceRecord} in an {@link EmbeddedEngineChangeEvent} when the records are
     * converted, and carries it as the value for the plain Connect format.
     */
    static SourceRecord toSourceRecord(ChangeEvent<?, ?> event) {
        if (event instanceof EmbeddedEngineChangeEvent<?, ?, ?> embedded) {
            return embedded.sourceRecord();
        }

        return event.value() instanceof SourceRecord sourceRecord ? sourceRecord : null;
    }

    static boolean isSchemaChange(SourceRecord record) {
        return record != null
                && record.value() instanceof Struct
                && SchemaFactory.get().isSchemaChangeSchema(record.valueSchema());
    }

    static SchemaChange toSchemaChange(EngineManifest manifest, String destination, SourceRecord record) {
        final Struct value = (Struct) record.value();
        final List<Struct> tableChanges = value.getArray(Fields.TABLE_CHANGES);

        return new SchemaChange(
                manifest.id(),
                destination,
                value.getString(Fields.DATABASE_NAME),
                value.getString(Fields.SCHEMA_NAME),
                value.getString(Fields.DDL_STATEMENTS),
                Instant.ofEpochMilli(value.getInt64(Fields.TIMESTAMP)),
                value.getStruct(Fields.SOURCE),
                tableChanges == null ? List.of() : List.copyOf(tableChanges),
                record);
    }

    private static void capture(List<SchemaChangeInvoker> invokers, EngineManifest manifest, String destination, SourceRecord record) {
        final SchemaChange change = toSchemaChange(manifest, destination, record);
        invokers.forEach(invoker -> invoker.capture(change));
    }
}
