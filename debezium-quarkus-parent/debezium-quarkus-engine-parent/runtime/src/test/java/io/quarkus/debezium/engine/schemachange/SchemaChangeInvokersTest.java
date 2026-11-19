/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine.schemachange;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.relational.history.HistoryRecord.Fields;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.SchemaChange;
import io.quarkus.debezium.engine.capture.consumer.QuarkusChangeConsumer;
import io.quarkus.debezium.engine.capture.consumer.SourceRecordEventConsumer;

class SchemaChangeInvokersTest {

    private static final EngineManifest ENGINE = new EngineManifest("default");
    private static final String SCHEMA_CHANGE_TOPIC = "topic";
    private static final String DATA_TOPIC = "topic.inventory.orders";

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .field("db", Schema.STRING_SCHEMA)
            .build();
    private static final Schema TABLE_CHANGE_SCHEMA = SchemaBuilder.struct()
            .field("type", Schema.STRING_SCHEMA)
            .field("id", Schema.STRING_SCHEMA)
            .build();
    private static final Schema SCHEMA_CHANGE_SCHEMA = SchemaBuilder.struct()
            .name("io.debezium.connector.mysql.SchemaChangeValue")
            .field(Fields.SOURCE, SOURCE_SCHEMA)
            .field(Fields.TIMESTAMP, Schema.INT64_SCHEMA)
            .field(Fields.DATABASE_NAME, Schema.OPTIONAL_STRING_SCHEMA)
            .field(Fields.SCHEMA_NAME, Schema.OPTIONAL_STRING_SCHEMA)
            .field(Fields.DDL_STATEMENTS, Schema.OPTIONAL_STRING_SCHEMA)
            .field(Fields.TABLE_CHANGES, SchemaBuilder.array(TABLE_CHANGE_SCHEMA).build())
            .build();
    private static final Schema DATA_SCHEMA = SchemaBuilder.struct()
            .name("topic.inventory.orders.Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .build();

    @Test
    @DisplayName("should select the invokers of the engine")
    void shouldSelectTheInvokersOfTheEngine() {
        RecordingInvoker assigned = new RecordingInvoker("default");
        RecordingInvoker other = new RecordingInvoker("alternative");

        assertThat(SchemaChangeInvokers.select(ENGINE, Map.of(), List.of(assigned, other))).containsExactly(assigned);
    }

    @Test
    @DisplayName("should return the consumers untouched for an engine without invokers")
    void shouldNotDecorateWithoutInvokers() {
        RecordingBatchConsumer batchConsumer = new RecordingBatchConsumer(new ArrayList<>());
        SourceRecordEventConsumer eventConsumer = event -> {
        };

        assertThat(SchemaChangeInvokers.apply(batchConsumer, ENGINE, List.of())).isSameAs(batchConsumer);
        assertThat(SchemaChangeInvokers.apply(eventConsumer, ENGINE, List.of())).isSameAs(eventConsumer);
    }

    @Test
    @DisplayName("should map the schema change record")
    void shouldMapTheSchemaChangeRecord() {
        SourceRecord record = schemaChange("CREATE TABLE orders (id INT)");

        SchemaChange change = SchemaChangeInvokers.toSchemaChange(ENGINE, SCHEMA_CHANGE_TOPIC, record);

        assertThat(change.engine()).isEqualTo("default");
        assertThat(change.destination()).isEqualTo(SCHEMA_CHANGE_TOPIC);
        assertThat(change.database()).isEqualTo("inventory");
        assertThat(change.schema()).isNull();
        assertThat(change.ddl()).isEqualTo("CREATE TABLE orders (id INT)");
        assertThat(change.timestamp()).isEqualTo(Instant.ofEpochMilli(1_000L));
        assertThat(change.source().getString("db")).isEqualTo("inventory");
        assertThat(change.tableChanges()).hasSize(1);
        assertThat(change.tableChanges().getFirst().getString("type")).isEqualTo("CREATE");
        assertThat(change.record()).isSameAs(record);
    }

    @Test
    @DisplayName("should hand a batch without schema changes to the consumer as it is")
    void shouldHandOverBatchWithoutSchemaChanges() throws InterruptedException {
        List<String> calls = new ArrayList<>();
        RecordingInvoker invoker = new RecordingInvoker("default", calls);
        RecordingBatchConsumer consumer = new RecordingBatchConsumer(calls);
        RecordingCommitter committer = new RecordingCommitter();
        List<ChangeEvent<Object, Object>> batch = List.of(event(data("a")), event(data("b")));

        SchemaChangeInvokers.apply(consumer, ENGINE, List.of(invoker)).handleBatch(batch, committer);

        assertThat(consumer.batches()).containsExactly(batch);
        assertThat(invoker.changes()).isEmpty();
        assertThat(committer.processed()).isEmpty();
        assertThat(committer.finished()).isEqualTo(1);
    }

    @Test
    @DisplayName("should split the batch at the schema changes and withhold them from the consumer")
    void shouldSplitTheBatchAtSchemaChanges() throws InterruptedException {
        List<String> calls = new ArrayList<>();
        RecordingInvoker first = new RecordingInvoker("default", calls);
        RecordingInvoker second = new RecordingInvoker("default", calls);
        RecordingBatchConsumer consumer = new RecordingBatchConsumer(calls);
        RecordingCommitter committer = new RecordingCommitter();

        ChangeEvent<Object, Object> a = event(data("a"));
        ChangeEvent<Object, Object> create = event(schemaChange("CREATE TABLE orders (id INT)"));
        ChangeEvent<Object, Object> b = event(data("b"));
        ChangeEvent<Object, Object> c = event(data("c"));
        ChangeEvent<Object, Object> alter = event(schemaChange("ALTER TABLE orders ADD name VARCHAR(10)"));

        SchemaChangeInvokers.apply(consumer, ENGINE, List.of(first, second)).handleBatch(List.of(a, create, b, c, alter), committer);

        assertThat(consumer.batches()).containsExactly(List.of(a), List.of(b, c));
        assertThat(first.changes()).extracting(SchemaChange::ddl)
                .containsExactly("CREATE TABLE orders (id INT)", "ALTER TABLE orders ADD name VARCHAR(10)");
        assertThat(second.changes()).isEqualTo(first.changes());
        assertThat(calls).containsExactly(
                "batch", "CREATE TABLE orders (id INT)", "CREATE TABLE orders (id INT)",
                "batch", "ALTER TABLE orders ADD name VARCHAR(10)", "ALTER TABLE orders ADD name VARCHAR(10)");
        assertThat(committer.processed()).containsExactly(create, alter);
        // one per segment handed to the consumer and one for the trailing schema change
        assertThat(committer.finished()).isEqualTo(3);
    }

    @Test
    @DisplayName("should not call the consumer for a batch that contains only schema changes")
    void shouldNotCallTheConsumerForSchemaChangesOnly() throws InterruptedException {
        RecordingInvoker invoker = new RecordingInvoker("default");
        RecordingBatchConsumer consumer = new RecordingBatchConsumer(new ArrayList<>());
        RecordingCommitter committer = new RecordingCommitter();
        ChangeEvent<Object, Object> create = event(schemaChange("CREATE TABLE orders (id INT)"));

        SchemaChangeInvokers.apply(consumer, ENGINE, List.of(invoker)).handleBatch(List.of(create), committer);

        assertThat(consumer.batches()).isEmpty();
        assertThat(invoker.changes()).hasSize(1);
        assertThat(committer.processed()).containsExactly(create);
        assertThat(committer.finished()).isEqualTo(1);
    }

    @Test
    @DisplayName("should deliver a single schema change to the invokers and the other events to the consumer")
    void shouldRouteSingleEvents() {
        RecordingInvoker invoker = new RecordingInvoker("default");
        List<ChangeEvent<SourceRecord, SourceRecord>> consumed = new ArrayList<>();
        SourceRecordEventConsumer delegate = consumed::add;
        SourceRecordEventConsumer consumer = SchemaChangeInvokers.apply(delegate, ENGINE, List.of(invoker));

        ChangeEvent<SourceRecord, SourceRecord> data = event(data("a"));
        consumer.accept(data);
        consumer.accept(event(schemaChange("CREATE TABLE orders (id INT)")));

        assertThat(consumed).containsExactly(data);
        assertThat(invoker.changes()).extracting(SchemaChange::ddl).containsExactly("CREATE TABLE orders (id INT)");
        assertThat(invoker.changes()).extracting(SchemaChange::destination).containsExactly(SCHEMA_CHANGE_TOPIC);
    }

    private static SourceRecord schemaChange(String ddl) {
        Struct value = new Struct(SCHEMA_CHANGE_SCHEMA)
                .put(Fields.SOURCE, new Struct(SOURCE_SCHEMA).put("db", "inventory"))
                .put(Fields.TIMESTAMP, 1_000L)
                .put(Fields.DATABASE_NAME, "inventory")
                .put(Fields.DDL_STATEMENTS, ddl)
                .put(Fields.TABLE_CHANGES, List.of(new Struct(TABLE_CHANGE_SCHEMA).put("type", "CREATE").put("id", "inventory.orders")));

        return new SourceRecord(null, null, SCHEMA_CHANGE_TOPIC, SCHEMA_CHANGE_SCHEMA, value);
    }

    private static SourceRecord data(String operation) {
        return new SourceRecord(null, null, DATA_TOPIC, DATA_SCHEMA, new Struct(DATA_SCHEMA).put("op", operation));
    }

    /**
     * An event of the plain Connect format, which carries the {@link SourceRecord} as its value.
     */
    @SuppressWarnings("unchecked")
    private static <K, V> ChangeEvent<K, V> event(SourceRecord record) {
        return new ChangeEvent<>() {
            @Override
            public K key() {
                return null;
            }

            @Override
            public V value() {
                return (V) record;
            }

            @Override
            public String destination() {
                return record.topic();
            }

            @Override
            public Integer partition() {
                return null;
            }
        };
    }

    private static final class RecordingInvoker implements SchemaChangeInvoker {
        private final String engine;
        private final List<String> calls;
        private final List<SchemaChange> changes = new ArrayList<>();

        private RecordingInvoker(String engine) {
            this(engine, new ArrayList<>());
        }

        private RecordingInvoker(String engine, List<String> calls) {
            this.engine = engine;
            this.calls = calls;
        }

        @Override
        public String engine() {
            return engine;
        }

        @Override
        public void capture(SchemaChange change) {
            calls.add(change.ddl());
            changes.add(change);
        }

        private List<SchemaChange> changes() {
            return changes;
        }
    }

    private static final class RecordingBatchConsumer implements QuarkusChangeConsumer {
        private final List<String> calls;
        private final List<List<ChangeEvent<Object, Object>>> batches = new ArrayList<>();

        private RecordingBatchConsumer(List<String> calls) {
            this.calls = calls;
        }

        @Override
        public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
                throws InterruptedException {
            calls.add("batch");
            batches.add(List.copyOf(records));
            committer.markBatchFinished();
        }

        private List<List<ChangeEvent<Object, Object>>> batches() {
            return batches;
        }
    }

    private static final class RecordingCommitter implements RecordCommitter<ChangeEvent<Object, Object>> {
        private final List<ChangeEvent<Object, Object>> processed = new ArrayList<>();
        private int finished;

        @Override
        public void markProcessed(ChangeEvent<Object, Object> record) {
            processed.add(record);
        }

        @Override
        public void markBatchFinished() {
            finished++;
        }

        @Override
        public void markProcessed(ChangeEvent<Object, Object> record, io.debezium.engine.DebeziumEngine.Offsets sourceOffsets) {
            processed.add(record);
        }

        @Override
        public io.debezium.engine.DebeziumEngine.Offsets buildOffsets() {
            throw new UnsupportedOperationException();
        }

        private List<ChangeEvent<Object, Object>> processed() {
            return processed;
        }

        private int finished() {
            return finished;
        }
    }
}
