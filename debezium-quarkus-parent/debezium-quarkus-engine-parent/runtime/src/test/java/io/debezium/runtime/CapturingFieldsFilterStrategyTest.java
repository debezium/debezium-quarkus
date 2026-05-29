/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CapturingFieldsFilterStrategyTest {

    private static final String DESTINATION = "topic.inventory.users";
    private static final String ENGINE = "default";

    @Nested
    @DisplayName("given no fields are specified")
    class NoFieldsSpecified {

        @Test
        @DisplayName("when fields are empty then it should throw IllegalArgumentException")
        void shouldThrowWhenFieldsAreEmpty() {
            assertThatThrownBy(() -> new TestFilter(Set.of()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("when fields are null then it should throw IllegalArgumentException")
        void shouldThrowWhenFieldsAreNull() {
            assertThatThrownBy(() -> new TestFilter(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("given a Create event")
    class CreateEvent {

        private final CapturingFieldsFilterStrategy underTest = new TestFilter(Set.of("name"));

        @Test
        @DisplayName("when the watched field exists in the schema then it should be captured")
        void shouldCaptureWhenFieldExistsInSchema() {
            Schema schema = schemaWith("id", "name", "price");
            var event = createCreateEvent(structWith(schema, "id", "1", "name", "t-shirt", "price", "10"));

            assertThat(underTest.shouldCapture(event)).isTrue();
        }

        @Test
        @DisplayName("when the watched field does not exist in the schema then it should be suppressed")
        void shouldSuppressWhenFieldNotInSchema() {
            Schema schema = schemaWith("id");
            var event = createCreateEvent(structWith(schema, "id", "1"));

            assertThat(underTest.shouldCapture(event)).isFalse();
        }
    }

    @Nested
    @DisplayName("given a Read event")
    class ReadEvent {

        private final CapturingFieldsFilterStrategy underTest = new TestFilter(Set.of("name"));

        @Test
        @DisplayName("when the watched field exists in the schema then it should be captured")
        void shouldCaptureWhenFieldExistsInSchema() {
            Schema schema = schemaWith("id", "name");
            var event = createReadEvent(structWith(schema, "id", "1", "name", "giovanni"));

            assertThat(underTest.shouldCapture(event)).isTrue();
        }

        @Test
        @DisplayName("when the watched field does not exist in the schema then it should be suppressed")
        void shouldSuppressWhenFieldNotInSchema() {
            Schema schema = schemaWith("id");
            var event = createReadEvent(structWith(schema, "id", "1"));

            assertThat(underTest.shouldCapture(event)).isFalse();
        }
    }

    @Nested
    @DisplayName("given a Delete event")
    class DeleteEvent {

        private final CapturingFieldsFilterStrategy underTest = new TestFilter(Set.of("name"));

        @Test
        @DisplayName("when the watched field exists in the before schema then it should be captured")
        void shouldCaptureWhenFieldExistsInBeforeSchema() {
            Schema schema = schemaWith("id", "name");
            var event = createDeleteEvent(structWith(schema, "id", "1", "name", "giovanni"));

            assertThat(underTest.shouldCapture(event)).isTrue();
        }

        @Test
        @DisplayName("when the watched field does not exist in the before schema then it should be suppressed")
        void shouldSuppressWhenFieldNotInBeforeSchema() {
            Schema schema = schemaWith("id");
            var event = createDeleteEvent(structWith(schema, "id", "1"));

            assertThat(underTest.shouldCapture(event)).isFalse();
        }
    }

    @Nested
    @DisplayName("given an Update event")
    class UpdateEvent {

        private final CapturingFieldsFilterStrategy underTest = new TestFilter(Set.of("name"));

        @Test
        @DisplayName("when a watched field has changed then it should be captured")
        void shouldCaptureWhenWatchedFieldChanged() {
            Schema schema = schemaWith("id", "name", "description");
            var event = createUpdateEvent(
                    structWith(schema, "id", "1", "name", "giovanni", "description", "developer"),
                    structWith(schema, "id", "1", "name", "poppo", "description", "developer"));

            assertThat(underTest.shouldCapture(event)).isTrue();
        }

        @Test
        @DisplayName("when no watched fields have changed then it should be suppressed")
        void shouldSuppressWhenNoWatchedFieldsChanged() {
            Schema schema = schemaWith("id", "name", "description");
            var event = createUpdateEvent(
                    structWith(schema, "id", "1", "name", "giovanni", "description", "developer"),
                    structWith(schema, "id", "1", "name", "giovanni", "description", "senior developer"));

            assertThat(underTest.shouldCapture(event)).isFalse();
        }

        @Test
        @DisplayName("when at least one watched field has changed then it should be captured")
        void shouldCaptureWhenAtLeastOneFieldChanged() {
            var filter = new TestFilter(Set.of("name", "description"));
            Schema schema = schemaWith("id", "name", "description");
            var event = createUpdateEvent(
                    structWith(schema, "id", "1", "name", "giovanni", "description", "developer"),
                    structWith(schema, "id", "1", "name", "giovanni", "description", "senior developer"));

            assertThat(filter.shouldCapture(event)).isTrue();
        }

        @Test
        @DisplayName("when the watched field does not exist in the schema then it should be suppressed")
        void shouldSuppressWhenFieldNotInSchema() {
            Schema schema = schemaWith("id");
            var event = createUpdateEvent(
                    structWith(schema, "id", "1"),
                    structWith(schema, "id", "1"));

            assertThat(underTest.shouldCapture(event)).isFalse();
        }

        @Test
        @DisplayName("when the before struct is null then it should be suppressed")
        void shouldSuppressWhenBeforeIsNull() {
            Schema schema = schemaWith("id", "name");
            var event = createUpdateEventWithNullBefore(structWith(schema, "id", "1", "name", "poppo"));

            assertThat(underTest.shouldCapture(event)).isFalse();
        }
    }

    @Nested
    @DisplayName("given a Truncate event")
    class TruncateEvent {

        private final CapturingFieldsFilterStrategy underTest = new TestFilter(Set.of("name"));

        @Test
        @DisplayName("when payload is null then it should be suppressed")
        void shouldSuppressWhenPayloadIsNull() {
            SourceRecord record = new SourceRecord(null, null, DESTINATION, null, null);
            var event = new CapturingEvent.Truncate<SourceRecord, SourceRecord>(null, record, DESTINATION, "NOT_AVAILABLE", Collections.emptyList(), ENGINE);

            assertThat(underTest.shouldCapture(event)).isFalse();
        }
    }

    @Nested
    @DisplayName("given a Message event")
    class MessageEvent {

        private final CapturingFieldsFilterStrategy underTest = new TestFilter(Set.of("name"));

        @Test
        @DisplayName("when fields are specified then it should be suppressed")
        void shouldSuppressHeartbeat() {
            Schema schema = SchemaBuilder.struct().field("ts_ms", Schema.INT64_SCHEMA).build();
            Struct value = new Struct(schema).put("ts_ms", System.currentTimeMillis());
            SourceRecord record = new SourceRecord(null, null, "__debezium-heartbeat.topic", schema, value);
            var event = new CapturingEvent.Message<SourceRecord, SourceRecord>(null, record, "__debezium-heartbeat.topic", "NOT_AVAILABLE", Collections.emptyList(),
                    ENGINE);

            assertThat(underTest.shouldCapture(event)).isFalse();
        }
    }

    static class TestFilter extends CapturingFieldsFilterStrategy {
        TestFilter(Set<String> fields) {
            super(fields);
        }
    }

    private static Schema schemaWith(String... fieldNames) {
        SchemaBuilder builder = SchemaBuilder.struct().name("topic.inventory.users.Value");
        for (String fieldName : fieldNames) {
            builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
        }
        return builder.build();
    }

    private static Struct structWith(Schema schema, String... keyValues) {
        Struct struct = new Struct(schema);
        for (int i = 0; i < keyValues.length; i += 2) {
            struct.put(keyValues[i], keyValues[i + 1]);
        }
        return struct;
    }

    private static SourceRecord envelopeRecord(String op, Struct before, Struct after) {
        Schema dataSchema = before != null ? before.schema() : after.schema();
        Schema envelopeSchema = SchemaBuilder.struct()
                .name("topic.inventory.users.Envelope")
                .field("before", dataSchema)
                .field("after", dataSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();
        Struct envelope = new Struct(envelopeSchema);
        envelope.put("op", op);
        if (before != null) {
            envelope.put("before", before);
        }
        if (after != null) {
            envelope.put("after", after);
        }
        return new SourceRecord(null, null, DESTINATION, envelopeSchema, envelope);
    }

    private static CapturingEvent<SourceRecord, SourceRecord> createReadEvent(Struct after) {
        SourceRecord record = envelopeRecord("r", null, after);
        return new CapturingEvent.Read<>(null, record, DESTINATION, "NOT_AVAILABLE", Collections.emptyList(), ENGINE);
    }

    private static CapturingEvent<SourceRecord, SourceRecord> createCreateEvent(Struct after) {
        SourceRecord record = envelopeRecord("c", null, after);
        return new CapturingEvent.Create<>(null, record, DESTINATION, "NOT_AVAILABLE", Collections.emptyList(), ENGINE);
    }

    private static CapturingEvent<SourceRecord, SourceRecord> createDeleteEvent(Struct before) {
        SourceRecord record = envelopeRecord("d", before, null);
        return new CapturingEvent.Delete<>(null, record, DESTINATION, "NOT_AVAILABLE", Collections.emptyList(), ENGINE);
    }

    private static CapturingEvent<SourceRecord, SourceRecord> createUpdateEvent(Struct before, Struct after) {
        SourceRecord record = envelopeRecord("u", before, after);
        return new CapturingEvent.Update<>(null, record, DESTINATION, "NOT_AVAILABLE", Collections.emptyList(), ENGINE);
    }

    private static CapturingEvent<SourceRecord, SourceRecord> createUpdateEventWithNullBefore(Struct after) {
        Schema envelopeSchema = SchemaBuilder.struct()
                .name("topic.inventory.users.Envelope")
                .field("after", after.schema())
                .field("op", Schema.STRING_SCHEMA)
                .build();
        Struct envelope = new Struct(envelopeSchema);
        envelope.put("after", after);
        envelope.put("op", "u");
        SourceRecord record = new SourceRecord(null, null, DESTINATION, envelopeSchema, envelope);
        return new CapturingEvent.Update<>(null, record, DESTINATION, "NOT_AVAILABLE", Collections.emptyList(), ENGINE);
    }
}
