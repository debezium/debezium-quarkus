/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.common.annotation.Incubating;

/**
 * Abstract {@link CapturingFilterStrategy} that captures events only when specific fields are impacted.
 * Supports both relational (Struct-based) and document (JSON String-based) connectors.
 * <p>
 * For relational connectors, Update events compare field values between before and after Structs.
 * For document connectors (e.g. MongoDB), where before/after are JSON strings,
 * the filter checks whether any watched field is present in the after JSON.
 * <p>
 * Usage:
 * <pre>
 * &#64;ApplicationScoped
 * public class MyFilter extends CapturingFieldsFilterStrategy {
 *     public MyFilter() {
 *         super(Set.of("name", "price"));
 *     }
 * }
 * </pre>
 * Then use it in the annotation:
 * <pre>
 * &#64;Capturing(filter = MyFilter.class)
 * public void onEvent(CapturingEvent&lt;SourceRecord, SourceRecord&gt; event) { ... }
 * </pre>
 */
@Incubating
public abstract class CapturingFieldsFilterStrategy implements CapturingFilterStrategy {

    private final Set<String> fields;

    protected CapturingFieldsFilterStrategy(Set<String> fields) {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("CapturingFieldsFilterStrategy requires at least one field");
        }
        this.fields = fields;
    }

    @Override
    public boolean shouldCapture(CapturingEvent<SourceRecord, SourceRecord> event) {
        if (!(event.record().value() instanceof Struct payload)) {
            return false;
        }

        return switch (event) {
            case CapturingEvent.Update<SourceRecord, SourceRecord> ignored -> shouldCaptureUpdateEvent(payload);
            case CapturingEvent.Create<SourceRecord, SourceRecord> ignored -> hasField(payload, "after");
            case CapturingEvent.Read<SourceRecord, SourceRecord> ignored -> hasField(payload, "after");
            case CapturingEvent.Delete<SourceRecord, SourceRecord> ignored -> hasField(payload, "before");
            case CapturingEvent.Truncate<SourceRecord, SourceRecord> ignored -> false;
            case CapturingEvent.Message<SourceRecord, SourceRecord> ignored -> false;
        };
    }

    private boolean shouldCaptureUpdateEvent(Struct payload) {
        if (isStructField(payload, "before") && isStructField(payload, "after")) {
            Struct before = payload.getStruct("before");
            Struct after = payload.getStruct("after");
            if (before == null || after == null) {
                return false;
            }
            return hasFieldChanged(before, after);
        }

        return hasField(payload, "after");
    }

    private boolean hasField(Struct payload, String envelopeField) {
        if (payload.schema().field(envelopeField) == null) {
            return false;
        }

        if (isStructField(payload, envelopeField)) {
            Struct struct = payload.getStruct(envelopeField);
            return struct != null && hasFieldInStruct(struct);
        }

        String json = payload.getString(envelopeField);
        return json != null && hasFieldInJson(json);
    }

    private boolean isStructField(Struct payload, String fieldName) {
        var field = payload.schema().field(fieldName);
        return field != null && field.schema().type() == Schema.Type.STRUCT;
    }

    private boolean hasFieldInStruct(Struct struct) {
        return fields.stream()
                .anyMatch(field -> resolveFieldName(struct, field) != null);
    }

    private boolean hasFieldInJson(String json) {
        return fields.stream()
                .anyMatch(field -> json.contains("\"" + field + "\""));
    }

    private boolean hasFieldChanged(Struct before, Struct after) {
        return fields.stream()
                .filter(isFieldInSchema(before, after))
                .anyMatch(field -> !Objects.equals(
                        before.get(resolveFieldName(before, field)),
                        after.get(resolveFieldName(after, field))));
    }

    private Predicate<String> isFieldInSchema(Struct before, Struct after) {
        return field -> resolveFieldName(before, field) != null && resolveFieldName(after, field) != null;
    }

    private static String resolveFieldName(Struct struct, String field) {
        if (struct.schema().field(field) != null) {
            return field;
        }
        return struct.schema().fields().stream()
                .map(org.apache.kafka.connect.data.Field::name)
                .filter(name -> name.equalsIgnoreCase(field))
                .findFirst()
                .orElse(null);
    }
}
