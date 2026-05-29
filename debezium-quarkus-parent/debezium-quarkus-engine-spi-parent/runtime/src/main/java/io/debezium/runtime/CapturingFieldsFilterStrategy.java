/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.common.annotation.Incubating;

/**
 * Abstract {@link CapturingFilterStrategy} that captures events only when specific fields are impacted.
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
        Struct payload = (Struct) event.record().value();
        if (payload == null) {
            return false;
        }

        return switch (event) {
            case CapturingEvent.Update<SourceRecord, SourceRecord> ignored -> shouldCaptureUpdateEvent(payload);
            case CapturingEvent.Create<SourceRecord, SourceRecord> ignored -> hasFieldInSchema(getStructOrNull(payload, "after"));
            case CapturingEvent.Read<SourceRecord, SourceRecord> ignored -> hasFieldInSchema(getStructOrNull(payload, "after"));
            case CapturingEvent.Delete<SourceRecord, SourceRecord> ignored -> hasFieldInSchema(getStructOrNull(payload, "before"));
            case CapturingEvent.Truncate<SourceRecord, SourceRecord> ignored -> false;
            case CapturingEvent.Message<SourceRecord, SourceRecord> ignored -> false;
        };
    }

    private boolean shouldCaptureUpdateEvent(Struct payload) {
        Struct before = getStructOrNull(payload, "before");
        Struct after = getStructOrNull(payload, "after");
        if (before == null || after == null) {
            return false;
        }
        return hasFieldChanged(before, after);
    }

    private Struct getStructOrNull(Struct payload, String fieldName) {
        if (payload.schema().field(fieldName) == null) {
            return null;
        }
        return payload.getStruct(fieldName);
    }

    private boolean hasFieldInSchema(Struct struct) {
        if (struct == null) {
            return true;
        }
        return fields.stream()
                .anyMatch(field -> struct.schema().field(field) != null);
    }

    private boolean hasFieldChanged(Struct before, Struct after) {
        return fields.stream()
                .filter(isFieldInSchema(before, after))
                .anyMatch(field -> !Objects.equals(before.get(field), after.get(field)));
    }

    private Predicate<String> isFieldInSchema(Struct before, Struct after) {
        return field -> before.schema().field(field) != null && after.schema().field(field) != null;
    }
}
