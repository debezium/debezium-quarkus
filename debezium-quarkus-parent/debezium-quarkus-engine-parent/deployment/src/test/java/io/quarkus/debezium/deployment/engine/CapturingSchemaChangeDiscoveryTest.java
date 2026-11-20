/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.deployment.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.CapturingSchemaChange;
import io.debezium.runtime.SchemaChange;
import io.quarkus.debezium.engine.schemachange.SchemaChangeInvoker;
import io.quarkus.test.QuarkusUnitTest;

/**
 * Each {@link CapturingSchemaChange} method is turned into a {@link SchemaChangeInvoker} that delegates to the bean method.
 */
public class CapturingSchemaChangeDiscoveryTest {

    @Inject
    Instance<SchemaChangeInvoker> invokers;

    @Inject
    SchemaChangeHandler handler;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(SchemaChangeHandler.class))
            .overrideConfigKey("quarkus.debezium.engine.autostart", "false")
            .overrideConfigKey("quarkus.debezium.name", "schemachange")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "schemachange")
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");

    @Test
    @DisplayName("should discover one invoker per method, assigned to the declared engine, and delegate to the annotated method")
    void shouldDiscoverInvokersAndDelegate() {
        assertThat(invokers.stream().map(SchemaChangeInvoker::engine)).containsExactlyInAnyOrder("default", "alternative");

        SchemaChange change = new SchemaChange("default", "schemachange", "inventory", null, "CREATE TABLE orders (id INT)",
                Instant.EPOCH, null, List.of(), null);

        invokers.stream().filter(invoker -> invoker.engine().equals("default")).findFirst().orElseThrow().capture(change);
        assertThat(handler.defaultChanges()).containsExactly(change);
        assertThat(handler.alternativeChanges()).isEmpty();

        invokers.stream().filter(invoker -> invoker.engine().equals("alternative")).findFirst().orElseThrow().capture(change);
        assertThat(handler.alternativeChanges()).containsExactly(change);
    }

    @ApplicationScoped
    static class SchemaChangeHandler {
        private final List<SchemaChange> defaultChanges = new CopyOnWriteArrayList<>();
        private final List<SchemaChange> alternativeChanges = new CopyOnWriteArrayList<>();

        @CapturingSchemaChange
        public void onSchemaChange(SchemaChange change) {
            defaultChanges.add(change);
        }

        @CapturingSchemaChange(engine = "alternative")
        public void onAlternativeSchemaChange(SchemaChange change) {
            alternativeChanges.add(change);
        }

        public List<SchemaChange> defaultChanges() {
            return defaultChanges;
        }

        public List<SchemaChange> alternativeChanges() {
            return alternativeChanges;
        }
    }
}
