/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.testsuite.deployment.suite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.given;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.doc.FixFor;
import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.CapturingSchemaChange;
import io.debezium.runtime.SchemaChange;
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.debezium.testsuite.deployment.TestSuiteConfigurations;
import io.quarkus.test.QuarkusUnitTest;

/**
 * The schema changes emitted during the snapshot are delivered to the {@link CapturingSchemaChange} method and withheld
 * from the {@link Capturing} method, which keeps receiving the data events.
 */
@Tag(SuiteTags.DEFAULT)
public class CapturingSchemaChangeTest {

    private static final String SCHEMA_CHANGE_TOPIC = "topic";
    private static final String ORDERS_TOPIC = "topic.inventory.orders";

    @Inject
    SchemaChangeHandler handler;

    @ConfigProperty(name = "quarkus.datasource.db-kind")
    String dbKind;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(SchemaChangeHandler.class))
            .withConfigurationResource("debezium-quarkus-testsuite.properties")
            .overrideConfigKey("quarkus.debezium.include.schema.changes", "true");

    @Test
    @FixFor("debezium/dbz#1350")
    @DisplayName("should deliver the schema changes to the schema change method only")
    void shouldDeliverSchemaChangesToTheSchemaChangeMethodOnly() {
        assumeFalse("postgresql".equals(dbKind), "the PostgreSQL connector does not emit schema change events");

        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(handler.changes()).isNotEmpty();
                    assertThat(handler.destinations()).contains(ORDERS_TOPIC);
                });

        assertThat(handler.changes()).allSatisfy(change -> {
            assertThat(change.engine()).isEqualTo("default");
            assertThat(change.destination()).isEqualTo(SCHEMA_CHANGE_TOPIC);
            assertThat(change.timestamp()).isNotNull();
            assertThat(change.source()).isNotNull();
            assertThat(change.record().topic()).isEqualTo(SCHEMA_CHANGE_TOPIC);
        });
        assertThat(handler.changes()).anySatisfy(change -> assertThat(change.tableChanges()).isNotEmpty());
        assertThat(handler.destinations()).doesNotContain(SCHEMA_CHANGE_TOPIC);
    }

    @ApplicationScoped
    static class SchemaChangeHandler {
        private final List<SchemaChange> changes = new CopyOnWriteArrayList<>();
        private final List<String> destinations = new CopyOnWriteArrayList<>();

        @CapturingSchemaChange
        public void onSchemaChange(SchemaChange change) {
            changes.add(change);
        }

        @Capturing
        public void capture(CapturingEvent<SourceRecord, SourceRecord> event) {
            destinations.add(event.destination());
        }

        public List<SchemaChange> changes() {
            return changes;
        }

        public List<String> destinations() {
            return destinations;
        }
    }
}
