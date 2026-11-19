/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.deployment.engine;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.CapturingSchemaChange;
import io.quarkus.test.QuarkusUnitTest;

/**
 * The annotated method must take a {@link io.debezium.runtime.SchemaChange}: any other parameter must fail the build.
 */
public class CapturingSchemaChangeInvalidSignatureTest {

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(InvalidSignatureHandler.class))
            .overrideConfigKey("quarkus.debezium.engine.autostart", "false")
            .overrideConfigKey("quarkus.debezium.name", "schemachange")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "schemachange")
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .assertException(t -> assertThat(t)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("@CapturingSchemaChange on method")
                    .hasMessageContaining("InvalidSignatureHandler.onSchemaChange")
                    .hasMessageContaining("must be a non-static, non-private method that returns void"));

    @Test
    @DisplayName("should fail the build when the schema change method does not take a SchemaChange")
    void shouldFailBuild() {
    }

    @ApplicationScoped
    static class InvalidSignatureHandler {

        @CapturingSchemaChange
        public void onSchemaChange(SourceRecord record) {
        }
    }
}
