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

import io.debezium.runtime.Capturing;
import io.debezium.runtime.ShutdownCondition;
import io.debezium.runtime.ShutdownCondition.Phase;
import io.quarkus.test.QuarkusUnitTest;

/**
 * A method is processed for a single Debezium annotation: a method with {@code @Capturing} and {@code @ShutdownCondition} must
 * fail the build rather than having the shutdown condition silently ignored.
 */
public class CombinedAnnotationsTest {

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(CombinedHandler.class))
            .overrideConfigKey("quarkus.debezium.engine.autostart", "false")
            .overrideConfigKey("quarkus.debezium.name", "combined")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "combined")
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .assertException(t -> assertThat(t)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("CombinedHandler.both")
                    .hasMessageContaining("carries more than one Debezium annotation: @Capturing, @ShutdownCondition"));

    @Test
    @DisplayName("should fail the build when a method carries more than one Debezium annotation")
    void shouldFailBuild() {
    }

    @ApplicationScoped
    static class CombinedHandler {

        @Capturing
        @ShutdownCondition(Phase.AFTER)
        public boolean both(SourceRecord record) {
            return false;
        }
    }
}
