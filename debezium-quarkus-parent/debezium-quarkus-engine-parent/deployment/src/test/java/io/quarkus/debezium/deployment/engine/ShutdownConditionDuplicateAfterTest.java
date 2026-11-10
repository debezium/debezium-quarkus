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

import io.debezium.runtime.ShutdownCondition;
import io.debezium.runtime.ShutdownCondition.Phase;
import io.quarkus.test.QuarkusUnitTest;

/**
 * Only one method per phase is allowed: two {@code AFTER} methods must fail the build.
 */
public class ShutdownConditionDuplicateAfterTest {

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(DuplicateAfterHandler.class))
            .overrideConfigKey("quarkus.debezium.engine.autostart", "false")
            .overrideConfigKey("quarkus.debezium.name", "shutdown")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "shutdown")
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .assertException(t -> assertThat(t)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("only one @ShutdownCondition(AFTER) method is allowed")
                    .hasMessageContaining("DuplicateAfterHandler.first")
                    .hasMessageContaining("DuplicateAfterHandler.second"));

    @Test
    @DisplayName("should fail the build when more than one AFTER shutdown method is declared")
    void shouldFailBuild() {
    }

    @ApplicationScoped
    static class DuplicateAfterHandler {

        @ShutdownCondition(Phase.AFTER)
        public boolean first(SourceRecord record) {
            return true;
        }

        @ShutdownCondition(Phase.AFTER)
        public boolean second(SourceRecord record) {
            return false;
        }
    }
}
