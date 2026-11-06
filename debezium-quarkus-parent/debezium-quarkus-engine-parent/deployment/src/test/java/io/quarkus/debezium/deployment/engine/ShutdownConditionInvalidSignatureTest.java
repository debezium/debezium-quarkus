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
 * The annotated method must return {@code boolean}: a {@code void} method must fail the build.
 */
public class ShutdownConditionInvalidSignatureTest {

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(InvalidSignatureHandler.class))
            .overrideConfigKey("quarkus.debezium.engine.autostart", "false")
            .overrideConfigKey("quarkus.debezium.name", "shutdown")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "shutdown")
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .assertException(t -> assertThat(t)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("@ShutdownCondition on method")
                    .hasMessageContaining("InvalidSignatureHandler.stop")
                    .hasMessageContaining("must be a non-static, non-private method that returns boolean"));

    @Test
    @DisplayName("should fail the build when the shutdown method does not return boolean")
    void shouldFailBuild() {
    }

    @ApplicationScoped
    static class InvalidSignatureHandler {

        @ShutdownCondition(Phase.AFTER)
        public void stop(SourceRecord record) {
        }
    }
}
