/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.testsuite.deployment.suite;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.CapturingFilterStrategy;
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.test.QuarkusUnitTest;

@Tag(SuiteTags.DEFAULT)
public class CapturingFilterValidationTest {

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(
                    InvalidHandler.class,
                    DummyFilter.class))
            .withConfigurationResource("debezium-quarkus-testsuite.properties")
            .assertException(t -> assertThat(t)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot set 'destination' or 'engine' when 'filter' is specified"));

    @Test
    @DisplayName("should fail the build when filter is combined with destination")
    void shouldFailBuild() {
    }

    @ApplicationScoped
    static class InvalidHandler {
        @Capturing(destination = "topic.inventory.products", filter = DummyFilter.class)
        public void invalidCapture(CapturingEvent<SourceRecord, SourceRecord> event) {
        }
    }

    @ApplicationScoped
    public static class DummyFilter implements CapturingFilterStrategy {
        @Override
        public boolean shouldCapture(CapturingEvent<SourceRecord, SourceRecord> event) {
            return true;
        }
    }
}
