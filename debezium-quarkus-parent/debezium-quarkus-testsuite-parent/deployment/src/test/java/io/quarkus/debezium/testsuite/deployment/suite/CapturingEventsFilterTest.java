/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.testsuite.deployment.suite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvents;
import io.debezium.runtime.CapturingFieldsFilterStrategy;
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.debezium.testsuite.deployment.TestSuiteConfigurations;
import io.quarkus.test.QuarkusUnitTest;

@Tag(SuiteTags.DEFAULT)
public class CapturingEventsFilterTest {

    @Inject
    BatchFilterHandler handler;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(
                    BatchFilterHandler.class,
                    KeyFieldBatchFilter.class))
            .withConfigurationResource("debezium-quarkus-testsuite.properties");

    @Test
    @DisplayName("should capture orders batch events via filter")
    void shouldCaptureOrdersBatchViaFilter() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(handler.filterCount()).isGreaterThanOrEqualTo(2));
    }

    @Test
    @DisplayName("should fallback to wildcard for batch events not matched by filter")
    void shouldFallbackToWildcardForBatch() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(handler.isWildcardInvoked()).isTrue());
    }

    @ApplicationScoped
    static class BatchFilterHandler {
        private final AtomicInteger filterCount = new AtomicInteger(0);
        private final AtomicBoolean wildcardInvoked = new AtomicBoolean(false);

        @Capturing(filter = KeyFieldBatchFilter.class)
        public void captureFilteredBatch(CapturingEvents<BatchEvent> events) {
            filterCount.addAndGet(events.records().size());
        }

        @Capturing()
        public void captureWildcard(CapturingEvents<BatchEvent> events) {
            wildcardInvoked.set(true);
        }

        public int filterCount() {
            return filterCount.get();
        }

        public boolean isWildcardInvoked() {
            return wildcardInvoked.get();
        }
    }

    @ApplicationScoped
    public static class KeyFieldBatchFilter extends CapturingFieldsFilterStrategy {
        public KeyFieldBatchFilter() {
            super(Set.of("key"));
        }
    }
}
