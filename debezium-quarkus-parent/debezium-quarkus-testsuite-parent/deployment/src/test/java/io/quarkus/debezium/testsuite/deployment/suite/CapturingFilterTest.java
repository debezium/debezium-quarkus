/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.testsuite.deployment.suite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.CapturingFilterStrategy;
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.debezium.testsuite.deployment.TestSuiteConfigurations;
import io.quarkus.test.QuarkusUnitTest;

@Tag(SuiteTags.DEFAULT)
public class CapturingFilterTest {

    @Inject
    FilterHandler filterHandler;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(
                    FilterHandler.class,
                    ProductsDestinationFilter.class))
            .withConfigurationResource("debezium-quarkus-testsuite.properties");

    @Test
    @DisplayName("should invoke the filter-based capture only for products destination")
    void shouldInvokeFilterBasedCapture() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(filterHandler.isInvoked()).isTrue());
    }

    @Test
    @DisplayName("should capture only products events via filter")
    void shouldCaptureOnlyProductsEvents() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(filterHandler.capturedCount()).isEqualTo(2));
    }

    @ApplicationScoped
    static class FilterHandler {
        private final AtomicBoolean isInvoked = new AtomicBoolean(false);
        private final AtomicInteger capturedCount = new AtomicInteger(0);

        @Capturing(filter = ProductsDestinationFilter.class)
        public void captureFiltered(CapturingEvent<SourceRecord, SourceRecord> event) {
            isInvoked.set(true);
            capturedCount.incrementAndGet();
        }

        public boolean isInvoked() {
            return isInvoked.get();
        }

        public int capturedCount() {
            return capturedCount.get();
        }
    }

    @ApplicationScoped
    public static class ProductsDestinationFilter implements CapturingFilterStrategy {
        @Override
        public boolean shouldCapture(CapturingEvent<SourceRecord, SourceRecord> event) {
            return "topic.inventory.products".equals(event.destination());
        }
    }
}
