/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.testsuite.deployment.suite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

import java.util.concurrent.TimeUnit;
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
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.debezium.testsuite.deployment.TestSuiteConfigurations;
import io.quarkus.test.QuarkusUnitTest;

@Tag(SuiteTags.DEFAULT)
public class CapturingEventsTest {

    @Inject
    CaptureProductsHandler captureProductsHandler;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(CaptureProductsHandler.class))
            .withConfigurationResource("quarkus-debezium-testsuite.properties");

    @Test
    @DisplayName("should call the filtered by destination capture")
    void shouldInvokeFilteredByDestinationCapture() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(captureProductsHandler.filteredEvent()).isEqualTo(2));
    }

    @ApplicationScoped
    static class CaptureProductsHandler {
        private final AtomicInteger isCapturingFilteredEvent = new AtomicInteger(0);

        @Capturing(destination = "topic.inventory.products")
        public void capture(CapturingEvents<BatchEvent> events) {
            isCapturingFilteredEvent.set(events.records().size());
        }

        public int filteredEvent() {
            return isCapturingFilteredEvent.get();
        }

    }

}
