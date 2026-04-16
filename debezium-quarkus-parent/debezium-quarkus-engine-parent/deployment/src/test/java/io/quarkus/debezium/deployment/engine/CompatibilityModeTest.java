/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.given;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.Connector;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.EngineManifest;
import io.quarkus.maven.dependency.Dependency;
import io.quarkus.test.QuarkusUnitTest;
import io.quarkus.test.common.QuarkusTestResource;

@QuarkusTestResource(InformixResource.class)
public class CompatibilityModeTest {

    @Inject
    DebeziumConnectorRegistry registry;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .setForcedDependencies(List.of(
                    Dependency.of("io.debezium", "debezium-connector-informix"),
                    Dependency.of("com.ibm.informix", "jdbc")))
            .withApplicationRoot((jar) -> jar.addClasses(CaptureProductsHandler.class))
            .withConfigurationResource("debezium-quarkus.properties");

    @Test
    @DisplayName("should inject a connector in compatibility mode")
    void shouldInjectConnectorInCompatibilityMode() {
        given().await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(registry.get(new EngineManifest("default"))
                        .connector())
                        .isEqualTo(new Connector("io.debezium.connector.informix.InformixConnector")));

    }

    @ApplicationScoped
    static class CaptureProductsHandler {
        private final AtomicBoolean isInvoked = new AtomicBoolean(false);

        @Capturing()
        public void newCapture(CapturingEvent<SourceRecord, SourceRecord> event) {
            isInvoked.set(true);
        }

        public boolean isInvoked() {
            return isInvoked.getAndSet(false);
        }

    }
}
