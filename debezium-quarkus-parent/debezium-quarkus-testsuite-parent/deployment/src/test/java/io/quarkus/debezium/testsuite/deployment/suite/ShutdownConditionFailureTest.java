/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.testsuite.deployment.suite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.given;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.ShutdownCondition;
import io.debezium.runtime.ShutdownCondition.Phase;
import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.debezium.testsuite.deployment.TestSuiteConfigurations;
import io.quarkus.test.QuarkusUnitTest;

/**
 * An exception thrown by the shutdown method is not treated as {@code false}: the engine fails and reports an unsuccessful
 * {@link DebeziumCompletionEvent}.
 */
@Tag(SuiteTags.DEFAULT)
public class ShutdownConditionFailureTest {

    private static final String FAILURE_MESSAGE = "shutdown condition failed";

    @Inject
    ShutdownHandler shutdownHandler;

    @Inject
    DebeziumConnectorRegistry debeziumConnectorRegistry;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(ShutdownHandler.class))
            .withConfigurationResource("debezium-quarkus-testsuite.properties");

    @Test
    @DisplayName("should fail the engine when the shutdown method throws")
    void shouldFailTheEngineWhenTheShutdownMethodThrows() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(shutdownHandler.completion()).isNotNull());

        DebeziumCompletionEvent completion = shutdownHandler.completion();

        assertThat(completion.isSuccess()).isFalse();
        assertThat(completion.getError())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(FAILURE_MESSAGE);
        assertThat(debeziumConnectorRegistry.get(new EngineManifest("default")).status())
                .isEqualTo(new DebeziumStatus(DebeziumStatus.State.STOPPED));
    }

    @ApplicationScoped
    static class ShutdownHandler {
        private final AtomicReference<DebeziumCompletionEvent> completion = new AtomicReference<>();

        public void completed(@Observes DebeziumCompletionEvent event) {
            completion.set(event);
        }

        public DebeziumCompletionEvent completion() {
            return completion.get();
        }

        @ShutdownCondition(Phase.AFTER)
        public boolean failingCondition(SourceRecord record) {
            throw new IllegalStateException(FAILURE_MESSAGE);
        }
    }
}
