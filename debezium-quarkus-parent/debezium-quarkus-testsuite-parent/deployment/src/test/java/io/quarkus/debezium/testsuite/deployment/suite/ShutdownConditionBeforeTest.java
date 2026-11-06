/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.testsuite.deployment.suite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.given;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;
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
 * The BEFORE shutdown method stops the engine on the first record of the orders table, before the batch that contains it
 * is handed to the consumers: the engine stops and no record of the orders table reaches the consumers.
 */
@Tag(SuiteTags.DEFAULT)
public class ShutdownConditionBeforeTest {

    private static final String ORDERS_TOPIC = "topic.inventory.orders";

    @Inject
    ShutdownHandler shutdownHandler;

    @Inject
    DebeziumConnectorRegistry debeziumConnectorRegistry;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(ShutdownHandler.class))
            .withConfigurationResource("debezium-quarkus-testsuite.properties");

    @Test
    @DisplayName("should stop the engine before the batch of the triggering record reaches the consumers")
    void shouldStopTheEngineBeforeTheConsumers() {
        DebeziumCompletionEvent completion = awaitCompletion();

        assertThat(completion.isSuccess()).as("completion error: %s", completion.getError()).isTrue();
        assertThat(debeziumConnectorRegistry.get(new EngineManifest("default")).status())
                .isEqualTo(new DebeziumStatus(DebeziumStatus.State.STOPPED));

        assertThat(shutdownHandler.isTriggered()).isTrue();
        assertThat(shutdownHandler.destinations()).doesNotContain(ORDERS_TOPIC);
    }

    /**
     * The engine reports the end of the shutdown with a {@link DebeziumCompletionEvent}. Waiting for it keeps the application
     * running until the engine has stopped, and the status alone is not enough: it is {@code STOPPED} before the engine starts.
     */
    private DebeziumCompletionEvent awaitCompletion() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(shutdownHandler.completion()).isNotNull());

        return shutdownHandler.completion();
    }

    @ApplicationScoped
    static class ShutdownHandler {
        private final AtomicReference<DebeziumCompletionEvent> completion = new AtomicReference<>();
        private final List<String> destinations = new CopyOnWriteArrayList<>();
        private final AtomicBoolean triggered = new AtomicBoolean(false);

        @Capturing()
        public void capture(CapturingEvent<SourceRecord, SourceRecord> event) {
            destinations.add(event.destination());
        }

        public void completed(@Observes DebeziumCompletionEvent event) {
            completion.set(event);
        }

        public DebeziumCompletionEvent completion() {
            return completion.get();
        }

        @ShutdownCondition(Phase.BEFORE)
        public boolean stopOnFirstOrder(SourceRecord record) {
            return ORDERS_TOPIC.equals(record.topic()) && triggered.compareAndSet(false, true);
        }

        public List<String> destinations() {
            return destinations;
        }

        public boolean isTriggered() {
            return triggered.get();
        }
    }
}
