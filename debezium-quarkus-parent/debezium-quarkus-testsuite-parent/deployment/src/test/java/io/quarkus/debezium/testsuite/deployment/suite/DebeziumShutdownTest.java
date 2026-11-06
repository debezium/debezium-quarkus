/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.testsuite.deployment.suite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.given;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumShutdown;
import io.debezium.runtime.DebeziumShutdown.Phase;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.debezium.testsuite.deployment.TestSuiteConfigurations;
import io.quarkus.test.QuarkusUnitTest;

/**
 * The AFTER shutdown method stops the engine on the first record of the orders table. The batch that contains it must be
 * delivered to the consumers, and the offset of the triggering record must be committed before the engine stops.
 * <p>
 * The offsets are stored in a file and flushed on every batch (flush interval 0), so that the committed offset can be read
 * after the engine stopped. The path is fixed because the test class is loaded again by the Quarkus classloader.
 * The check does not depend on the connector: every entry of the source offset of the triggering record must be stored.
 */
@Tag(SuiteTags.DEFAULT)
public class DebeziumShutdownTest {

    private static final String ORDERS_TOPIC = "topic.inventory.orders";
    private static final Path OFFSETS = offsetsFile();
    private static final ObjectMapper JSON = new ObjectMapper();

    @Inject
    ShutdownHandler shutdownHandler;

    @Inject
    DebeziumConnectorRegistry debeziumConnectorRegistry;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(ShutdownHandler.class))
            .withConfigurationResource("debezium-quarkus-testsuite.properties")
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
            .overrideConfigKey("quarkus.debezium.offset.storage.file.filename", OFFSETS.toString())
            .overrideConfigKey("quarkus.debezium.offset.flush.interval.ms", "0");

    @Test
    @DisplayName("should stop the engine when the AFTER shutdown method returns true")
    void shouldStopTheEngine() {
        DebeziumCompletionEvent completion = awaitCompletion();

        assertThat(completion.isSuccess()).as("completion error: %s", completion.getError()).isTrue();
        assertThat(debeziumConnectorRegistry.get(new EngineManifest("default")).status())
                .isEqualTo(new DebeziumStatus(DebeziumStatus.State.STOPPED));
        assertThat(shutdownHandler.triggeringRecord().topic()).isEqualTo(ORDERS_TOPIC);
    }

    @Test
    @DisplayName("should deliver the batch that contains the triggering record")
    void shouldDeliverTheBatchOfTheTriggeringRecord() {
        awaitCompletion();

        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(shutdownHandler.destinations()).contains(ORDERS_TOPIC));
    }

    @Test
    @DisplayName("should commit the offset of the triggering record")
    void shouldCommitTheOffsetOfTheTriggeringRecord() {
        awaitCompletion();

        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    // the store is a java serialized map that embeds the offsets as JSON text
                    String stored = new String(Files.readAllBytes(OFFSETS), StandardCharsets.ISO_8859_1);

                    for (Map.Entry<String, ?> entry : shutdownHandler.triggeringRecord().sourceOffset().entrySet()) {
                        assertThat(stored).contains("\"" + entry.getKey() + "\":" + JSON.writeValueAsString(entry.getValue()));
                    }
                });
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

    private static Path offsetsFile() {
        Path file = Path.of("target", "debezium-shutdown", "offsets.dat");
        try {
            Files.createDirectories(file.getParent());
            Files.deleteIfExists(file);
            return file;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @ApplicationScoped
    static class ShutdownHandler {
        private final AtomicReference<DebeziumCompletionEvent> completion = new AtomicReference<>();
        private final List<String> destinations = new CopyOnWriteArrayList<>();
        private final AtomicBoolean triggered = new AtomicBoolean(false);
        private final AtomicReference<SourceRecord> triggeringRecord = new AtomicReference<>();

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

        @DebeziumShutdown(Phase.AFTER)
        public boolean stopOnFirstOrder(SourceRecord record) {
            if (ORDERS_TOPIC.equals(record.topic()) && triggered.compareAndSet(false, true)) {
                triggeringRecord.set(record);
                return true;
            }

            return false;
        }

        public List<String> destinations() {
            return destinations;
        }

        public SourceRecord triggeringRecord() {
            return Optional.ofNullable(triggeringRecord.get())
                    .orElseThrow(() -> new IllegalStateException("the shutdown has not been triggered yet"));
        }
    }
}
