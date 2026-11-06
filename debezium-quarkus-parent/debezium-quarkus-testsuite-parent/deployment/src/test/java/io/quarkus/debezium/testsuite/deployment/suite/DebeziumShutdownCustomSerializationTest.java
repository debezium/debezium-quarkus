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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.engine.format.Json;
import io.debezium.engine.format.SerializationFormat;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvents;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumSerialization;
import io.debezium.runtime.DebeziumShutdown;
import io.debezium.runtime.DebeziumShutdown.Phase;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.quarkus.arc.Unremovable;
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.debezium.testsuite.deployment.TestSuiteConfigurations;
import io.quarkus.test.QuarkusUnitTest;

/**
 * With a custom serialization, as used by Debezium Server, the engine records are converted to JSON. The AFTER shutdown
 * method must still receive the {@link SourceRecord}, stop the engine, let the batch of the triggering record reach
 * the consumers, and commit the offset of the triggering record. The offset check fails when the stop does not complete,
 * because a failed engine does not commit the offsets of its last batch.
 */
@Tag(SuiteTags.DEFAULT)
public class DebeziumShutdownCustomSerializationTest {

    private static final String ORDERS_TOPIC = "topic.inventory.orders";
    private static final Path OFFSETS = offsetsFile();
    private static final ObjectMapper JSON = new ObjectMapper();

    @Inject
    ShutdownHandler shutdownHandler;

    @Inject
    DebeziumConnectorRegistry debeziumConnectorRegistry;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(ShutdownHandler.class, JsonSerializationProducer.class))
            .withConfigurationResource("debezium-quarkus-testsuite.properties")
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
            .overrideConfigKey("quarkus.debezium.offset.storage.file.filename", OFFSETS.toString())
            .overrideConfigKey("quarkus.debezium.offset.flush.interval.ms", "0");

    @Test
    @DisplayName("should stop the engine with a custom serialization when the AFTER shutdown method returns true")
    void shouldStopTheEngine() {
        DebeziumCompletionEvent completion = awaitCompletion();

        assertThat(completion.isSuccess()).as("completion error: %s", completion.getError()).isTrue();
        assertThat(debeziumConnectorRegistry.get(new EngineManifest("default")).status())
                .isEqualTo(new DebeziumStatus(DebeziumStatus.State.STOPPED));

        assertThat(shutdownHandler.triggeringRecord()).isNotNull();
        assertThat(shutdownHandler.triggeringRecord().topic()).isEqualTo(ORDERS_TOPIC);
    }

    @Test
    @DisplayName("should deliver the serialized batch that contains the triggering record")
    void shouldDeliverTheBatchOfTheTriggeringRecord() {
        awaitCompletion();

        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(shutdownHandler.destinations()).contains(ORDERS_TOPIC));

        assertThat(shutdownHandler.values()).allMatch(String.class::isInstance);
    }

    @Test
    @DisplayName("should commit the offset of the triggering record with a custom serialization")
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
        Path file = Path.of("target", "debezium-shutdown-serialization", "offsets.dat");
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
    static class JsonSerializationProducer {

        @Produces
        @Unremovable
        @ApplicationScoped
        DebeziumSerialization<String, String, String> serialization() {
            return new DebeziumSerialization<>() {
                @Override
                public Class<? extends SerializationFormat<String>> getKeyFormat() {
                    return Json.class;
                }

                @Override
                public Class<? extends SerializationFormat<String>> getValueFormat() {
                    return Json.class;
                }

                @Override
                public Class<? extends SerializationFormat<String>> getHeaderFormat() {
                    return Json.class;
                }

                @Override
                public String getEngineId() {
                    return "default";
                }
            };
        }
    }

    @ApplicationScoped
    static class ShutdownHandler {
        private final AtomicReference<DebeziumCompletionEvent> completion = new AtomicReference<>();
        private final List<String> destinations = new CopyOnWriteArrayList<>();
        private final List<Object> values = new CopyOnWriteArrayList<>();
        private final AtomicBoolean triggered = new AtomicBoolean(false);
        private final AtomicReference<SourceRecord> triggeringRecord = new AtomicReference<>();

        @Capturing()
        public void capture(CapturingEvents<BatchEvent> events) {
            events.records().forEach(event -> {
                destinations.add(event.destination());
                values.add(event.value());
            });
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

        public List<Object> values() {
            return values;
        }

        public SourceRecord triggeringRecord() {
            return triggeringRecord.get();
        }
    }
}
