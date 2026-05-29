/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.testsuite.deployment.suite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.CapturingFieldsFilterStrategy;
import io.quarkus.debezium.engine.deserializer.CapturingEventDeserializerRegistry;
import io.quarkus.debezium.engine.deserializer.MutableCapturingEventDeserializerRegistry;
import io.quarkus.debezium.engine.deserializer.ObjectMapperDeserializer;
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.debezium.testsuite.deployment.TestSuiteConfigurations;
import io.quarkus.test.QuarkusUnitTest;

/**
 * Tests the fallback chain: filter registry -> object invoker -> destination registry -> wildcard.
 * <p>
 * The filter watches the 'key' field (only exists on the orders table).
 * <ul>
 *   <li>Filter captures orders events (has 'key' field)</li>
 *   <li>Object invoker with deserializer captures users events (filter rejected, falls to object invoker)</li>
 *   <li>Destination invoker captures products events</li>
 *   <li>Wildcard invoker catches general_table and heartbeats</li>
 * </ul>
 */
@Tag(SuiteTags.DEFAULT)
public class CapturingFieldsFilterTest {

    @Inject
    FallbackHandler handler;

    @Inject
    CapturingEventDeserializerRegistry<SourceRecord, SourceRecord> registry;

    @BeforeEach
    void setUp() {
        var mutableRegistry = (MutableCapturingEventDeserializerRegistry<SourceRecord, SourceRecord>) registry;
        mutableRegistry.register("topic.inventory.users", new UserDeserializer());
    }

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(
                    FallbackHandler.class,
                    KeyFieldFilter.class))
            .withConfigurationResource("debezium-quarkus-testsuite.properties");

    @Test
    @DisplayName("should capture orders events via filter (only table with 'key' field)")
    void shouldCaptureOrdersViaFilter() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(handler.filterCount()).isGreaterThanOrEqualTo(2));
    }

    @Test
    @DisplayName("should fallback to deserialized object invoker for users events not matched by filter")
    void shouldFallbackToDeserializedObjectInvoker() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(handler.getUsers()).containsExactlyInAnyOrder(
                        new User(1, "giovanni", "developer"),
                        new User(2, "mario", "developer")));
    }

    @Test
    @DisplayName("should fallback to destination invoker for products events")
    void shouldFallbackToDestinationInvoker() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(handler.destinationCount()).isGreaterThanOrEqualTo(2));
    }

    @Test
    @DisplayName("should fallback to wildcard invoker for events not matched by filter, object or destination")
    void shouldFallbackToWildcardInvoker() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(handler.wildcardCount()).isGreaterThan(0));
    }

    @Test
    @DisplayName("filter invoker should not capture general_table events")
    void shouldNotCaptureGeneralTableViaFilter() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(handler.filterCount()).isGreaterThan(0);
                    assertThat(handler.filterGeneralTableCount()).isEqualTo(0);
                });
    }

    @ApplicationScoped
    static class FallbackHandler {
        private final AtomicInteger filterCount = new AtomicInteger(0);
        private final AtomicInteger filterGeneralTableCount = new AtomicInteger(0);
        private final AtomicInteger destinationCount = new AtomicInteger(0);
        private final AtomicInteger wildcardCount = new AtomicInteger(0);
        private final List<User> users = new ArrayList<>();

        @Capturing(filter = KeyFieldFilter.class)
        public void captureViaFilter(CapturingEvent<SourceRecord, SourceRecord> event) {
            filterCount.incrementAndGet();
            if (event.destination().contains("general_table")) {
                filterGeneralTableCount.incrementAndGet();
            }
        }

        @Capturing(destination = "topic.inventory.users")
        public void captureDeserializedUsers(User user) {
            users.add(user);
        }

        @Capturing(destination = "topic.inventory.products")
        public void captureProducts(CapturingEvent<SourceRecord, SourceRecord> event) {
            destinationCount.incrementAndGet();
        }

        @Capturing()
        public void captureWildcard(CapturingEvent<SourceRecord, SourceRecord> event) {
            wildcardCount.incrementAndGet();
        }

        public int filterCount() {
            return filterCount.get();
        }

        public int filterGeneralTableCount() {
            return filterGeneralTableCount.get();
        }

        public int destinationCount() {
            return destinationCount.get();
        }

        public int wildcardCount() {
            return wildcardCount.get();
        }

        public List<User> getUsers() {
            return users;
        }
    }

    @ApplicationScoped
    public static class KeyFieldFilter extends CapturingFieldsFilterStrategy {
        public KeyFieldFilter() {
            super(Set.of("key"));
        }
    }

    public record User(int id, String name, String description) {
    }

    private static final ObjectMapper configuredMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

    public static class UserDeserializer extends ObjectMapperDeserializer<User> {
        public UserDeserializer() {
            super(User.class, configuredMapper);
        }
    }
}
