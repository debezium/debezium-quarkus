/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.deployment.engine;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.DebeziumShutdown;
import io.debezium.runtime.DebeziumShutdown.Phase;
import io.quarkus.debezium.engine.shutdown.ShutdownInvoker;
import io.quarkus.test.QuarkusUnitTest;

/**
 * Each {@link DebeziumShutdown} method is turned into a {@link ShutdownInvoker} that delegates to the bean method.
 */
public class DebeziumShutdownDiscoveryTest {

    @Inject
    Instance<ShutdownInvoker> invokers;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(ShutdownHandler.class))
            .overrideConfigKey("quarkus.debezium.engine.autostart", "false")
            .overrideConfigKey("quarkus.debezium.name", "shutdown")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "shutdown")
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");

    @Test
    @DisplayName("should discover one invoker per phase and delegate to the annotated method")
    void shouldDiscoverInvokersAndDelegate() {
        assertThat(invokers.stream().map(ShutdownInvoker::phase)).containsExactlyInAnyOrder(Phase.BEFORE, Phase.AFTER);

        ShutdownInvoker before = invokers.stream().filter(invoker -> invoker.phase() == Phase.BEFORE).findFirst().orElseThrow();
        ShutdownInvoker after = invokers.stream().filter(invoker -> invoker.phase() == Phase.AFTER).findFirst().orElseThrow();

        assertThat(before.shouldShutdown(record("topic.stop"))).isTrue();
        assertThat(before.shouldShutdown(record("topic.keep"))).isFalse();
        assertThat(after.shouldShutdown(record("topic.keep"))).isFalse();
        assertThat(after.shouldShutdown(record("topic.stop"))).isTrue();
    }

    private static SourceRecord record(String topic) {
        return new SourceRecord(null, null, topic, null, "value");
    }

    @ApplicationScoped
    static class ShutdownHandler {

        @DebeziumShutdown(Phase.BEFORE)
        public boolean stopBefore(SourceRecord record) {
            return "topic.stop".equals(record.topic());
        }

        @DebeziumShutdown(Phase.AFTER)
        public boolean stopAfter(SourceRecord record) {
            return "topic.stop".equals(record.topic());
        }
    }
}
