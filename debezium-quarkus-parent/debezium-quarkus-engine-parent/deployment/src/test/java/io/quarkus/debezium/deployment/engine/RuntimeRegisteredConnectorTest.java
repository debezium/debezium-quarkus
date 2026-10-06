/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.deployment.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.Connector;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumConnectorsRegistry;
import io.debezium.runtime.EngineManifest;
import io.quarkus.test.QuarkusUnitTest;

/**
 * A {@code connector.class} that is neither provided by a Debezium extension nor discovered among
 * the application's dependencies at build time (for example a connector jar dropped into the
 * {@code lib/} directory of a Debezium Server distribution) must still get a registry at runtime.
 */
public class RuntimeRegisteredConnectorTest {

    @Inject
    DebeziumConnectorsRegistry registry;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(ExternalConnector.class, ExternalTask.class))
            .overrideConfigKey("quarkus.debezium.engine.autostart", "false")
            .overrideConfigKey("quarkus.debezium.connector.class", ExternalConnector.class.getName())
            .overrideConfigKey("quarkus.debezium.name", "external")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "external")
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");

    @Test
    @DisplayName("should register a connector that is only known at runtime")
    void shouldRegisterConnectorKnownOnlyAtRuntime() {
        Connector connector = new Connector(ExternalConnector.class.getName());

        assertThat(registry.registry(connector)).isPresent();

        DebeziumConnectorRegistry connectorRegistry = registry.registry(connector).get();
        assertThat(connectorRegistry.manifests()).containsExactly(EngineManifest.DEFAULT);
        assertThat(connectorRegistry.get(EngineManifest.DEFAULT).connector()).isEqualTo(connector);
        assertThat(connectorRegistry.get(EngineManifest.DEFAULT).manifest()).isEqualTo(EngineManifest.DEFAULT);
        assertThat(registry.engines()).contains(connectorRegistry.get(EngineManifest.DEFAULT));
        assertThat(registry.runningEngines()).isEmpty();
    }

    public static class ExternalConnector extends SourceConnector {
        @Override
        public String version() {
            return "test";
        }

        @Override
        public void start(Map<String, String> props) {
        }

        @Override
        public Class<? extends Task> taskClass() {
            return ExternalTask.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            return List.of();
        }

        @Override
        public void stop() {
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }
    }

    public static class ExternalTask extends SourceTask {
        @Override
        public String version() {
            return "test";
        }

        @Override
        public void start(Map<String, String> props) {
        }

        @Override
        public List<SourceRecord> poll() {
            return List.of();
        }

        @Override
        public void stop() {
        }
    }
}
