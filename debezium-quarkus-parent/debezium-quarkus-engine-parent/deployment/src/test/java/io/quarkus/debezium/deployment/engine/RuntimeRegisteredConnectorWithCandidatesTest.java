/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.deployment.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import jakarta.inject.Inject;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.Module;
import io.debezium.connector.informix.InformixConnector;
import io.debezium.runtime.Connector;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumConnectorsRegistry;
import io.debezium.runtime.EngineManifest;
import io.quarkus.debezium.deployment.engine.RuntimeRegisteredConnectorTest.ExternalConnector;
import io.quarkus.debezium.deployment.engine.RuntimeRegisteredConnectorTest.ExternalTask;
import io.quarkus.maven.dependency.Dependency;
import io.quarkus.test.QuarkusUnitTest;

/**
 * With several {@code debezium-connector-*} dependencies no connector class is inferred: a
 * {@code connector.class} that none of them provides is resolved at runtime by the dedicated
 * registry, next to the registries of the dependencies.
 */
public class RuntimeRegisteredConnectorWithCandidatesTest {

    @Inject
    DebeziumConnectorsRegistry registry;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .setForcedDependencies(List.of(Dependency.of("io.debezium", "debezium-connector-vitess", Module.version())))
            .withApplicationRoot((jar) -> jar.addClasses(ExternalConnector.class, ExternalTask.class))
            .overrideConfigKey("quarkus.debezium.engine.autostart", "false")
            .overrideConfigKey("quarkus.debezium.connector.class", ExternalConnector.class.getName())
            .overrideConfigKey("quarkus.debezium.name", "external")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "external")
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");

    @Test
    @DisplayName("should register a connector that is only known at runtime next to the connectors known at build time")
    void shouldRegisterConnectorKnownOnlyAtRuntime() {
        Connector connector = new Connector(ExternalConnector.class.getName());

        assertThat(registry.registries())
                .map(DebeziumConnectorRegistry::connector)
                .contains(connector,
                        new Connector(InformixConnector.class.getName()),
                        new Connector("io.debezium.connector.vitess.VitessConnector"));

        DebeziumConnectorRegistry connectorRegistry = registry.registry(connector).orElseThrow();
        assertThat(connectorRegistry.manifests()).containsExactly(EngineManifest.DEFAULT);
        assertThat(connectorRegistry.get(EngineManifest.DEFAULT).connector()).isEqualTo(connector);
        assertThat(registry.runningEngines()).isEmpty();
    }
}
