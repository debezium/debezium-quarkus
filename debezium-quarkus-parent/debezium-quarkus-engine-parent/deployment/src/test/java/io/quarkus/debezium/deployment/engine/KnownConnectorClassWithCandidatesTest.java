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
import io.debezium.runtime.DebeziumConnectorsRegistry;
import io.quarkus.maven.dependency.Dependency;
import io.quarkus.test.QuarkusUnitTest;

/**
 * With several {@code debezium-connector-*} dependencies, a {@code connector.class} that one of them
 * provides is served by that dependency's registry only: the registry resolving the configuration
 * at runtime stays empty.
 */
public class KnownConnectorClassWithCandidatesTest {

    @Inject
    DebeziumConnectorsRegistry registry;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .setForcedDependencies(List.of(Dependency.of("io.debezium", "debezium-connector-vitess", Module.version())))
            .overrideConfigKey("quarkus.debezium.engine.autostart", "false")
            .overrideConfigKey("quarkus.debezium.connector.class", InformixConnector.class.getName())
            .overrideConfigKey("quarkus.debezium.name", "known")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "known")
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");

    @Test
    @DisplayName("should not register a connector known at build time a second time")
    void shouldNotRegisterKnownConnectorTwice() {
        Connector connector = new Connector(InformixConnector.class.getName());

        assertThat(registry.registries().stream().filter(item -> item.connector().equals(connector))).hasSize(1);
        assertThat(registry.engines()).filteredOn(engine -> engine.connector().equals(connector)).hasSize(1);
        assertThat(registry.runningEngines()).isEmpty();
    }
}
