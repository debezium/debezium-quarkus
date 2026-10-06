/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import static io.debezium.embedded.EmbeddedEngineConfig.CONNECTOR_CLASS;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.runtime.Connector;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumConnectorsRegistry;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;

public class DebeziumConnectorsRegistryProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumConnectorsRegistryProducer.class);

    private final Instance<DebeziumConnectorRegistry> registryInstances;
    private final DebeziumFactory debeziumFactory;
    private final DebeziumEngineRuntimeConfiguration configuration;
    private final DebeziumConfigurationEngineParser engineParser = new DebeziumConfigurationEngineParser();

    @Inject
    public DebeziumConnectorsRegistryProducer(Instance<DebeziumConnectorRegistry> registryInstances,
                                              DebeziumFactory debeziumFactory,
                                              DebeziumEngineRuntimeConfiguration configuration) {
        this.registryInstances = registryInstances;
        this.debeziumFactory = debeziumFactory;
        this.configuration = configuration;
    }

    @Produces
    @Singleton
    public DebeziumConnectorsRegistry produce() {
        // Read the configuration as the user wrote it before the build-time registries are resolved:
        // compatibility mode may rewrite `connector.class` in the shared configuration while doing so.
        List<MultiEngineConfiguration> configuredEngines = configuredEngines();

        List<DebeziumConnectorRegistry> registered = registryInstances
                .stream()
                .toList();

        List<DebeziumConnectorRegistry> registries = Stream
                .concat(registered.stream(), runtimeRegistries(configuredEngines, registered).stream())
                .toList();

        return new DebeziumConnectorsRegistry() {
            @Override
            public Optional<DebeziumConnectorRegistry> registry(Connector connector) {
                return registries
                        .stream()
                        .filter(registry -> registry.connector().equals(connector))
                        .findFirst();
            }

            @Override
            public List<DebeziumConnectorRegistry> registries() {
                return registries;
            }

            @Override
            public Optional<Debezium> get(EngineManifest manifest) {
                return registries
                        .stream()
                        .filter(registry -> registry.manifests().contains(manifest))
                        .map(registry -> registry.get(manifest))
                        .findFirst();
            }

            @Override
            public List<Debezium> runningEngines() {
                return registries
                        .stream()
                        .flatMap(registry -> registry.runningEngines().stream())
                        .toList();
            }

            @Override
            public List<Debezium> engines() {
                return registries
                        .stream()
                        .flatMap(registry -> registry.engines().stream())
                        .toList();
            }

            @Override
            public void start(EngineManifest manifest) {
                registries
                        .stream()
                        .filter(registry -> registry.manifests().contains(manifest))
                        .forEach(registry -> registry.start(manifest));
            }

            @Override
            public void stop(EngineManifest manifest) {
                registries
                        .stream()
                        .filter(registry -> registry.manifests().contains(manifest))
                        .forEach(registry -> registry.stop(manifest));
            }
        };
    }

    /**
     * Registries for the configured engines whose {@code connector.class} was not registered by any
     * Debezium extension nor discovered among the application's dependencies at build time.
     * <p>
     * This keeps a connector that is only present at runtime working, for example a connector jar
     * added to the {@code lib/} directory of a Debezium Server distribution, which the embedded
     * engine has always loaded by name. A {@code connector.class} that cannot be loaded is a
     * configuration error and fails the start-up instead of leaving the engine silently unstarted.
     */
    private List<MultiEngineConfiguration> configuredEngines() {
        return engineParser
                .parse(configuration)
                .stream()
                .map(engine -> new MultiEngineConfiguration(engine.engineId(), new LinkedHashMap<>(engine.configuration())))
                .toList();
    }

    private List<DebeziumConnectorRegistry> runtimeRegistries(List<MultiEngineConfiguration> configuredEngines,
                                                              List<DebeziumConnectorRegistry> registered) {
        Map<String, Map<String, Debezium>> enginesByConnector = new LinkedHashMap<>();

        for (MultiEngineConfiguration engine : configuredEngines) {
            String connectorClass = engine.configuration().get(CONNECTOR_CLASS.name());
            if (connectorClass == null || connectorClass.isBlank()) {
                // compatibility mode infers the connector class when there is a single candidate
                continue;
            }

            Connector connector = new Connector(connectorClass);
            if (registered.stream().anyMatch(registry -> registry.connector().equals(connector))) {
                continue;
            }

            loadConnectorClass(connectorClass, engine.engineId());

            LOGGER.warn("Connector class '{}' configured for engine '{}' is not provided by a Debezium extension; " +
                    "registering it at runtime in compatibility mode", connectorClass, engine.engineId());

            enginesByConnector
                    .computeIfAbsent(connectorClass, key -> new LinkedHashMap<>())
                    .put(engine.engineId(), debeziumFactory.get(connector, engine));
        }

        return enginesByConnector
                .entrySet()
                .stream()
                .map(entry -> (DebeziumConnectorRegistry) new CompatibleModeConnectorRegistry(new Connector(entry.getKey()), entry.getValue()))
                .toList();
    }

    private static void loadConnectorClass(String connectorClass, String engineId) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = DebeziumConnectorsRegistryProducer.class.getClassLoader();
        }

        Class<?> clazz;
        try {
            clazz = Class.forName(connectorClass, false, classLoader);
        }
        catch (ClassNotFoundException | LinkageError e) {
            LOGGER.error("Connector class '{}' configured for engine '{}' cannot be loaded; the engine will not start",
                    connectorClass, engineId);
            throw new DebeziumException("Unable to load connector class '" + connectorClass + "' configured for engine '" + engineId + "'", e);
        }

        if (!SourceConnector.class.isAssignableFrom(clazz)) {
            LOGGER.error("Connector class '{}' configured for engine '{}' is not a {}; the engine will not start",
                    connectorClass, engineId, SourceConnector.class.getName());
            throw new DebeziumException("Connector class '" + connectorClass + "' configured for engine '" + engineId
                    + "' does not implement " + SourceConnector.class.getName());
        }
    }
}
