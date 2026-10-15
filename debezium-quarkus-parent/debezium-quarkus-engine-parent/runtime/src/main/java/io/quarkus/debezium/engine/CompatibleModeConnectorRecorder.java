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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.common.BaseSourceConnector;
import io.debezium.runtime.Connector;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;
import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;
import io.quarkus.arc.Arc;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class CompatibleModeConnectorRecorder {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompatibleModeConnectorRecorder.class);

    /**
     * The connector of an empty registry, produced when the configured {@code connector.class} is
     * already served by another registry: it never matches a real connector.
     */
    private static final Connector NO_CONNECTOR = new Connector("");

    private final DebeziumConfigurationEngineParser engineParser = new DebeziumConfigurationEngineParser();
    private final RuntimeValue<DebeziumEngineRuntimeConfiguration> debeziumEngineConfigurationRuntimeValue;

    public CompatibleModeConnectorRecorder(RuntimeValue<DebeziumEngineRuntimeConfiguration> debeziumEngineConfigurationRuntimeValue) {
        this.debeziumEngineConfigurationRuntimeValue = debeziumEngineConfigurationRuntimeValue;
    }

    /**
     * Registry for a connector discovered among the application's dependencies at build time.
     */
    public Supplier<DebeziumConnectorRegistry> engine(Class<? extends BaseSourceConnector> connectorClass) {
        return () -> registry(new Connector(connectorClass.getName()), defaultConfiguration(connectorClass.getName()));
    }

    /**
     * Registry for the connector configured with {@code connector.class}, resolved at runtime, when the
     * configured class is neither provided by a Debezium extension nor discovered among the application's
     * dependencies at build time (the {@code knownConnectorClasses}), for example a connector jar added to
     * the {@code lib/} directory of a Debezium Server distribution. When the configured class is a known
     * one, or there is none (the extension infers it), another registry already serves the engine and this
     * registry is empty; the same holds for every engine of a multi-engine configuration. A
     * {@code connector.class} that cannot be loaded is a configuration error and fails the start-up
     * instead of leaving the engine silently unstarted.
     */
    public Supplier<DebeziumConnectorRegistry> configuredEngine(List<String> knownConnectorClasses) {
        return () -> configuredRegistry(knownConnectorClasses);
    }

    /**
     * Like {@link #configuredEngine(List)}, but when there is no {@code connector.class} configured the
     * connector is inferred from the single dependency found at build time: {@code inferredConnectorClass}.
     */
    public Supplier<DebeziumConnectorRegistry> inferredEngine(List<String> knownConnectorClasses, String inferredConnectorClass) {
        return () -> inferredRegistry(knownConnectorClasses, inferredConnectorClass);
    }

    private DebeziumConnectorRegistry configuredRegistry(List<String> knownConnectorClasses) {
        List<MultiEngineConfiguration> engines = engineParser.parse(debeziumEngineConfigurationRuntimeValue.getValue());

        // an engine without connector.class, or with one provided by an extension or a dependency, is served by another registry
        List<MultiEngineConfiguration> unserved = engines
                .stream()
                .filter(engine -> !isServed(engine.configuration().get(CONNECTOR_CLASS.name()), knownConnectorClasses))
                .toList();

        if (unserved.isEmpty()) {
            if (engines.size() == 1 && knownConnectorClasses.isEmpty()) {
                // nothing to infer the connector class from
                loadConnectorClass(engines.get(0).configuration().get(CONNECTOR_CLASS.name()), engines.get(0).engineId());
            }
            return emptyRegistry();
        }

        MultiEngineConfiguration engine = unserved.get(0);
        String connectorClass = engine.configuration().get(CONNECTOR_CLASS.name());

        if (engines.size() > 1 || !engine.engineId().equals(QuarkusDatasourceConfiguration.DEFAULT)) {
            throw new DebeziumException("Connector class '" + connectorClass + "' configured for engine '" + engine.engineId()
                    + "' is not provided by a Debezium extension nor found among the application's dependencies; "
                    + "such a connector is only supported by the default engine in Compatibility mode, which does not support the Multi Engine feature");
        }

        loadConnectorClass(connectorClass, engine.engineId());

        LOGGER.info("Connector class '{}' is not provided by a Debezium extension nor found among the application's dependencies; " +
                "loading it from the configuration", connectorClass);

        return registry(new Connector(connectorClass), new LinkedHashMap<>(engine.configuration()));
    }

    private DebeziumConnectorRegistry inferredRegistry(List<String> knownConnectorClasses, String inferredConnectorClass) {
        Map<String, String> configuration = defaultConfiguration(inferredConnectorClass);
        String connectorClass = configuration.get(CONNECTOR_CLASS.name());

        if (connectorClass == null || connectorClass.isBlank()) {
            // to reduce friction, if there is only one dependency it's possible to infer the connector class
            configuration.put(CONNECTOR_CLASS.name(), inferredConnectorClass);
            return registry(new Connector(inferredConnectorClass), configuration);
        }

        if (knownConnectorClasses.contains(connectorClass)) {
            return emptyRegistry();
        }

        loadConnectorClass(connectorClass, EngineManifest.DEFAULT.id());

        if (!connectorClass.equals(inferredConnectorClass)) {
            LOGGER.info("Connector class '{}' is not provided by a Debezium extension nor found among the application's dependencies; " +
                    "loading it from the configuration", connectorClass);
        }

        return registry(new Connector(connectorClass), configuration);
    }

    private static boolean isServed(String connectorClass, List<String> knownConnectorClasses) {
        return connectorClass == null || connectorClass.isBlank() || knownConnectorClasses.contains(connectorClass);
    }

    /**
     * A copy of the default engine configuration, so that a registry can adjust it without affecting
     * the other registries that read the same configuration.
     */
    private Map<String, String> defaultConfiguration(String connector) {
        List<MultiEngineConfiguration> multiEngineConfigurations = engineParser.parse(debeziumEngineConfigurationRuntimeValue.getValue());

        if (multiEngineConfigurations.size() > 1) {
            throw new RuntimeException("Compatibility mode error: Multi Engine feature is not supported in Compatibility mode");
        }

        Optional<MultiEngineConfiguration> multiEngineConfiguration = multiEngineConfigurations
                .stream()
                .filter(item -> item.engineId().equals(QuarkusDatasourceConfiguration.DEFAULT))
                .findFirst();

        if (multiEngineConfiguration.isEmpty()) {
            throw new RuntimeException(
                    "Compatibility mode Error: impossible to get configuration for connector " + connector
                            + " . Please use default configuration in Compatibility mode");
        }

        return new LinkedHashMap<>(multiEngineConfiguration.get().configuration());
    }

    private DebeziumConnectorRegistry registry(Connector connector, Map<String, String> configuration) {
        /*
         * This is a workaround, we should avoid to get statically beans but `RuntimeValue<>` doesn't work: Quarkus detect this fragment as deployment time
         */
        DebeziumFactory debeziumFactory = Arc.container().instance(DebeziumFactory.class).get();

        Map<String, Debezium> engines = Map.of(EngineManifest.DEFAULT.id(),
                debeziumFactory.get(connector,
                        new MultiEngineConfiguration(EngineManifest.DEFAULT.id(), configuration)));

        Map<String, DebeziumRunner> runners = new ConcurrentHashMap<>();

        return new DebeziumConnectorRegistry() {
            @Override
            public Connector connector() {
                return connector;
            }

            @Override
            public Debezium get(EngineManifest manifest) {
                return engines.get(manifest.id());
            }

            @Override
            public List<EngineManifest> manifests() {
                return List.of(EngineManifest.DEFAULT);
            }

            @Override
            public List<Debezium> runningEngines() {
                return engines.entrySet().stream()
                        .filter(e -> runners.containsKey(e.getKey()))
                        .map(Map.Entry::getValue)
                        .toList();
            }

            @Override
            public List<Debezium> engines() {
                return engines
                        .values()
                        .stream()
                        .toList();
            }

            @Override
            public void start(EngineManifest manifest) {
                Debezium debezium = engines.get(manifest.id());
                if (debezium == null) {
                    throw new DebeziumException("No engine found for manifest: " + manifest.id());
                }
                DebeziumRunner runner = new DebeziumRunner(DebeziumThreadHandler.getThreadFactory(debezium), debezium);
                if (runners.putIfAbsent(manifest.id(), runner) != null) {
                    throw new DebeziumException("Engine already running for manifest: " + manifest.id());
                }
                try {
                    runner.start();
                }
                catch (RuntimeException e) {
                    runners.remove(manifest.id());
                    throw e;
                }
            }

            @Override
            public void stop(EngineManifest manifest) {
                DebeziumRunner runner = runners.remove(manifest.id());
                if (runner == null) {
                    throw new DebeziumException("No running engine found for manifest: " + manifest.id());
                }
                runner.shutdown();
            }
        };
    }

    private static DebeziumConnectorRegistry emptyRegistry() {
        return new DebeziumConnectorRegistry() {
            @Override
            public Connector connector() {
                return NO_CONNECTOR;
            }

            @Override
            public Debezium get(EngineManifest manifest) {
                return null;
            }

            @Override
            public List<EngineManifest> manifests() {
                return List.of();
            }

            @Override
            public List<Debezium> runningEngines() {
                return List.of();
            }

            @Override
            public List<Debezium> engines() {
                return List.of();
            }

            @Override
            public void start(EngineManifest manifest) {
                throw new DebeziumException("No engine found for manifest: " + manifest.id());
            }

            @Override
            public void stop(EngineManifest manifest) {
                throw new DebeziumException("No running engine found for manifest: " + manifest.id());
            }
        };
    }

    private static void loadConnectorClass(String connectorClass, String engineId) {
        if (connectorClass == null || connectorClass.isBlank()) {
            LOGGER.error("No 'connector.class' configured for engine '{}'; set 'quarkus.debezium.connector.class' " +
                    "or add a debezium-connector-* dependency", engineId);
            throw new DebeziumException("No 'connector.class' configured for engine '" + engineId
                    + "'; set 'quarkus.debezium.connector.class' or add a debezium-connector-* dependency");
        }

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = CompatibleModeConnectorRecorder.class.getClassLoader();
        }

        Class<?> clazz;
        try {
            clazz = Class.forName(connectorClass, false, classLoader);
        }
        catch (ClassNotFoundException e) {
            LOGGER.error("Connector class '{}' configured for engine '{}' was not found on the classpath; " +
                    "verify the class name and that the connector jar is on the classpath", connectorClass, engineId);
            throw new DebeziumException("Connector class '" + connectorClass + "' configured for engine '" + engineId
                    + "' was not found on the classpath", e);
        }
        catch (LinkageError e) {
            LOGGER.error("Connector class '{}' configured for engine '{}' could not be linked; " +
                    "a required dependency may be missing or incompatible", connectorClass, engineId);
            throw new DebeziumException("Connector class '" + connectorClass + "' configured for engine '" + engineId
                    + "' could not be linked: " + e.getMessage(), e);
        }

        if (!SourceConnector.class.isAssignableFrom(clazz)) {
            LOGGER.error("Connector class '{}' configured for engine '{}' does not implement {}; " +
                    "only Kafka Connect source connectors are supported", connectorClass, engineId, SourceConnector.class.getName());
            throw new DebeziumException("Connector class '" + connectorClass + "' configured for engine '" + engineId
                    + "' does not implement " + SourceConnector.class.getName());
        }
    }
}
