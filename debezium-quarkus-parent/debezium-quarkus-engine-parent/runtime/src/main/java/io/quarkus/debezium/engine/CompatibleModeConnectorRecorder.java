/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import static io.debezium.embedded.EmbeddedEngineConfig.CONNECTOR_CLASS;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import io.debezium.connector.common.BaseSourceConnector;
import io.debezium.runtime.Connector;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;
import io.quarkus.arc.Arc;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class CompatibleModeConnectorRecorder {
    private final DebeziumConfigurationEngineParser engineParser = new DebeziumConfigurationEngineParser();

    public Supplier<DebeziumConnectorRegistry> engine(DebeziumEngineConfiguration debeziumEngineConfiguration,
                                                      Class<? extends BaseSourceConnector> connectorClass) {
        return () -> {
            /*
             * This is a workaround, we should avoid to get statically beans but `RuntimeValue<>` doesn't work: Quarkus detect this fragment as deployment time
             */
            DebeziumFactory debeziumFactory = Arc.container().instance(DebeziumFactory.class).get();
            List<MultiEngineConfiguration> multiEngineConfigurations = engineParser.parse(debeziumEngineConfiguration);

            if (multiEngineConfigurations.size() > 1) {
                throw new RuntimeException("Compatibility mode error: Multi Engine feature is not supported in Compatibility mode");
            }

            Optional<MultiEngineConfiguration> multiEngineConfiguration = multiEngineConfigurations
                    .stream()
                    .filter(item -> item.engineId().equals(QuarkusDatasourceConfiguration.DEFAULT))
                    .findFirst();

            if (multiEngineConfiguration.isEmpty()) {
                throw new RuntimeException(
                        "Compatibility mode Error: impossible to get configuration for connector " + connectorClass.getName()
                                + " . Please use default configuration in Compatibility mode");
            }

            Map<String, String> configuration = multiEngineConfiguration.get().configuration();
            configuration.put(CONNECTOR_CLASS.name(), connectorClass.getName());

            Map<String, Debezium> engines = Map.of(EngineManifest.DEFAULT.id(),
                    debeziumFactory.get(new Connector(connectorClass.getName()),
                            new MultiEngineConfiguration(EngineManifest.DEFAULT.id(), configuration)));

            Map<String, DebeziumRunner> runners = new ConcurrentHashMap<>();

            return new DebeziumConnectorRegistry() {
                @Override
                public Connector connector() {
                    return new Connector(connectorClass.getName());
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
                public List<Debezium> engines() {
                    return engines.entrySet().stream()
                            .filter(e -> runners.containsKey(e.getKey()))
                            .map(Map.Entry::getValue)
                            .toList();
                }

                @Override
                public void start(EngineManifest manifest) {
                    Debezium debezium = engines.get(manifest.id());
                    if (debezium == null) {
                        throw new IllegalDebeziumStateException("No engine found for manifest: " + manifest.id());
                    }
                    DebeziumRunner runner = new DebeziumRunner(DebeziumThreadHandler.getThreadFactory(debezium), debezium);
                    if (runners.putIfAbsent(manifest.id(), runner) != null) {
                        throw new IllegalDebeziumStateException("Engine already running for manifest: " + manifest.id());
                    }
                    runner.start();
                }

                @Override
                public void stop(EngineManifest manifest) {
                    DebeziumRunner runner = runners.remove(manifest.id());
                    if (runner == null) {
                        throw new IllegalDebeziumStateException("No running engine found for manifest: " + manifest.id());
                    }
                    runner.shutdown();
                }
            };
        };
    }
}
