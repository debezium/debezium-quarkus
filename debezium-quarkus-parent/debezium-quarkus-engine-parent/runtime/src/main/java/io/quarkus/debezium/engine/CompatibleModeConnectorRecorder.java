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
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class CompatibleModeConnectorRecorder {
    private final DebeziumConfigurationEngineParser engineParser = new DebeziumConfigurationEngineParser();
    private final RuntimeValue<DebeziumEngineConfiguration> debeziumEngineConfiguration;

    public CompatibleModeConnectorRecorder(RuntimeValue<DebeziumEngineConfiguration> debeziumEngineConfiguration) {
        this.debeziumEngineConfiguration = debeziumEngineConfiguration;
    }

    public Supplier<DebeziumConnectorRegistry> engine(Class<? extends BaseSourceConnector> connectorClass) {
        return () -> {
            DebeziumFactory debeziumFactory = Arc.container().instance(DebeziumFactory.class).get();
            List<MultiEngineConfiguration> multiEngineConfigurations = engineParser.parse(debeziumEngineConfiguration.getValue());

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
                public List<Debezium> engines() {
                    return engines.values().stream().toList();
                }
            };
        };
    }
}
