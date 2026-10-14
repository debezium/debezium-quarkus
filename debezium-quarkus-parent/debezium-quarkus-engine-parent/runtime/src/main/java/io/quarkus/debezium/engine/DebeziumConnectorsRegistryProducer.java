/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.List;
import java.util.Optional;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.debezium.runtime.Connector;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumConnectorsRegistry;
import io.debezium.runtime.EngineManifest;

public class DebeziumConnectorsRegistryProducer {
    private final Instance<DebeziumConnectorRegistry> registryInstances;

    @Inject
    public DebeziumConnectorsRegistryProducer(Instance<DebeziumConnectorRegistry> registryInstances) {
        this.registryInstances = registryInstances;
    }

    @Produces
    @Singleton
    public DebeziumConnectorsRegistry produce() {
        return new DebeziumConnectorsRegistry() {
            private final List<DebeziumConnectorRegistry> registries = registryInstances
                    .stream()
                    .toList();

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
}
