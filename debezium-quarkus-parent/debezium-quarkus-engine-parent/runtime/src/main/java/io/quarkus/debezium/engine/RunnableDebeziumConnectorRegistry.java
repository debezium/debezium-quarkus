/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.Connector;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.EngineManifest;

public class RunnableDebeziumConnectorRegistry implements DebeziumConnectorRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunnableDebeziumConnectorRegistry.class);

    private final Connector connector;
    private final Map<String, Supplier<Debezium>> engineSuppliers;
    private final Map<String, Debezium> currentEngines = new ConcurrentHashMap<>();
    private final Map<String, DebeziumRunner> runners = new ConcurrentHashMap<>();

    public RunnableDebeziumConnectorRegistry(Connector connector, Map<String, Supplier<Debezium>> engineSuppliers) {
        this.connector = connector;
        this.engineSuppliers = engineSuppliers;
        engineSuppliers.forEach((id, supplier) -> currentEngines.put(id, supplier.get()));
    }

    @Override
    public Connector connector() {
        return connector;
    }

    @Override
    public Debezium get(EngineManifest manifest) {
        return currentEngines.get(manifest.id());
    }

    @Override
    public List<Debezium> engines() {
        return List.copyOf(currentEngines.values());
    }

    @Override
    public void start(EngineManifest manifest) {
        if (!engineSuppliers.containsKey(manifest.id())) {
            throw new IllegalDebeziumStateException("No engine found for manifest: " + manifest.id());
        }

        if (runners.containsKey(manifest.id())) {
            throw new IllegalDebeziumStateException("Engine already running for manifest: " + manifest.id());
        }

        Debezium debezium = engineSuppliers.get(manifest.id()).get();
        currentEngines.put(manifest.id(), debezium);

        DebeziumRunner runner = new DebeziumRunner(
                DebeziumThreadHandler.getThreadFactory(debezium), debezium);

        DebeziumRunner existing = runners.putIfAbsent(manifest.id(), runner);
        if (existing != null) {
            throw new IllegalDebeziumStateException("Engine already running for manifest: " + manifest.id());
        }

        try {
            runner.start();
        }
        catch (Exception e) {
            runners.remove(manifest.id());
            LOGGER.error("Failed to start engine for manifest: {}", manifest.id(), e);
            throw new IllegalDebeziumStateException(
                    "Failed to start engine for manifest: " + manifest.id(), e);
        }
    }

    @Override
    public void stop(EngineManifest manifest) {
        DebeziumRunner runner = runners.remove(manifest.id());
        if (runner == null) {
            throw new IllegalDebeziumStateException("No running engine found for manifest: " + manifest.id());
        }

        try {
            runner.shutdown();
        }
        catch (Exception e) {
            LOGGER.error("Failed to stop engine for manifest: {}", manifest.id(), e);
            throw new IllegalDebeziumStateException(
                    "Failed to stop engine for manifest: " + manifest.id(), e);
        }
    }
}
