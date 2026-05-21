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

import io.debezium.DebeziumException;
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
    public List<EngineManifest> manifests() {
        return engineSuppliers.keySet().stream()
                .map(EngineManifest::new)
                .toList();
    }

    @Override
    public List<Debezium> engines() {
        return currentEngines.entrySet().stream()
                .filter(e -> runners.containsKey(e.getKey()))
                .map(Map.Entry::getValue)
                .toList();
    }

    @Override
    public void start(EngineManifest manifest) {
        LOGGER.info("start({}) called; known engines={}, currently running={}",
                manifest.id(), engineSuppliers.keySet(), runners.keySet());

        if (!engineSuppliers.containsKey(manifest.id())) {
            LOGGER.warn("start({}) rejected: no engine registered for manifest. Known: {}",
                    manifest.id(), engineSuppliers.keySet());
            throw new DebeziumException("No engine found for manifest: " + manifest.id());
        }

        if (runners.containsKey(manifest.id())) {
            LOGGER.warn("start({}) rejected: engine already running for manifest", manifest.id());
            throw new DebeziumException("Engine already running for manifest: " + manifest.id());
        }

        Debezium debezium;
        try {
            debezium = engineSuppliers.get(manifest.id()).get();
        }
        catch (RuntimeException e) {
            LOGGER.error("start({}) failed while creating Debezium instance ({}: {})",
                    manifest.id(), e.getClass().getName(), e.getMessage(), e);
            throw e;
        }

        DebeziumRunner runner = new DebeziumRunner(
                DebeziumThreadHandler.getThreadFactory(debezium), debezium);

        if (runners.putIfAbsent(manifest.id(), runner) != null) {
            closeQuietly(debezium, manifest);
            LOGGER.warn("start({}) lost putIfAbsent race; treating as already running", manifest.id());
            throw new DebeziumException("Engine already running for manifest: " + manifest.id());
        }

        currentEngines.put(manifest.id(), debezium);

        try {
            runner.start();
            LOGGER.info("start({}) success; runner thread launched", manifest.id());
        }
        catch (RuntimeException e) {
            runners.remove(manifest.id());
            currentEngines.remove(manifest.id());
            closeQuietly(debezium, manifest);
            LOGGER.error("Failed to start engine for manifest: {} ({}: {})",
                    manifest.id(), e.getClass().getName(), e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void stop(EngineManifest manifest) {
        LOGGER.info("stop({}) called; currently running={}", manifest.id(), runners.keySet());
        DebeziumRunner runner = runners.remove(manifest.id());
        if (runner == null) {
            LOGGER.warn("stop({}) rejected: no running engine. Current runners: {}",
                    manifest.id(), runners.keySet());
            throw new DebeziumException("No running engine found for manifest: " + manifest.id());
        }
        try {
            runner.shutdown();
            LOGGER.info("stop({}) success", manifest.id());
        }
        catch (RuntimeException e) {
            LOGGER.error("Failed to stop engine for manifest: {} ({}: {})",
                    manifest.id(), e.getClass().getName(), e.getMessage(), e);
            throw e;
        }
    }

    private static void closeQuietly(Debezium debezium, EngineManifest manifest) {
        try {
            ((RunnableDebezium) debezium).close();
        }
        catch (Exception closeError) {
            // Log instead of rethrow: this runs on the failure path of start(), and the caller needs to seethe original start() failure,
            // not a secondary close error that would shadow it.
            LOGGER.warn("Failed to close engine after failed start for manifest: {}", manifest.id(), closeError);
        }
    }
}
