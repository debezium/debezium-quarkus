/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.debezium.DebeziumException;
import io.debezium.runtime.Connector;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.EngineManifest;

/**
 * A {@link DebeziumConnectorRegistry} for a connector that has no dedicated Quarkus extension
 * (compatibility mode): the engines are created up front, one per manifest, and started on demand.
 * <p>
 * Used both for the connectors discovered at build time from the application's dependencies and for
 * a {@code connector.class} that is only known at runtime, for example a connector jar added to a
 * Debezium Server distribution.
 */
public class CompatibleModeConnectorRegistry implements DebeziumConnectorRegistry {

    private final Connector connector;
    private final Map<String, Debezium> engines;
    private final Map<String, DebeziumRunner> runners = new ConcurrentHashMap<>();

    public CompatibleModeConnectorRegistry(Connector connector, Map<String, Debezium> engines) {
        this.connector = connector;
        this.engines = Map.copyOf(engines);
    }

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
        return engines.keySet()
                .stream()
                .map(EngineManifest::new)
                .toList();
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
}
