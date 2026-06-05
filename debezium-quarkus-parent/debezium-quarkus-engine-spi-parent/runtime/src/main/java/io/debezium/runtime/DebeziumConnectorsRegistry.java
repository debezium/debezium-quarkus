/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import java.util.List;
import java.util.Optional;

import io.debezium.DebeziumException;

/**
 * A top-level registry that aggregates {@link DebeziumConnectorRegistry} instances across all
 * active Debezium connectors.
 * <p>
 * Each {@link DebeziumConnectorRegistry} groups the {@link Debezium} engine instances that belong
 * to a single connector type. This interface allows looking up those per-connector registries and
 * querying engines regardless of which connector they belong to.
 */
public interface DebeziumConnectorsRegistry {
    /**
     * Returns the {@link DebeziumConnectorRegistry} that holds all engines for the given connector type.
     *
     * @param connector the connector type whose registry is requested
     * @return the registry for {@code connector}, or an empty {@link Optional} if no engines for
     *         that connector are registered
     */
    Optional<DebeziumConnectorRegistry> registry(Connector connector);

    /**
     * Returns all the {@link DebeziumConnectorRegistry} for all the connectors.
     *
     * @return an unmodifiable list of all registered registries; never {@code null}
     */
    List<DebeziumConnectorRegistry> registries();

    /**
     * Returns the {@link Debezium} engine instance identified by the given manifest.
     *
     * @param manifest the manifest that uniquely identifies the engine
     * @return the engine assigned to {@code manifest}, or an empty {@link Optional} if no matching
     *         engine is found
     */
    Optional<Debezium> get(EngineManifest manifest);

    /**
     * Returns all the running {@link Debezium} engines inside the registry currently held across every connector registry.
     *
     * @return an unmodifiable list of all registered engines; never {@code null}
     */
    List<Debezium> runningEngines();

    /**
     * Returns all the {@link Debezium} engines inside the registry currently held across every connector registry.
     *
     * @return an unmodifiable list of all registered engines; never {@code null}
     */
    List<Debezium> engines();

    /**
     * Starts the {@link Debezium} engine assigned to the given {@link EngineManifest}.
     *
     * @param manifest the manifest identifying the engine to start
     * @throws DebeziumException if no engine is registered for the manifest, or if the engine is already running
     */
    void start(EngineManifest manifest);

    /**
     * Stops the {@link Debezium} engine assigned to the given {@link EngineManifest}.
     *
     * @param manifest the manifest identifying the engine to stop
     * @throws DebeziumException if no running engine is found for the manifest
     */
    void stop(EngineManifest manifest);
}
