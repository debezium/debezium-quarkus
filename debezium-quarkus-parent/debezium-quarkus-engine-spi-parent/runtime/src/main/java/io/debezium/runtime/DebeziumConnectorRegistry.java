/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import java.util.List;

public interface DebeziumConnectorRegistry {
    /**
     *
     * @return the {@link Connector} type for this registry
     */
    Connector connector();

    /**
     *
     * @param manifest
     * @return the {@link Debezium} engine instance assigned to a {@link EngineManifest}
     */
    Debezium get(EngineManifest manifest);

    /**
     *
     * @return all registered {@link EngineManifest}s, regardless of whether their engines are running
     */
    List<EngineManifest> manifests();

    /**
     *
     * @return the currently running {@link Debezium} engines inside the registry
     */
    List<Debezium> engines();

    /**
     * Starts the {@link Debezium} engine assigned to the given {@link EngineManifest}.
     *
     * @param manifest the manifest identifying the engine to start
     */
    void start(EngineManifest manifest);

    /**
     * Stops the {@link Debezium} engine assigned to the given {@link EngineManifest}.
     *
     * @param manifest the manifest identifying the engine to stop
     */
    void stop(EngineManifest manifest);
}
