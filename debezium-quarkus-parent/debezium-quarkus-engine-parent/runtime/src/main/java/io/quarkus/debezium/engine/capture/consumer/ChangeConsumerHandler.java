/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import io.debezium.runtime.EngineManifest;

/**
 * Resolves a {@link QuarkusChangeConsumer} for a specific engine instance.
 * <p>
 * Given an {@link EngineManifest}, implementations return a consumer configured
 * to dispatch CDC events according to the engine's routing strategy
 * (e.g., by destination, tombstone support).
 *
 * @see DefaultConsumerHandler
 */
public interface ChangeConsumerHandler {
    QuarkusChangeConsumer get(EngineManifest engineManifest);
}
