/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.deserializer;

/**
 * Mutable Registry that should be used only for Testing purpose to avoid side effects
 * @param <V>
 */
public interface MutableCapturingEventDeserializerRegistry<K, V> extends CapturingEventDeserializerRegistry<K, V> {
    void register(String identifier, Deserializer<?> deserializer);

    void unregister(String identifier);
}
