/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime;

import io.debezium.common.annotation.Incubating;
import io.debezium.engine.format.SerializationFormat;

/**
 * serialization information associated to a running {@link Debezium} engine
 */
@Incubating
public interface DebeziumSerialization<K, V, H> {

    /**
     *
     * @return the key {@link SerializationFormat}
     */
    Class<? extends SerializationFormat<K>> getKeyFormat();

    /**
     *
     * @return the value {@link SerializationFormat}
     */
    Class<? extends SerializationFormat<V>> getValueFormat();

    /**
     *
     * @return the header {@link SerializationFormat}
     */
    Class<? extends SerializationFormat<H>> getHeaderFormat();

    /**
     *
     * @return the id for the {@link Debezium} engine
     */
    String getEngineId();
}
