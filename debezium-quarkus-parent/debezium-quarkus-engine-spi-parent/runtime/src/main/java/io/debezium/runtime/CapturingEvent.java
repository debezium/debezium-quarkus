/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import java.util.Collections;
import java.util.List;

import io.debezium.common.annotation.Incubating;
import io.debezium.engine.Header;

/**
 * A capturing event with key, value, headers and information related to source and destination.
 *
 * @param <V>
 */
@Incubating
public sealed interface CapturingEvent<K, V> {

    K key();

    V record();

    default <H> List<Header<H>> headers() {
        return Collections.emptyList();
    }

    /**
     * @return logical destination for which the event is intended
     */
    String destination();

    /**
     *
     * @return logical source for which the event is intended
     */
    String source();

    /***
     * @return engine for which the event is emitted
     */
    String engine();

    record Read<K, V>(K key, V record, String destination, String source,
            List<Header<Object>> headers, String engine) implements CapturingEvent<K, V> {

    }

    record Create<K, V>(K key, V record, String destination, String source,
            List<Header<Object>> headers, String engine) implements CapturingEvent<K, V> {

    }

    record Update<K, V>(K key, V record, String destination, String source,
            List<Header<Object>> headers, String engine) implements CapturingEvent<K, V> {

    }

    record Delete<K, V>(K key, V record, String destination, String source,
            List<Header<Object>> headers, String engine) implements CapturingEvent<K, V> {

    }

    record Truncate<K, V>(K key, V record, String destination, String source,
            List<Header<Object>> headers, String engine) implements CapturingEvent<K, V> {

    }

    record Message<K, V>(K key, V record, String destination, String source,
            List<Header<Object>> headers, String engine) implements CapturingEvent<K, V> {

    }
}
