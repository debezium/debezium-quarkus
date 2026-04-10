/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import io.debezium.common.annotation.Incubating;

/**
 * Indicates whether an implementation supports capturing and processing tombstone events.
 * <p>
 * In Debezium, a tombstone event is a special change event with a null value that is emitted
 * after a delete operation. These events are used to signal that a record has been deleted
 * and can be used by downstream consumers (e.g., for compaction in Kafka topics).
 * <p>
 * Implementations of this interface can be provided as CDI beans to declare support for
 * tombstone event processing from Debezium Server Sink side.
 * <p>
 * A Sink (DS Sink) may or may not handle a tombstone event.
 */
@Incubating
public interface CapturingTombstoneEvents {
    boolean isSupported();
}
