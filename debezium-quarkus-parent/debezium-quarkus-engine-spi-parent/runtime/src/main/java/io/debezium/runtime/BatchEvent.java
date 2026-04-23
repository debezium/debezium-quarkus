/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime;

import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.Header;

public interface BatchEvent {

    Object key();

    Object value();

    Integer partition();

    SourceRecord record();

    void commit();

    default <H> List<Header<H>> headers() {
        return Collections.emptyList();
    }
}
