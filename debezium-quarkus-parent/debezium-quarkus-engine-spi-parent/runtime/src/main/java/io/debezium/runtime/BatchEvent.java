/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime;

import io.debezium.engine.Header;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Collections;
import java.util.List;

public interface BatchEvent {

    Object key();

    Object value();

    int partition();

    SourceRecord record();

    void commit();

    default <H> List<Header<H>> headers() {
        return Collections.emptyList();
    }
}
