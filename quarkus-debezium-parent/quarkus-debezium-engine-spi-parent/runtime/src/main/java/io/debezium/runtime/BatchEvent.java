package io.debezium.runtime;

import org.apache.kafka.connect.source.SourceRecord;

public interface BatchEvent {
    Object key();

    Object value();

    int partition();

    SourceRecord record();

    void commit();
}
