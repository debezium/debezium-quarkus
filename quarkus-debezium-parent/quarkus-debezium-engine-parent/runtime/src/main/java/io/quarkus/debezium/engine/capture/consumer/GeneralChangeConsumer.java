/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import java.util.List;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.DebeziumEngine.RecordCommitter;

public interface GeneralChangeConsumer extends ChangeConsumer<ChangeEvent<Object, Object>> {

    @Override
    void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException;
}
