/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;

public class Handler {

    @Capturing
    public void capturing(CapturingEvent<SourceRecord> event) {

    }
}
