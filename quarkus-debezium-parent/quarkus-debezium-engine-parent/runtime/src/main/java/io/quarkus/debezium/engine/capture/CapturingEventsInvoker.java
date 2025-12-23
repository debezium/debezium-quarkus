/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import io.debezium.runtime.CapturingEvents;

public interface CapturingEventsInvoker extends CapturingInvoker<CapturingEvents<Object>> {
    @Override
    void capture(CapturingEvents<Object> event);
}
