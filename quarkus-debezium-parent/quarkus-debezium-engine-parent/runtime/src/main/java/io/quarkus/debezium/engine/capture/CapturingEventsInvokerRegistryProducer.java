/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import io.debezium.runtime.CapturingEvents;

public class CapturingEventsInvokerRegistryProducer {

    @Inject
    Instance<CapturingEventsInvoker> invokers;

    @Produces
    @Dependent
    public CapturingEventsInvokerRegistry<CapturingEvents<Object>> produce() {
        Map<String, CapturingEventsInvoker> handlers = this.invokers
                .stream()
                .collect(Collectors.toMap(CapturingInvoker::generateKey, Function.identity()));

        if (handlers.isEmpty()) {
            return null;
        }

        return event -> handlers.get(event.engine() + "_" + event.destination());
    }
}
