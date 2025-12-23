/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvents;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CapturingEventsInvokerRegistryProducer {

    @Inject
    Instance<CapturingEventsInvoker> invokers;

    @Produces
    public CapturingEventsInvokerRegistry<CapturingEvents<Object>> produce() {
        Map<String, CapturingEventsInvoker> handlers = this.invokers
                .stream()
                .collect(Collectors.toMap(CapturingInvoker::generateKey, Function.identity()));

        return event -> handlers.getOrDefault(event.engine() + "_" + event.destination(), handlers.get(event.engine() + "_" + Capturing.ALL));
    }
}
