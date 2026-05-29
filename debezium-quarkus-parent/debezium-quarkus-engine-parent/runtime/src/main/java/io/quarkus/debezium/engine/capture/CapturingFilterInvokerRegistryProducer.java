/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import java.util.List;
import java.util.Optional;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import io.debezium.runtime.CapturingEvent;

public class CapturingFilterInvokerRegistryProducer {

    @Inject
    Instance<CapturingEventInvoker> invokers;

    @Produces
    @Singleton
    @Named("capturingFilterInvokerRegistry")
    public CapturingInvokerRegistry<CapturingEvent<Object, Object>> produce() {
        List<CapturingEventInvoker> filterInvokers = this.invokers
                .stream()
                .filter(CapturingInvoker::hasFilter)
                .toList();

        return event -> {
            List<CapturingEventInvoker> matched = filterInvokers
                    .stream()
                    .filter(invoker -> invoker.shouldCapture(event))
                    .toList();

            if (matched.size() > 1) {
                throw new IllegalStateException(
                        "Multiple @Capturing filters matched the same event on destination '"
                                + event.destination() + "' and engine '" + event.engine() + "'");
            }

            return matched.isEmpty() ? Optional.empty() : Optional.of(matched.getFirst());
        };
    }
}
