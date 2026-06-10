/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import java.util.List;

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.quarkus.debezium.engine.OperationMapper;

/**
 * Filter registry for batch event invokers.
 * Collects batch invokers with a custom filter and returns the first invoker
 * whose filter accepts at least one event in the batch.
 * Returns null if no filter invoker matches any event in the batch.
 */
public class CapturingFilterEventsInvokerRegistryProducer {

    @Inject
    Instance<CapturingEventsInvoker> invokers;

    @Produces
    @Dependent
    @Named("capturingFilterEventsInvokerRegistry")
    @SuppressWarnings("unchecked")
    public CapturingEventsInvokerRegistry<CapturingEvents<Object>> produce() {
        List<CapturingEventsInvoker> filterInvokers = this.invokers
                .stream()
                .filter(CapturingInvoker::hasFilter)
                .toList();

        if (filterInvokers.isEmpty()) {
            return null;
        }

        return events -> {
            OperationMapper operationMapper = new OperationMapper(events.engine());
            List<BatchEvent> batchEvents = (List<BatchEvent>) (List<?>) events.records();

            return filterInvokers
                    .stream()
                    .filter(invoker -> hasAtLeastOneEventToCapture(events, invoker, batchEvents, operationMapper))
                    .findFirst()
                    .orElse(null);
        };
    }

    private static boolean hasAtLeastOneEventToCapture(CapturingEvents<Object> events, CapturingEventsInvoker invoker, List<BatchEvent> batchEvents,
                                                       OperationMapper operationMapper) {
        return batchEvents.stream()
                .anyMatch(batchEvent -> invoker.shouldCapture(
                        operationMapper.from(batchEvent.record(), events.destination())));
    }
}
