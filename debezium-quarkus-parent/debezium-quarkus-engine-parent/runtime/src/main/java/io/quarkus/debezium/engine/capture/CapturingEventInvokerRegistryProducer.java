/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import static io.quarkus.debezium.engine.capture.CapturingInvoker.CapturingInvokerKey;
import static io.quarkus.debezium.engine.capture.CapturingInvoker.getAllDestinations;
import static io.quarkus.debezium.engine.capture.CapturingInvoker.getKey;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import io.debezium.runtime.CapturingEvent;

public class CapturingEventInvokerRegistryProducer {
    public static final Predicate<CapturingEventInvoker> NO_CAPTURING_FILTER_PREDICATE = inv -> !inv.hasFilter();
    private final CapturingInvokerValidator<CapturingEventInvoker> validator = new CapturingInvokerValidator<>();

    @Inject
    Instance<CapturingEventInvoker> invokers;

    @Produces
    @Singleton
    @Named("capturingEventInvokerRegistry")
    public CapturingInvokerRegistry<CapturingEvent<Object, Object>> produce() {
        validator.validate(this.invokers
                .stream()
                .filter(NO_CAPTURING_FILTER_PREDICATE)
                .toList());

        Map<CapturingInvokerKey, CapturingEventInvoker> invokers = this.invokers
                .stream()
                .filter(NO_CAPTURING_FILTER_PREDICATE)
                .collect(Collectors.toMap(CapturingInvoker::generateKey, Function.identity()));

        return event -> Optional.ofNullable(invokers.getOrDefault(getKey(event), invokers.get(getAllDestinations(event))));
    }

}
