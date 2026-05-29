/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import static io.quarkus.debezium.engine.capture.CapturingInvoker.CapturingInvokerKey;
import static io.quarkus.debezium.engine.capture.CapturingInvoker.getKey;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

public class CapturingObjectInvokerRegistryProducer {
    private final CapturingInvokerValidator<CapturingObjectInvoker> validator = new CapturingInvokerValidator<>();

    @Inject
    Instance<CapturingObjectInvoker> invokers;

    @Produces
    @Singleton
    public CapturingInvokerRegistry<Object> produce() {
        validator.validate(this.invokers
                .stream()
                .toList());

        Map<CapturingInvokerKey, CapturingObjectInvoker> invokers = this.invokers
                .stream()
                .collect(Collectors.toMap(CapturingInvoker::generateKey, Function.identity()));

        return event -> Optional.ofNullable(invokers.get(getKey(event)));
    }

}
