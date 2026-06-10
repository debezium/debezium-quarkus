/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import java.util.Optional;

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;

import io.debezium.runtime.CapturingEvents;
import io.quarkus.debezium.engine.capture.CapturingEventsInvokerRegistry;
import io.quarkus.debezium.engine.capture.CapturingTombstoneEvents;

public class GeneralChangeConsumerProducer {

    private final CapturingEventsInvokerRegistry<CapturingEvents> filterRegistry;
    private final CapturingEventsInvokerRegistry<CapturingEvents> registry;
    private final Optional<CapturingTombstoneEvents> capturingTombstoneEvents;

    public GeneralChangeConsumerProducer(@Named("capturingFilterEventsInvokerRegistry") CapturingEventsInvokerRegistry<CapturingEvents> filterRegistry,
                                         @Named("capturingEventsInvokerRegistry") CapturingEventsInvokerRegistry<CapturingEvents> registry,
                                         Instance<CapturingTombstoneEvents> instances) {
        this.filterRegistry = filterRegistry;
        this.registry = registry;
        this.capturingTombstoneEvents = instances
                .stream()
                .findFirst();
    }

    @Produces
    @Dependent
    public ChangeConsumerFactory produce() {
        if (registry == null) {
            return null;
        }

        return new DefaultChangeConsumerFactory(filterRegistry, registry, capturingTombstoneEvents);
    }

}
