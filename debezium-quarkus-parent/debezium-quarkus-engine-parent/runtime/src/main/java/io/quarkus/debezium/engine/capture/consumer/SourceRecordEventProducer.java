/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import java.util.Optional;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.runtime.CapturingEvent;
import io.quarkus.debezium.engine.OperationMapper;
import io.quarkus.debezium.engine.capture.CapturingInvoker;
import io.quarkus.debezium.engine.capture.CapturingInvokerRegistry;
import io.quarkus.debezium.engine.deserializer.CapturingEventDeserializerRegistry;

public class SourceRecordEventProducer {

    private final CapturingInvokerRegistry<CapturingEvent<?, ?>> capturingFilterRegistry;
    private final CapturingInvokerRegistry<CapturingEvent<?, ?>> capturingDefaultRegistry;
    private final CapturingInvokerRegistry<Object> capturingObjectInvokerRegistry;
    private final CapturingEventDeserializerRegistry<SourceRecord, SourceRecord> capturingEventDeserializerRegistry;

    @Inject
    public SourceRecordEventProducer(@Named("capturingFilterInvokerRegistry") CapturingInvokerRegistry<CapturingEvent<?, ?>> capturingFilterRegistry,
                                     @Named("capturingEventInvokerRegistry") CapturingInvokerRegistry<CapturingEvent<?, ?>> capturingEventRegistry,
                                     CapturingEventDeserializerRegistry<SourceRecord, SourceRecord> capturingEventDeserializerRegistry,
                                     CapturingInvokerRegistry<Object> capturingObjectInvokerRegistry) {
        this.capturingFilterRegistry = capturingFilterRegistry;
        this.capturingDefaultRegistry = capturingEventRegistry;
        this.capturingEventDeserializerRegistry = capturingEventDeserializerRegistry;
        this.capturingObjectInvokerRegistry = capturingObjectInvokerRegistry;
    }

    /**
     * Dispatches events through a fallback chain: filter registry first, then object invoker
     * with deserializer, then default registry (destination match + wildcard fallback).
     * First match wins. If nothing matches, the event is dropped with a warning.
     */
    @Produces
    @Singleton
    public SourceRecordConsumerHandler produce() {
        return manifest -> new SourceRecordEventConsumer() {
            private final Logger logger = LoggerFactory.getLogger(SourceRecordEventConsumer.class);

            @Override
            public void accept(ChangeEvent<SourceRecord, SourceRecord> event) {
                logger.trace("receiving event {} with engine id {}", event.destination(), manifest.id());
                CapturingEvent<SourceRecord, SourceRecord> capturingEvent = new OperationMapper(manifest.id()).from(event);

                Optional<CapturingInvoker<CapturingEvent<?, ?>>> filterInvoker = capturingFilterRegistry.get(capturingEvent);
                if (filterInvoker.isPresent()) {
                    logger.trace("filter invoker matched for capturing event: {}", capturingEvent.destination());
                    filterInvoker.get().capture(capturingEvent);
                    return;
                }

                var deserializer = capturingEventDeserializerRegistry.get(capturingEvent.destination());
                Optional<CapturingInvoker<Object>> objectCapturingInvoker = capturingObjectInvokerRegistry.get(capturingEvent);
                if (deserializer != null && objectCapturingInvoker.isPresent()) {
                    logger.trace("method annotated with @Capturing with object mapping found for destination: {}", capturingEvent.destination());
                    objectCapturingInvoker.get().capture(deserializer.deserialize(capturingEvent).record());
                    return;
                }

                Optional<CapturingInvoker<CapturingEvent<?, ?>>> invoker = capturingDefaultRegistry.get(capturingEvent);
                if (invoker.isPresent()) {
                    if (deserializer != null) {
                        logger.trace("deserializer found for destination: {}", capturingEvent.destination());
                        invoker.get().capture(deserializer.deserialize(capturingEvent));
                        return;
                    }
                    logger.trace("deserializer not found, using default invoker for: {}", capturingEvent.destination());
                    invoker.get().capture(capturingEvent);
                    return;
                }

                logger.warn("no invoker found for event with destination: {} and engine id: {}", capturingEvent.destination(), manifest.id());
            }
        };
    }
}
