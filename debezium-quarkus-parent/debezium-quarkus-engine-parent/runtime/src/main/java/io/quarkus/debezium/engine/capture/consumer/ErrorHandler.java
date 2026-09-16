/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import java.util.Optional;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;

public interface ErrorHandler {

    @FunctionalInterface
    interface BatchConsumer {
        void accept(CapturingEvents<BatchEvent> events) throws Exception;
    }

    @FunctionalInterface
    interface EventConsumer {
        void accept(CapturingEvent<SourceRecord, SourceRecord> event) throws Exception;
    }

    default void handle(CapturingEvents<BatchEvent> records, BatchConsumer consumer) throws Exception {
        consumer.accept(records);
    }

    default void handle(CapturingEvent<SourceRecord, SourceRecord> event, EventConsumer consumer) throws Exception {
        consumer.accept(event);
    }

    static ErrorHandler resolve(
                                EngineManifest manifest,
                                DebeziumEngineRuntimeConfiguration configuration,
                                Instance<ErrorHandler> errorHandlers,
                                Logger logger) {
        if (configuration == null || configuration.capturing() == null) {
            return null;
        }

        Optional<DebeziumEngineRuntimeConfiguration.Capturing> capturingConfig = configuration.capturing().values().stream()
                .filter(c -> c.engineId().isPresent() && c.engineId().get().equals(manifest.id()))
                .findFirst();
        if (capturingConfig.isEmpty()) {
            DebeziumEngineRuntimeConfiguration.Capturing byKey = configuration.capturing().get(manifest.id());
            if (byKey != null) {
                capturingConfig = Optional.of(byKey);
            }
        }
        if (capturingConfig.isEmpty() && "default".equals(manifest.id())) {
            DebeziumEngineRuntimeConfiguration.Capturing defaultKey = configuration.capturing().get("default");
            if (defaultKey != null) {
                capturingConfig = Optional.of(defaultKey);
            }
        }

        ErrorHandler errorHandler = null;

        if (capturingConfig.isPresent()) {
            DebeziumEngineRuntimeConfiguration.Capturing cap = capturingConfig.get();
            if (cap.errorHandler().isPresent()) {
                String handlerName = cap.errorHandler().get();
                Instance<ErrorHandler> namedInstance = errorHandlers.select(NamedLiteral.of(handlerName));
                if (namedInstance.isResolvable()) {
                    errorHandler = namedInstance.get();
                }
                else {
                    try {
                        Class<?> clazz = Class.forName(handlerName, true, Thread.currentThread().getContextClassLoader());
                        errorHandler = (ErrorHandler) clazz.getDeclaredConstructor().newInstance();
                    }
                    catch (Exception e) {
                        throw new RuntimeException("Could not find or instantiate ErrorHandler: " + handlerName, e);
                    }
                }
            }
            else {
                if (!errorHandlers.isUnsatisfied()) {
                    if (errorHandlers.isResolvable()) {
                        errorHandler = errorHandlers.get();
                    }
                    else {
                        logger.warn("Multiple ErrorHandler CDI beans found but none specifically configured for engine. Using none.");
                    }
                }

                if (errorHandler == null) {
                    int maxRetries = cap.maxRetries().orElse(0);
                    if (maxRetries > 0) {
                        long initialDelayMs = cap.initialDelayMs().orElse(1000L);
                        long maxDelayMs = cap.maxDelayMs().orElse(10000L);
                        double delayMultiplier = cap.delayMultiplier().orElse(2.0);
                        errorHandler = new DefaultRetryErrorHandler(maxRetries, initialDelayMs, maxDelayMs, delayMultiplier);
                    }
                }
            }
        }
        else {
            if (!errorHandlers.isUnsatisfied()) {
                if (errorHandlers.isResolvable()) {
                    errorHandler = errorHandlers.get();
                }
                else {
                    logger.warn("Multiple ErrorHandler CDI beans found but none specifically configured for engine. Using none.");
                }
            }
        }

        return errorHandler;
    }
}
