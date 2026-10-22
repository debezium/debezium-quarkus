/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine.capture.consumer;

import java.util.Optional;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;

import org.slf4j.Logger;

import io.debezium.DebeziumException;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.ErrorHandler;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;

public final class ErrorHandlerResolver {

    private ErrorHandlerResolver() {
    }

    public static ErrorHandler resolve(
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

        if (capturingConfig.isEmpty()) {
            return null;
        }

        DebeziumEngineRuntimeConfiguration.Capturing cap = capturingConfig.get();

        if (cap.errorHandler().isPresent()) {
            String handlerName = cap.errorHandler().get();

            Instance<ErrorHandler> namedInstance = errorHandlers.select(NamedLiteral.of(handlerName));
            if (namedInstance.isResolvable()) {
                return namedInstance.get();
            }

            for (Instance.Handle<ErrorHandler> handle : errorHandlers.handles()) {
                Class<?> beanClass = handle.getBean().getBeanClass();
                if (beanClass.getName().equals(handlerName) || beanClass.getSimpleName().equals(handlerName)) {
                    return handle.get();
                }
            }

            throw new DebeziumException("Could not find ErrorHandler CDI bean for: " + handlerName);
        }

        int maxRetries = cap.maxRetries().orElse(0);
        if (maxRetries > 0) {
            long initialDelayMs = cap.initialDelayMs().orElse(1000L);
            long maxDelayMs = cap.maxDelayMs().orElse(10000L);
            double delayMultiplier = cap.delayMultiplier().orElse(2.0);
            return new DefaultRetryErrorHandler(maxRetries, initialDelayMs, maxDelayMs, delayMultiplier);
        }

        return null;
    }
}
