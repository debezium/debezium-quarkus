/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.ErrorHandler;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;

class ErrorHandlerResolverTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorHandlerResolverTest.class);

    @SuppressWarnings("unchecked")
    @Test
    void testResolveNamedBean() {
        DebeziumEngineRuntimeConfiguration config = mock(DebeziumEngineRuntimeConfiguration.class);
        DebeziumEngineConfiguration.Capturing capturing = mock(DebeziumEngineConfiguration.Capturing.class);
        when(config.capturing()).thenReturn(Map.of("test-engine", capturing));
        when(capturing.engineId()).thenReturn(Optional.of("test-engine"));
        when(capturing.errorHandler()).thenReturn(Optional.of("myNamedHandler"));

        ErrorHandler handler = mock(ErrorHandler.class);
        Instance<ErrorHandler> errorHandlers = mock(Instance.class);
        Instance<ErrorHandler> namedInstance = mock(Instance.class);
        when(errorHandlers.select(any(jakarta.enterprise.inject.literal.NamedLiteral.class))).thenReturn(namedInstance);
        when(namedInstance.isResolvable()).thenReturn(true);
        when(namedInstance.get()).thenReturn(handler);

        ErrorHandler resolved = ErrorHandlerResolver.resolve(new EngineManifest("test-engine"), config, errorHandlers, LOGGER);
        assertSame(handler, resolved);
    }

    @SuppressWarnings("unchecked")
    @Test
    void testResolveByClassName() {
        DebeziumEngineRuntimeConfiguration config = mock(DebeziumEngineRuntimeConfiguration.class);
        DebeziumEngineConfiguration.Capturing capturing = mock(DebeziumEngineConfiguration.Capturing.class);
        when(config.capturing()).thenReturn(Map.of("test-engine", capturing));
        when(capturing.engineId()).thenReturn(Optional.of("test-engine"));
        when(capturing.errorHandler()).thenReturn(Optional.of(MyCustomHandler.class.getName()));

        ErrorHandler handler = new MyCustomHandler();
        Instance<ErrorHandler> errorHandlers = mock(Instance.class);
        Instance<ErrorHandler> namedInstance = mock(Instance.class);
        when(errorHandlers.select(any(jakarta.enterprise.inject.literal.NamedLiteral.class))).thenReturn(namedInstance);
        when(namedInstance.isResolvable()).thenReturn(false);

        Instance.Handle<ErrorHandler> handle = mock(Instance.Handle.class);
        Bean bean = mock(Bean.class);
        when(handle.getBean()).thenReturn(bean);
        when(bean.getBeanClass()).thenReturn(MyCustomHandler.class);
        when(handle.get()).thenReturn(handler);
        doReturn(Collections.singletonList(handle)).when(errorHandlers).handles();

        ErrorHandler resolved = ErrorHandlerResolver.resolve(new EngineManifest("test-engine"), config, errorHandlers, LOGGER);
        assertSame(handler, resolved);
    }

    @SuppressWarnings("unchecked")
    @Test
    void testResolveNotFoundThrowsException() {
        DebeziumEngineRuntimeConfiguration config = mock(DebeziumEngineRuntimeConfiguration.class);
        DebeziumEngineConfiguration.Capturing capturing = mock(DebeziumEngineConfiguration.Capturing.class);
        when(config.capturing()).thenReturn(Map.of("test-engine", capturing));
        when(capturing.engineId()).thenReturn(Optional.of("test-engine"));
        when(capturing.errorHandler()).thenReturn(Optional.of("nonExistent"));

        Instance<ErrorHandler> errorHandlers = mock(Instance.class);
        Instance<ErrorHandler> namedInstance = mock(Instance.class);
        when(errorHandlers.select(any(jakarta.enterprise.inject.literal.NamedLiteral.class))).thenReturn(namedInstance);
        when(namedInstance.isResolvable()).thenReturn(false);
        when(errorHandlers.handles()).thenReturn(Collections.emptyList());

        assertThrows(DebeziumException.class, () -> ErrorHandlerResolver.resolve(new EngineManifest("test-engine"), config, errorHandlers, LOGGER));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testResolveRetryHandlerWhenMaxRetriesConfigured() {
        DebeziumEngineRuntimeConfiguration config = mock(DebeziumEngineRuntimeConfiguration.class);
        DebeziumEngineConfiguration.Capturing capturing = mock(DebeziumEngineConfiguration.Capturing.class);
        when(config.capturing()).thenReturn(Map.of("test-engine", capturing));
        when(capturing.engineId()).thenReturn(Optional.of("test-engine"));
        when(capturing.errorHandler()).thenReturn(Optional.empty());
        when(capturing.maxRetries()).thenReturn(Optional.of(3));
        when(capturing.initialDelayMs()).thenReturn(Optional.of(500L));
        when(capturing.maxDelayMs()).thenReturn(Optional.of(5000L));
        when(capturing.delayMultiplier()).thenReturn(Optional.of(2.0));

        Instance<ErrorHandler> errorHandlers = mock(Instance.class);

        ErrorHandler resolved = ErrorHandlerResolver.resolve(new EngineManifest("test-engine"), config, errorHandlers, LOGGER);
        assertNotNull(resolved);
        assertInstanceOf(DefaultRetryErrorHandler.class, resolved);
    }

    @SuppressWarnings("unchecked")
    @Test
    void testResolveNullWhenNoConfiguration() {
        DebeziumEngineRuntimeConfiguration config = mock(DebeziumEngineRuntimeConfiguration.class);
        DebeziumEngineConfiguration.Capturing capturing = mock(DebeziumEngineConfiguration.Capturing.class);
        when(config.capturing()).thenReturn(Map.of("test-engine", capturing));
        when(capturing.engineId()).thenReturn(Optional.of("test-engine"));
        when(capturing.errorHandler()).thenReturn(Optional.empty());
        when(capturing.maxRetries()).thenReturn(Optional.empty());

        Instance<ErrorHandler> errorHandlers = mock(Instance.class);

        ErrorHandler resolved = ErrorHandlerResolver.resolve(new EngineManifest("test-engine"), config, errorHandlers, LOGGER);
        assertNull(resolved);
    }

    static class MyCustomHandler implements ErrorHandler {
    }
}
