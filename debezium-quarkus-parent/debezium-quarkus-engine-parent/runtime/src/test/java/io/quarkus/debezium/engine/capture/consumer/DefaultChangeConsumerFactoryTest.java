/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.inject.Instance;

import org.junit.jupiter.api.Test;

import io.debezium.runtime.CapturingEvents;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.ErrorHandler;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;
import io.quarkus.debezium.engine.capture.CapturingEventsInvokerRegistry;

class DefaultChangeConsumerFactoryTest {

    @SuppressWarnings("unchecked")
    @Test
    void testChangeConsumerWrappedWithErrorHandling() throws Exception {
        CapturingEventsInvokerRegistry filterRegistry = mock(CapturingEventsInvokerRegistry.class);
        CapturingEventsInvokerRegistry registry = mock(CapturingEventsInvokerRegistry.class);

        DebeziumEngineRuntimeConfiguration configuration = mock(DebeziumEngineRuntimeConfiguration.class);
        DebeziumEngineConfiguration.Capturing capturing = mock(DebeziumEngineConfiguration.Capturing.class);
        when(configuration.capturing()).thenReturn(Map.of("test-engine", capturing));

        when(capturing.engineId()).thenReturn(Optional.of("test-engine"));
        when(capturing.errorHandler()).thenReturn(Optional.of("customErrorHandler"));

        ErrorHandler customHandler = mock(ErrorHandler.class);
        Instance<ErrorHandler> errorHandlers = mock(Instance.class);
        Instance<ErrorHandler> selectInstance = mock(Instance.class);

        when(errorHandlers.select(any(java.lang.annotation.Annotation.class))).thenReturn(selectInstance);
        when(selectInstance.isResolvable()).thenReturn(true);
        when(selectInstance.get()).thenReturn(customHandler);

        DefaultChangeConsumerFactory factory = new DefaultChangeConsumerFactory(
                filterRegistry, registry, Optional.empty(), configuration, errorHandlers);

        EngineManifest manifest = new EngineManifest("test-engine");
        QuarkusChangeConsumer changeConsumer = factory.get(manifest);

        assertNotNull(changeConsumer);

        List records = new ArrayList<>();
        io.debezium.engine.DebeziumEngine.RecordCommitter committer = mock(io.debezium.engine.DebeziumEngine.RecordCommitter.class);

        changeConsumer.handleBatch(records, committer);

        verify(customHandler).handle(any(CapturingEvents.class), any(ErrorHandler.BatchConsumer.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testBatchConsumerRetryRedeliversEvents() throws Exception {
        CapturingEventsInvokerRegistry filterRegistry = mock(CapturingEventsInvokerRegistry.class);
        CapturingEventsInvokerRegistry registry = mock(CapturingEventsInvokerRegistry.class);

        DebeziumEngineRuntimeConfiguration configuration = mock(DebeziumEngineRuntimeConfiguration.class);
        DebeziumEngineConfiguration.Capturing capturing = mock(DebeziumEngineConfiguration.Capturing.class);
        when(configuration.capturing()).thenReturn(Map.of("test-engine", capturing));

        when(capturing.engineId()).thenReturn(Optional.of("test-engine"));
        when(capturing.errorHandler()).thenReturn(Optional.of("retryingHandler"));

        ErrorHandler retryingHandler = new ErrorHandler() {
            @Override
            public void handle(CapturingEvents<io.debezium.runtime.BatchEvent> records, BatchConsumer consumer) throws Exception {
                try {
                    consumer.accept(records);
                }
                catch (Exception ignored) {
                }
                consumer.accept(records);
            }
        };

        Instance<ErrorHandler> errorHandlers = mock(Instance.class);
        Instance<ErrorHandler> selectInstance = mock(Instance.class);

        when(errorHandlers.select(any(java.lang.annotation.Annotation.class))).thenReturn(selectInstance);
        when(selectInstance.isResolvable()).thenReturn(true);
        when(selectInstance.get()).thenReturn(retryingHandler);

        DefaultChangeConsumerFactory factory = new DefaultChangeConsumerFactory(
                filterRegistry, registry, Optional.empty(), configuration, errorHandlers);

        EngineManifest manifest = new EngineManifest("test-engine");
        QuarkusChangeConsumer changeConsumer = factory.get(manifest);

        assertNotNull(changeConsumer);

        io.debezium.engine.ChangeEvent record = mock(io.debezium.engine.ChangeEvent.class);
        when(record.destination()).thenReturn("orders");
        List records = List.of(record);
        io.debezium.engine.DebeziumEngine.RecordCommitter committer = mock(io.debezium.engine.DebeziumEngine.RecordCommitter.class);

        io.quarkus.debezium.engine.capture.CapturingInvoker invoker = mock(io.quarkus.debezium.engine.capture.CapturingInvoker.class);
        when(invoker.destination()).thenReturn("orders");
        when(registry.get(any(CapturingEvents.class))).thenReturn(invoker);

        changeConsumer.handleBatch(records, committer);

        verify(invoker, org.mockito.Mockito.times(4)).capture(any(CapturingEvents.class));
    }
}
