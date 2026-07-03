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

import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.inject.Instance;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.engine.ChangeEvent;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;
import io.quarkus.debezium.engine.capture.CapturingInvokerRegistry;
import io.quarkus.debezium.engine.deserializer.CapturingEventDeserializerRegistry;

class SourceRecordEventProducerTest {

    @SuppressWarnings("unchecked")
    @Test
    void testSingleRecordConsumerWrappedWithErrorHandling() throws Exception {
        CapturingInvokerRegistry filterRegistry = mock(CapturingInvokerRegistry.class);
        CapturingInvokerRegistry eventRegistry = mock(CapturingInvokerRegistry.class);
        CapturingEventDeserializerRegistry deserializerRegistry = mock(CapturingEventDeserializerRegistry.class);
        CapturingInvokerRegistry objectInvokerRegistry = mock(CapturingInvokerRegistry.class);

        SourceRecordEventProducer producer = new SourceRecordEventProducer(
                filterRegistry, eventRegistry, deserializerRegistry, objectInvokerRegistry);

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

        SourceRecordConsumerHandler handler = producer.produce(configuration, errorHandlers);
        assertNotNull(handler);

        EngineManifest manifest = new EngineManifest("test-engine");
        SourceRecordEventConsumer consumer = handler.get(manifest);
        assertNotNull(consumer);

        ChangeEvent<SourceRecord, SourceRecord> event = mock(ChangeEvent.class);
        SourceRecord sourceRecord = mock(SourceRecord.class);
        when(event.value()).thenReturn(sourceRecord);
        consumer.accept(event);

        verify(customHandler).handle(any(CapturingEvent.class), any(ErrorHandler.EventConsumer.class));
    }
}
