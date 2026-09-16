/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.CapturingEvents;

class DefaultRetryErrorHandlerTest {

    @Test
    void testBatchConsumerRetrySuccess() throws Exception {
        DefaultRetryErrorHandler handler = new DefaultRetryErrorHandler(3, 10, 50, 2.0);
        ErrorHandler.BatchConsumer batchConsumer = mock(ErrorHandler.BatchConsumer.class);
        CapturingEvents<BatchEvent> records = mock(CapturingEvents.class);

        doThrow(new RuntimeException("Temporary failure"))
                .doNothing()
                .when(batchConsumer).accept(records);

        handler.handle(records, batchConsumer);

        verify(batchConsumer, times(2)).accept(records);
    }

    @Test
    void testBatchConsumerRetryFailureExceeded() throws Exception {
        DefaultRetryErrorHandler handler = new DefaultRetryErrorHandler(2, 5, 20, 2.0);
        ErrorHandler.BatchConsumer batchConsumer = mock(ErrorHandler.BatchConsumer.class);
        CapturingEvents<BatchEvent> records = mock(CapturingEvents.class);

        doThrow(new RuntimeException("Persistent failure"))
                .when(batchConsumer).accept(records);

        assertThrows(RuntimeException.class, () -> {
            handler.handle(records, batchConsumer);
        });

        verify(batchConsumer, times(3)).accept(records);
    }

    @Test
    void testSingleRecordRetrySuccess() throws Exception {
        DefaultRetryErrorHandler handler = new DefaultRetryErrorHandler(3, 10, 50, 2.0);
        ErrorHandler.EventConsumer eventConsumer = mock(ErrorHandler.EventConsumer.class);
        CapturingEvent<SourceRecord, SourceRecord> event = new CapturingEvent.Create<>(null, null, null, null, null, null);

        doThrow(new RuntimeException("Temporary failure"))
                .doNothing()
                .when(eventConsumer).accept(event);

        handler.handle(event, eventConsumer);

        verify(eventConsumer, times(2)).accept(event);
    }

    @Test
    void testSingleRecordRetryFailureExceeded() throws Exception {
        DefaultRetryErrorHandler handler = new DefaultRetryErrorHandler(2, 5, 20, 2.0);
        ErrorHandler.EventConsumer eventConsumer = mock(ErrorHandler.EventConsumer.class);
        CapturingEvent<SourceRecord, SourceRecord> event = new CapturingEvent.Create<>(null, null, null, null, null, null);

        doThrow(new RuntimeException("Persistent failure"))
                .when(eventConsumer).accept(event);

        assertThrows(RuntimeException.class, () -> {
            handler.handle(event, eventConsumer);
        });

        verify(eventConsumer, times(3)).accept(event);
    }
}
