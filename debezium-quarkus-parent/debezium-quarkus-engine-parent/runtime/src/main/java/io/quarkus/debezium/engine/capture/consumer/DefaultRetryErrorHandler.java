/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import java.time.Duration;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.util.DelayStrategy;
import io.debezium.util.RetryingRunnable;

public class DefaultRetryErrorHandler implements ErrorHandler {

    private final int maxRetries;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final double delayMultiplier;

    public DefaultRetryErrorHandler(int maxRetries, long initialDelayMs, long maxDelayMs, double delayMultiplier) {
        this.maxRetries = maxRetries;
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.delayMultiplier = delayMultiplier;
    }

    @Override
    public void handle(CapturingEvents<BatchEvent> records, BatchConsumer consumer) throws Exception {
        RetryingRunnable.<Exception> builder()
                .retries(maxRetries)
                .delayStrategy(DelayStrategy.exponential(
                        Duration.ofMillis(initialDelayMs),
                        Duration.ofMillis(maxDelayMs),
                        delayMultiplier))
                .doRun(() -> consumer.accept(records))
                .build()
                .run();
    }

    @Override
    public void handle(CapturingEvent<SourceRecord, SourceRecord> event, EventConsumer consumer) throws Exception {
        RetryingRunnable.<Exception> builder()
                .retries(maxRetries)
                .delayStrategy(DelayStrategy.exponential(
                        Duration.ofMillis(initialDelayMs),
                        Duration.ofMillis(maxDelayMs),
                        delayMultiplier))
                .doRun(() -> consumer.accept(event))
                .build()
                .run();
    }
}
