/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.Header;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvents;
import io.debezium.runtime.EngineManifest;
import io.quarkus.debezium.engine.capture.CapturingEventsInvokerRegistry;
import io.quarkus.debezium.engine.capture.CapturingTombstoneEvents;

public final class DefaultConsumerHandler implements ChangeConsumerHandler {

    private final RoutedQuarkusChangeConsumer routedQuarkusChangeConsumer;
    private final Optional<CapturingTombstoneEvents> capturingTombstoneEvents;

    DefaultConsumerHandler(CapturingEventsInvokerRegistry<CapturingEvents> registry,
                           Optional<CapturingTombstoneEvents> capturingTombstoneEvents) {
        this.capturingTombstoneEvents = capturingTombstoneEvents;
        this.routedQuarkusChangeConsumer = new RoutedQuarkusChangeConsumer(
                capturingTombstoneEvents,
                new AllDestinationsChangeConsumer(registry),
                new DestinationSpecificChangeConsumer(registry));
    }

    @Override
    public QuarkusChangeConsumer get(EngineManifest manifest) {
        return new QuarkusChangeConsumer() {
            @Override
            public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer) {
                routedQuarkusChangeConsumer.handle(manifest, records, committer);
            }

            @Override
            public boolean supportsTombstoneEvents() {
                return capturingTombstoneEvents
                        .orElse(QuarkusChangeConsumer.super::supportsTombstoneEvents)
                        .isSupported();
            }
        };
    }

    /**
     *
     * {@link AllDestinationsChangeConsumer} triggers the handler that is annotated with {@link Capturing} without any destination:
     *
     * <pre>
     * {@code @Capturing() }
     * {@code public void handle(CapturingEvents<BatchEvent> events) { }
     *     //
     * }
     * </pre>
     * The events keep the engine order
     *
     */
    private static final class AllDestinationsChangeConsumer implements DestinationChangeConsumer {

        private final CapturingEventsInvokerRegistry<CapturingEvents> registry;

        private AllDestinationsChangeConsumer(CapturingEventsInvokerRegistry<CapturingEvents> registry) {
            this.registry = registry;
        }

        @Override
        public void handle(EngineManifest manifest, List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer) {
            List<BatchEvent> batchEvents = records.stream()
                    .map(record -> new DefaultBatchEvent<>(record, committer))
                    .collect(Collectors.toList());

            CapturingEvents<BatchEvent> capturingEvents = new DefaultCapturingEvents<>(batchEvents, manifest, null);

            registry.get(capturingEvents).capture(capturingEvents);
        }

    }

    /**
     *
     * {@link DestinationSpecificChangeConsumer} triggers the handlers that are annotated with {@link Capturing} with a destination:
     *
     * <pre>
     * {@code @Capturing("pre.inventory.orders") }
     * {@code public void handle(CapturingEvents<BatchEvent> events) { }
     *     //
     * }
     *
     * {@code @Capturing("pre.inventory.products") }
     * {@code public void handle(CapturingEvents<BatchEvent> events) { }
     *     //
     * }
     * </pre>
     * The order of events is based on destination
     *
     */
    private static class DestinationSpecificChangeConsumer implements DestinationChangeConsumer {

        private final CapturingEventsInvokerRegistry<CapturingEvents> registry;

        private DestinationSpecificChangeConsumer(CapturingEventsInvokerRegistry<CapturingEvents> registry) {
            this.registry = registry;
        }

        @Override
        public void handle(EngineManifest manifest, List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer) {
            Map<String, List<BatchEvent>> batchEventsByDestination = records
                    .stream()
                    .map(record -> Map.entry(record.destination(), new DefaultBatchEvent<>(record, committer)))
                    .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

            List<CapturingEvents<BatchEvent>> orderedEvents = batchEventsByDestination
                    .entrySet()
                    .stream()
                    .map(entry -> new DefaultCapturingEvents<>(entry.getValue(), manifest, entry.getKey()))
                    .collect(Collectors.toList());

            orderedEvents
                    .stream()
                    .filter(events -> !registry.get(events).destination().equals(Capturing.ALL))
                    .forEach(events -> registry.get(events).capture(events));
        }

    }

    private final class RoutedQuarkusChangeConsumer implements DestinationChangeConsumer {

        private static final Logger LOGGER = LoggerFactory.getLogger(RoutedQuarkusChangeConsumer.class);
        private final List<DestinationChangeConsumer> consumers;
        private final Optional<CapturingTombstoneEvents> capturingTombstoneEvents;

        RoutedQuarkusChangeConsumer(Optional<CapturingTombstoneEvents> capturingTombstoneEvents,
                                    DestinationChangeConsumer... consumers) {
            this.capturingTombstoneEvents = capturingTombstoneEvents;
            this.consumers = Arrays.stream(consumers).toList();
        }

        @Override
        public void handle(EngineManifest manifest, List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer) {
            LOGGER.trace("receiving events for engine id {}", manifest.id());

            try {
                for (DestinationChangeConsumer consumer : consumers) {
                    consumer.handle(manifest, records, committer);
                }

                committer.markBatchFinished();
            }
            catch (InterruptedException e) {
                throw new DebeziumException(e);
            }

        }

    }

    private static class DefaultCapturingEvents<V> implements CapturingEvents<V> {

        private final List<V> batchEvents;

        private final EngineManifest manifest;
        private final String destination;

        DefaultCapturingEvents(List<V> batchEvents,
                               EngineManifest manifest,
                               String destination) {
            this.batchEvents = batchEvents;
            this.manifest = manifest;
            this.destination = destination;
        }

        @Override
        public List<V> records() {
            return batchEvents;
        }

        @Override
        public String destination() {
            return destination;
        }

        @Override
        public String source() {
            return "NOT_AVAILABLE";
        }

        @Override
        public String engine() {
            return manifest.id();
        }

    }

    private static final class DefaultBatchEvent<K, V, H> implements BatchEvent {

        private final ChangeEvent<K, V> record;

        private final DebeziumEngine.RecordCommitter<ChangeEvent<K, V>> committer;

        DefaultBatchEvent(ChangeEvent<K, V> record,
                          DebeziumEngine.RecordCommitter<ChangeEvent<K, V>> committer) {
            this.record = record;
            this.committer = committer;
        }

        @Override
        public Object key() {
            return record.key();
        }

        @Override
        public Object value() {
            return record.value();
        }

        @Override
        public Integer partition() {
            return record.partition();
        }

        @Override
        public SourceRecord record() {
            return ((EmbeddedEngineChangeEvent<K, V, H>) record).sourceRecord();
        }

        @Override
        public String destination() {
            return record.destination();
        }

        @Override
        public <H> List<Header<H>> headers() {
            return record.headers();
        }

        @Override
        public void commit() {
            try {
                committer.markProcessed(record);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DebeziumException(e);
            }
        }

    }

    /**
     * ChangeConsumer enriched with engine specific information
     */
    private interface DestinationChangeConsumer {
        void handle(EngineManifest manifest,
                    List<ChangeEvent<Object, Object>> records,
                    DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer);
    }

}
