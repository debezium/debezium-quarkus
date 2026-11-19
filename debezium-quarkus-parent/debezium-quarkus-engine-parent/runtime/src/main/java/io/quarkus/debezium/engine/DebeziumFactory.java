/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.debezium.runtime.Connector;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumSerialization;
import io.debezium.runtime.EngineManifest;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;
import io.quarkus.debezium.engine.capture.consumer.ChangeConsumerFactory;
import io.quarkus.debezium.engine.capture.consumer.SourceRecordConsumerHandler;
import io.quarkus.debezium.engine.schemachange.SchemaChangeInvoker;
import io.quarkus.debezium.engine.schemachange.SchemaChangeInvokers;
import io.quarkus.debezium.engine.shutdown.ShutdownInvoker;

public class DebeziumFactory {

    private final Instance<DebeziumSerialization<?, ?, ?>> serializations;
    private final StateHandler stateHandler;
    private final SourceRecordConsumerHandler sourceRecordConsumerHandler;
    private final List<DebeziumConfigurationEnhancer> enhancers;
    private final ChangeConsumerFactory changeConsumerHandler;
    private final Instance<ShutdownInvoker> shutdownInvokers;
    private final Instance<SchemaChangeInvoker> schemaChangeInvokers;

    @Inject
    public DebeziumFactory(
                           Instance<DebeziumConfigurationEnhancer> enhancerInstance,
                           Instance<DebeziumSerialization<?, ?, ?>> serializations,
                           StateHandler stateHandler,
                           SourceRecordConsumerHandler sourceRecordConsumerHandler,
                           ChangeConsumerFactory changeConsumerHandler,
                           Instance<ShutdownInvoker> shutdownInvokers,
                           Instance<SchemaChangeInvoker> schemaChangeInvokers) {
        this.serializations = serializations;
        this.stateHandler = stateHandler;
        this.sourceRecordConsumerHandler = sourceRecordConsumerHandler;
        this.enhancers = enhancerInstance
                .stream()
                .toList();
        this.changeConsumerHandler = changeConsumerHandler;
        this.shutdownInvokers = shutdownInvokers;
        this.schemaChangeInvokers = schemaChangeInvokers;
    }

    public Debezium get(Connector connector, MultiEngineConfiguration engine) {
        List<ShutdownInvoker> shutdown = shutdownInvokers.stream().toList();
        EngineManifest engineManifest = new EngineManifest(engine.engineId());
        List<SchemaChangeInvoker> schemaChange = SchemaChangeInvokers.select(engineManifest, engine.configuration(), schemaChangeInvokers.stream().toList());

        if (serializations.isResolvable() && changeConsumerHandler != null) {
            List<DebeziumSerialization<?, ?, ?>> serializations = this.serializations
                    .stream()
                    .filter(serialization -> serialization.getEngineId().equals(engine.engineId()))
                    .toList();

            if (serializations.size() > 1) {
                throw new RuntimeException("Multiple serialization configurations were found for the engine " + engine.engineId());
            }

            return new DebeziumWithCustomSerialization(
                    serializations.getFirst(),
                    engine.configuration(),
                    SchemaChangeInvokers.apply(changeConsumerHandler.get(engineManifest), engineManifest, schemaChange),
                    connector,
                    stateHandler,
                    engineManifest,
                    shutdown);
        }

        ComposeConfigurationEnhancer enhancer = new ComposeConfigurationEnhancer(engine.configuration(),
                enhancers.stream()
                        .filter(enhancers -> enhancers.applicableTo().equals(connector))
                        .toList());

        if (changeConsumerHandler != null) {
            return new SourceRecordDebezium(
                    engine.configuration(),
                    SchemaChangeInvokers.apply(changeConsumerHandler.get(engineManifest), engineManifest, schemaChange),
                    connector,
                    stateHandler,
                    engineManifest,
                    shutdown);
        }

        return new SourceRecordDebezium(
                enhancer.get(),
                stateHandler,
                connector,
                SchemaChangeInvokers.apply(sourceRecordConsumerHandler.get(engineManifest), engineManifest, schemaChange),
                engineManifest,
                shutdown);
    }

    private class ComposeConfigurationEnhancer {
        private final Map<String, String> base;
        private final List<DebeziumConfigurationEnhancer> enhancers;

        ComposeConfigurationEnhancer(Map<String, String> base,
                                     List<DebeziumConfigurationEnhancer> enhancers) {
            this.base = base;
            this.enhancers = enhancers;
        }

        Map<String, String> get() {
            Map<String, String> entries = enhancers
                    .stream()
                    .map(enhancer -> enhancer.apply(base))
                    .flatMap(a -> a.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            base.putAll(entries);

            return base;
        }
    }
}
