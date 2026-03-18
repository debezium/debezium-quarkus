/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

import java.util.List;

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
import io.quarkus.debezium.engine.capture.consumer.ChangeConsumerHandler;
import io.quarkus.debezium.engine.capture.consumer.SourceRecordConsumerHandler;

public class DebeziumFactory {

    private final Instance<DebeziumSerialization<?, ?, ?>> serializations;
    private final StateHandler stateHandler;
    private final SourceRecordConsumerHandler sourceRecordConsumerHandler;
    private final List<DebeziumConfigurationEnhancer> enhancers;
    private final ChangeConsumerHandler changeConsumerHandler;

    @Inject
    public DebeziumFactory(
            Instance<DebeziumConfigurationEnhancer> enhancerInstance,
                           Instance<DebeziumSerialization<?, ?, ?>> serializations,
                           StateHandler stateHandler,
                           SourceRecordConsumerHandler sourceRecordConsumerHandler,
                           ChangeConsumerHandler changeConsumerHandler) {
        this.serializations = serializations;
        this.stateHandler = stateHandler;
        this.sourceRecordConsumerHandler = sourceRecordConsumerHandler;
        this.enhancers = enhancerInstance
                .stream()
                .toList();
        this.changeConsumerHandler = changeConsumerHandler;
    }

    public Debezium get(Connector connector, MultiEngineConfiguration engine) {

        if (serializations.isResolvable() && changeConsumerHandler != null) {
            EngineManifest engineManifest = new EngineManifest(engine.engineId());

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
                    changeConsumerHandler.get(engineManifest),
                    connector,
                    stateHandler,
                    engineManifest);
        }

        EngineManifest engineManifest = new EngineManifest(engine.engineId());
        ComposeConfigurationEnhancer enhancer = new ComposeConfigurationEnhancer(engine.configuration(),
                enhancers.stream()
                        .filter(enhancers -> enhancers.applicableTo().equals(connector))
                        .toList());

        if (changeConsumerHandler != null) {
            return new SourceRecordDebezium(
                    engine.configuration(),
                    changeConsumerHandler.get(engineManifest),
                    connector,
                    stateHandler,
                    engineManifest);
        }

        return new SourceRecordDebezium(
                enhancer.get(),
                stateHandler,
                connector,
                sourceRecordConsumerHandler.get(engineManifest),
                engineManifest);
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
