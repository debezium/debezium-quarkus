/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.debezium.DebeziumException;
import io.debezium.runtime.Connector;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumSerialization;
import io.debezium.runtime.EngineManifest;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;
import io.quarkus.debezium.engine.capture.consumer.ChangeConsumerHandler;
import io.quarkus.debezium.engine.capture.consumer.SourceRecordConsumerHandler;

public class DebeziumFactory {

    private final Instance<DebeziumSerialization> serialization;
    private final StateHandler stateHandler;
    private final SourceRecordConsumerHandler sourceRecordConsumerHandler;
    private final ChangeConsumerHandler changeConsumerHandler;

    @Inject
    public DebeziumFactory(
                           Instance<DebeziumSerialization> serialization,
                           StateHandler stateHandler,
                           SourceRecordConsumerHandler sourceRecordConsumerHandler,
                           ChangeConsumerHandler changeConsumerHandler) {
        this.serialization = serialization;
        this.stateHandler = stateHandler;
        this.sourceRecordConsumerHandler = sourceRecordConsumerHandler;
        this.changeConsumerHandler = changeConsumerHandler;
    }

    public Debezium get(Connector connector, MultiEngineConfiguration engine) {
        if (serialization.isResolvable()) {
            throw new DebeziumException("not implemented yet engine with configurable serialization");
        }

        EngineManifest engineManifest = new EngineManifest(engine.engineId());

        if (changeConsumerHandler != null) {
            return new SourceRecordDebezium(
                    engine.configuration(),
                    changeConsumerHandler.get(engineManifest),
                    connector,
                    stateHandler,
                    engineManifest);
        }

        return new SourceRecordDebezium(
                engine.configuration(),
                stateHandler,
                connector,
                sourceRecordConsumerHandler.get(engineManifest),
                engineManifest);
    }
}
