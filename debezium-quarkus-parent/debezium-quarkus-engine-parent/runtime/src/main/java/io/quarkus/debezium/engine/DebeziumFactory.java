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
import io.quarkus.debezium.engine.capture.consumer.SourceRecordConsumerHandler;

public class DebeziumFactory {

    private final Instance<DebeziumSerialization> serialization;
    private final StateHandler stateHandler;
    private final SourceRecordConsumerHandler sourceRecordConsumerHandler;

    @Inject
    public DebeziumFactory(
                           Instance<DebeziumSerialization> serialization,
                           StateHandler stateHandler,
                           SourceRecordConsumerHandler sourceRecordConsumerHandler) {
        this.serialization = serialization;
        this.stateHandler = stateHandler;
        this.sourceRecordConsumerHandler = sourceRecordConsumerHandler;
    }

    public Debezium get(Connector connector, MultiEngineConfiguration engine) {
        if (serialization.isResolvable()) {
            throw new DebeziumException("not implemented yet engine with configurable serialization");
        }

        EngineManifest engineManifest = new EngineManifest(engine.engineId());

        return new SourceRecordDebezium(
                engine.configuration(),
                stateHandler,
                connector,
                sourceRecordConsumerHandler.get(engineManifest),
                engineManifest);
    }
}
