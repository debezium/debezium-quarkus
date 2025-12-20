/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

import jakarta.inject.Inject;

import io.debezium.runtime.Connector;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.EngineManifest;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;
import io.quarkus.debezium.engine.capture.consumer.SourceRecordConsumerHandler;

public class DebeziumFactory {

    private final StateHandler stateHandler;
    private final SourceRecordConsumerHandler sourceRecordConsumerHandler;

    @Inject
    public DebeziumFactory(StateHandler stateHandler,
                           SourceRecordConsumerHandler sourceRecordConsumerHandler) {
        this.stateHandler = stateHandler;
        this.sourceRecordConsumerHandler = sourceRecordConsumerHandler;
    }

    public Debezium get(Connector connector, MultiEngineConfiguration engine) {
        EngineManifest engineManifest = new EngineManifest(engine.engineId());

        return new SourceRecordDebezium(
                engine.configuration(),
                stateHandler,
                connector,
                sourceRecordConsumerHandler.get(engineManifest),
                engineManifest);
    }
}
