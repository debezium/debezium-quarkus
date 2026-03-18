/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.engine.DebeziumEngine;
import io.debezium.runtime.Connector;
import io.debezium.runtime.DebeziumSerialization;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.EngineManifest;

public class DebeziumWithCustomSerialization extends RunnableDebezium {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumWithCustomSerialization.class.getName());
    private final Map<String, String> configuration;
    private final DebeziumEngine<?> engine;
    private final Connector connector;
    private final StateHandler stateHandler;
    private final EngineManifest engineManifest;

    public DebeziumWithCustomSerialization(DebeziumSerialization debeziumSerialization,
                                           Map<String, String> configuration,
                                           DebeziumEngine.ChangeConsumer batchConsumer,
                                           Connector connector,
                                           StateHandler stateHandler,
                                           EngineManifest engineManifest) {
        this.configuration = configuration;
        this.connector = connector;
        this.stateHandler = stateHandler;
        this.engineManifest = engineManifest;
        LOGGER.trace("Creating Debezium with Custom Serialization for engine {}", engineManifest);
        this.engine = DebeziumEngine.create(debeziumSerialization.getKeyFormat(),
                debeziumSerialization.getValueFormat(),
                debeziumSerialization.getHeaderFormat(),
                ConvertingAsyncEngineBuilderFactory.class.getName())
                .using(Configuration.empty()
                        .withSystemProperties(Function.identity())
                        .edit()
                        .with(Configuration.from(configuration))
                        .build().asProperties())
                .using(this.stateHandler.connectorCallback(engineManifest, this))
                .using(this.stateHandler.completionCallback(engineManifest, this))
                .notifying(batchConsumer)
                .build();
    }

    @Override
    public DebeziumEngine.Signaler signaler() {
        return engine.getSignaler();
    }

    @Override
    public Map<String, String> configuration() {
        return configuration;
    }

    @Override
    public DebeziumStatus status() {
        return stateHandler.get(engineManifest);
    }

    @Override
    public Connector connector() {
        return connector;
    }

    @Override
    public EngineManifest manifest() {
        return engineManifest;
    }

    protected void run() {
        this.engine.run();
    }

    protected void close() throws IOException {
        this.engine.close();
    }
}
