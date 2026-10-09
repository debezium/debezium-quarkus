/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.heartbeat;

import java.util.Optional;

import jakarta.enterprise.context.Dependent;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.heartbeat.DebeziumHeartbeatFactory;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatConnectionProvider;
import io.debezium.heartbeat.HeartbeatErrorHandler;
import io.debezium.pipeline.DataChangeEvent;
import io.quarkus.arc.Arc;

/**
 * The {@link DebeziumHeartbeatFactory} that Debezium resolves from Quarkus Arc, it supplies the
 * {@link Heartbeat} bean of the application.
 */
@Dependent
public class ArcHeartbeatFactory implements DebeziumHeartbeatFactory {

    @Override
    public Optional<Heartbeat> getHeartbeat(CommonConnectorConfig connectorConfig,
                                            HeartbeatConnectionProvider connectionProvider,
                                            HeartbeatErrorHandler errorHandler,
                                            ChangeEventQueue<DataChangeEvent> queue) {
        return Arc.container()
                .select(Heartbeat.class)
                .stream()
                .findFirst();
    }

}
