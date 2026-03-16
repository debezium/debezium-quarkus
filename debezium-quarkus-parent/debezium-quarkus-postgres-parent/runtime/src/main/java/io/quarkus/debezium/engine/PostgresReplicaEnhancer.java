/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.HashMap;
import java.util.Map;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.runtime.Connector;

public class PostgresReplicaEnhancer extends ReplicaConfigurationEnhancer {

    @Override
    public String property() {
        return PostgresConnectorConfig.SLOT_NAME.name();
    }

    @Override
    public Map<String, String> additionalValues() {
        HashMap<String, String> additionalValues = new HashMap<>();
        additionalValues.put(PostgresConnectorConfig.DROP_SLOT_ON_STOP.name(), "true");

        return additionalValues;
    }

    @Override
    public Connector applicableTo() {
        return PostgresEngineProducer.POSTGRES;
    }
}
