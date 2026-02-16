/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.runtime.Connector;

public class MySqlReplicaEnhancer extends ReplicaConfigurationEnhancer {

    @Override
    public String property() {
        return BinlogConnectorConfig.SERVER_ID.name();
    }

    @Override
    public Connector applicableTo() {
        return MySqlEngineProducer.MYSQL;
    }
}
