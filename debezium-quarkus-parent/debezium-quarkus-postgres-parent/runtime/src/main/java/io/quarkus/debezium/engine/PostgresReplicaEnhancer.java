/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import io.debezium.runtime.Connector;

public class PostgresReplicaEnhancer extends ReplicaConfigurationEnhancer {

    @Override
    public String property() {
        return "slot.name";
    }

    @Override
    public Connector applicableTo() {
        return PostgresEngineProducer.POSTGRES;
    }
}
