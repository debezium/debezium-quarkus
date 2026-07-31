/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.agroal.deployment;

import io.quarkus.builder.item.MultiBuildItem;

/**
 * A build item emitted for each Debezium connector that is Agroal-compatible, i.e. exposes a JDBC
 * datasource shape (hostname/port/user/database). Non-JDBC connectors (e.g. MongoDB) never produce
 * this item, so the Agroal wiring only ever sees datasources it can actually build.
 */
public final class AgroalConnectorBuildItem extends MultiBuildItem {

    private final String name;
    private final Class<?> connector;

    public AgroalConnectorBuildItem(String name, Class<?> connector) {
        this.name = name;
        this.connector = connector;
    }

    public String name() {
        return name;
    }

    public Class<?> connector() {
        return connector;
    }
}
