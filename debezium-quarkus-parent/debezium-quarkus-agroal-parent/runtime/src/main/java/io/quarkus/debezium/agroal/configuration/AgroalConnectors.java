/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.agroal.configuration;

import java.util.Optional;

import io.quarkus.datasource.common.runtime.DatabaseKind;

/**
 * Maps Debezium connector names to the corresponding Quarkus {@link DatabaseKind}. Only connectors
 * that expose a JDBC/Agroal datasource shape (hostname/port/user/database) are recognised; anything
 * else (e.g. MongoDB) is not Agroal-compatible and yields {@link Optional#empty()}.
 * <p>
 * Kept as a shared helper so the same mapping is used both at build time, to decide which connectors
 * become an Agroal datasource, and at runtime when the datasource configuration is materialized.
 */
public final class AgroalConnectors {

    private AgroalConnectors() {
    }

    /**
     * @param connectorName the Debezium connector name (e.g. {@code "postgresql"}, {@code "mongodb"})
     * @return the matching {@link DatabaseKind} value, or {@link Optional#empty()} if the connector is
     *         not JDBC/Agroal-compatible
     */
    public static Optional<String> databaseKindFor(String connectorName) {
        return switch (connectorName) {
            case "postgresql" -> Optional.of(DatabaseKind.POSTGRESQL);
            case "mysql" -> Optional.of(DatabaseKind.MYSQL);
            case "oracle" -> Optional.of(DatabaseKind.ORACLE);
            case "sqlserver" -> Optional.of(DatabaseKind.MSSQL);
            case "mariadb" -> Optional.of(DatabaseKind.MARIADB);
            case "db2" -> Optional.of(DatabaseKind.DB2);
            default -> Optional.empty();
        };
    }

    /**
     * @param connectorName the Debezium connector name
     * @return {@code true} if the connector exposes a JDBC/Agroal datasource shape
     */
    public static boolean isAgroalCompatible(String connectorName) {
        return databaseKindFor(connectorName).isPresent();
    }
}
