/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.agroal.configuration;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

import java.util.function.Supplier;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;
import io.quarkus.datasource.common.runtime.DatabaseKind;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class AgroalCompatibilityDatasourceRecorder {

    private final RuntimeValue<DebeziumEngineRuntimeConfiguration> debeziumEngineConfigurationRuntimeValue;

    public AgroalCompatibilityDatasourceRecorder(RuntimeValue<DebeziumEngineRuntimeConfiguration> debeziumEngineConfigurationRuntimeValue) {
        this.debeziumEngineConfigurationRuntimeValue = debeziumEngineConfigurationRuntimeValue;
    }

    public Supplier<AgroalDatasourceConfiguration> get(String name) {
        return () -> {
            DebeziumEngineRuntimeConfiguration configuration = debeziumEngineConfigurationRuntimeValue.getValue();
            return new AgroalDatasourceConfiguration(
                    configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.HOSTNAME.name()),
                    configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.USER.name()),
                    configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PASSWORD.name()),
                    configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.DATABASE.name()),
                    configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PORT.name()),
                    true,
                    "default",
                    convert(name));
        };
    }

    private String convert(String name) {
        return switch (name) {
            case "postgresql" -> DatabaseKind.POSTGRESQL;
            case "mysql" -> DatabaseKind.MYSQL;
            case "oracle" -> DatabaseKind.ORACLE;
            case "sqlserver" -> DatabaseKind.MSSQL;
            case "mariadb" -> DatabaseKind.MARIADB;
            case "db2" -> DatabaseKind.DB2;
            default -> throw new IllegalStateException("Unexpected value: " + name);
        };
    }
}
