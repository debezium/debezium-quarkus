/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.agroal.configuration;

import static io.debezium.config.CommonConnectorConfig.CONNECTOR_CLASS;
import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;
import io.quarkus.datasource.common.runtime.DatabaseKind;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;

/**
 * A Quarkus {@link Recorder} that bridges Debezium's runtime database configuration into an
 * {@link AgroalDatasourceConfiguration}, enabling Agroal-based datasource setup from Debezium
 * connector properties.
 *
 * @see AgroalDatasourceConfiguration
 * @see DebeziumEngineRuntimeConfiguration
 */
@Recorder
public class AgroalCompatibilityDatasourceRecorder {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgroalCompatibilityDatasourceRecorder.class);
    private final RuntimeValue<DebeziumEngineRuntimeConfiguration> debeziumEngineConfigurationRuntimeValue;

    public AgroalCompatibilityDatasourceRecorder(RuntimeValue<DebeziumEngineRuntimeConfiguration> debeziumEngineConfigurationRuntimeValue) {
        this.debeziumEngineConfigurationRuntimeValue = debeziumEngineConfigurationRuntimeValue;
    }

    public Supplier<AgroalDatasourceConfiguration> get(List<Datasource> datasourceList) {
        return () -> {
            DebeziumEngineRuntimeConfiguration configuration = debeziumEngineConfigurationRuntimeValue.getValue();
            String connectors = datasourceList.stream().map(Datasource::connector).map(Class::getName).collect(
                    Collectors.joining(","));

            LOGGER.info("found Agroal compatible connectors: {}", connectors);
            LOGGER.info("Agroal default engine: {}", configuration.defaultConfiguration().get(CONNECTOR_CLASS));

            return datasourceList.stream()
                    .filter(datasource -> datasource.connector.getName().equals(configuration.defaultConfiguration().get(CONNECTOR_CLASS)))
                    // Skip non-JDBC connectors (e.g. MongoDB): they have no Agroal datasource shape
                    // (hostname/port/user/database) and must not be routed through the JDBC path.
                    .filter(datasource -> convert(datasource.name).isPresent())
                    .findFirst()
                    .map(item -> new AgroalDatasourceConfiguration(
                            configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.HOSTNAME.name()),
                            configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.USER.name()),
                            configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PASSWORD.name()),
                            configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.DATABASE.name()),
                            configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PORT.name()),
                            true,
                            "default",
                            convert(item.name).orElseThrow()))
                    // When the default engine is not a JDBC connector (e.g. MongoDB), there is no
                    // Agroal datasource to build. Return an inert configuration (empty dbKind) so it
                    // matches no JDBC engine producer, instead of failing the whole application start.
                    .orElseGet(() -> new AgroalDatasourceConfiguration(null, null, null, null, null, false, "default", ""));
        };
    }

    /**
     * Converts a Debezium connector name to the corresponding Quarkus {@link DatabaseKind} constant.
     *
     * @param name the connector name
     * @return the matching {@link DatabaseKind} value, or {@link Optional#empty()} if the connector
     *         is not a JDBC/Agroal-compatible one (e.g. MongoDB)
     */
    private Optional<String> convert(String name) {
        return switch (name) {
            case "postgresql" -> Optional.of(DatabaseKind.POSTGRESQL);
            case "mysql" -> Optional.of(DatabaseKind.MYSQL);
            case "oracle" -> Optional.of(DatabaseKind.ORACLE);
            case "sqlserver" -> Optional.of(DatabaseKind.MSSQL);
            case "mariadb" -> Optional.of(DatabaseKind.MARIADB);
            case "db2" -> Optional.of(DatabaseKind.DB2);
            default -> Optional.empty();
        };
    }

    public record Datasource(String name, Class<?> connector) {
    }

}
