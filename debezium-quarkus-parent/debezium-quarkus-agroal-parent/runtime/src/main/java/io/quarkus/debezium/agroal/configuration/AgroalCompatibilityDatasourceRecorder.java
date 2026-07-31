/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.agroal.configuration;

import static io.debezium.config.CommonConnectorConfig.CONNECTOR_CLASS;
import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;
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

            // The datasource list only contains JDBC/Agroal-compatible connectors (filtered upstream in
            // AgroalEngineProcessor), so any entry can be materialized into an Agroal datasource.
            return datasourceList.stream()
                    .filter(datasource -> datasource.connector.getName().equals(configuration.defaultConfiguration().get(CONNECTOR_CLASS)))
                    .findFirst()
                    .map(item -> new AgroalDatasourceConfiguration(
                            configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.HOSTNAME.name()),
                            configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.USER.name()),
                            configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PASSWORD.name()),
                            configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.DATABASE.name()),
                            configuration.defaultConfiguration().get(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PORT.name()),
                            true,
                            "default",
                            AgroalConnectors.databaseKindFor(item.name).orElseThrow()))
                    // No datasource matched the default engine: return an inert configuration (empty dbKind)
                    // so it matches no JDBC engine producer, instead of failing the whole application start.
                    .orElseGet(() -> new AgroalDatasourceConfiguration(null, null, null, null, null, false, "default", ""));
        };
    }

    public record Datasource(String name, Class<?> connector) {
    }

}
