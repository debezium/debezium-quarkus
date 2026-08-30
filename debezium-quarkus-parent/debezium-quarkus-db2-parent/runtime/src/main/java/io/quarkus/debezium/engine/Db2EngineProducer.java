/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.debezium.connector.db2.Db2Connector;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.runtime.Connector;
import io.debezium.runtime.ConnectorProducer;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;
import io.quarkus.datasource.common.runtime.DatabaseKind;
import io.quarkus.debezium.agroal.engine.AgroalParser;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;

public class Db2EngineProducer implements ConnectorProducer {

    public static final Connector DB2 = new Connector(Db2Connector.class.getName());

    private final AgroalParser agroalParser;
    private final DebeziumFactory debeziumFactory;

    @Inject
    public Db2EngineProducer(AgroalParser agroalParser, DebeziumFactory debeziumFactory) {
        this.agroalParser = agroalParser;
        this.debeziumFactory = debeziumFactory;
    }

    @Produces
    @Singleton
    @Override
    public DebeziumConnectorRegistry engine(DebeziumEngineRuntimeConfiguration debeziumEngineConfiguration) {
        List<MultiEngineConfiguration> multiEngineConfigurations = agroalParser.parse(debeziumEngineConfiguration, DatabaseKind.DB2, DB2);

        Map<String, Supplier<Debezium>> engineSuppliers = multiEngineConfigurations
                .stream()
                .map(engine -> {
                    // DB2 connector requires 'database.dbname'; remap from the generic 'database.database'
                    // key that AgroalParser provides from the JDBC URL
                    String dbName = engine.configuration()
                            .remove(DATABASE_CONFIG_PREFIX + JdbcConfiguration.DATABASE.name());
                    if (dbName != null) {
                        engine.configuration().put("database.dbname", dbName);
                    }

                    return Map.entry(engine.engineId(), (Supplier<Debezium>) () -> debeziumFactory.get(DB2, engine));
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new RunnableDebeziumConnectorRegistry(DB2, engineSuppliers);
    }
}
