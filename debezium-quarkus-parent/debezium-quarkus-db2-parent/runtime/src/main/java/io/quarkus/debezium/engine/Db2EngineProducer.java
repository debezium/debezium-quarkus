/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.debezium.connector.db2.Db2Connector;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.runtime.Connector;
import io.debezium.runtime.ConnectorProducer;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.quarkus.datasource.common.runtime.DatabaseKind;
import io.quarkus.debezium.agroal.engine.AgroalParser;

public class Db2EngineProducer implements ConnectorProducer {

    public static final Connector DB2 = new Connector(Db2Connector.class.getName());

    private final AgroalParser agroalParser;
    private final DebeziumFactory debeziumFactory;

    public Db2EngineProducer(AgroalParser agroalParser, DebeziumFactory debeziumFactory) {
        this.agroalParser = agroalParser;
        this.debeziumFactory = debeziumFactory;
    }

    @Override
    public DebeziumConnectorRegistry engine(DebeziumEngineConfiguration debeziumEngineConfiguration) {
        Map<String, Supplier<Debezium>> engineSuppliers = agroalParser.parse(debeziumEngineConfiguration, DatabaseKind.DB2, DB2)
                .stream()
                .map(engine -> {
                    // remove unnecessary configuration for db2
                    engine.configuration()
                            .remove(DATABASE_CONFIG_PREFIX + JdbcConfiguration.DATABASE.name());

                    return Map.entry(engine.engineId(), (Supplier<Debezium>) () -> debeziumFactory.get(DB2, engine));
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new RunnableDebeziumConnectorRegistry(DB2, engineSuppliers);
    }
}
