/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.debezium.connector.oracle.OracleConnector;
import io.debezium.runtime.Connector;
import io.debezium.runtime.ConnectorProducer;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.quarkus.datasource.common.runtime.DatabaseKind;
import io.quarkus.debezium.agroal.engine.AgroalParser;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;

public class OracleEngineProducer implements ConnectorProducer {

    public static final Connector ORACLE = new Connector(OracleConnector.class.getName());

    private final AgroalParser agroalParser;
    private final DebeziumFactory debeziumFactory;

    @Inject
    public OracleEngineProducer(AgroalParser agroalParser, DebeziumFactory debeziumFactory) {
        this.agroalParser = agroalParser;
        this.debeziumFactory = debeziumFactory;
    }

    @Produces
    @Singleton
    @Override
    public DebeziumConnectorRegistry engine(DebeziumEngineConfiguration debeziumEngineConfiguration) {
        final List<MultiEngineConfiguration> multiEngineConfigurations = agroalParser.parse(
                debeziumEngineConfiguration, DatabaseKind.ORACLE, ORACLE);

        Map<String, Supplier<Debezium>> engineSuppliers = multiEngineConfigurations
                .stream()
                .map(engine -> Map.entry(engine.engineId(), (Supplier<Debezium>) () -> debeziumFactory.get(ORACLE, engine)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new RunnableDebeziumConnectorRegistry(ORACLE, engineSuppliers);
    }
}
