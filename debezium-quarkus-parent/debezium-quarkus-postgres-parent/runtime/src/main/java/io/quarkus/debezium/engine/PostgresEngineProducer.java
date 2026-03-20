/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.runtime.Connector;
import io.debezium.runtime.ConnectorProducer;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.quarkus.datasource.common.runtime.DatabaseKind;
import io.quarkus.debezium.agroal.engine.AgroalParser;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;

@ApplicationScoped
public class PostgresEngineProducer implements ConnectorProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresEngineProducer.class);

    public static final Connector POSTGRES = new Connector(PostgresConnector.class.getName());

    private final AgroalParser agroalParser;
    private final DebeziumFactory debeziumFactory;

    @Inject
    public PostgresEngineProducer(AgroalParser agroalParser, DebeziumFactory debeziumFactory) {
        this.agroalParser = agroalParser;
        this.debeziumFactory = debeziumFactory;
    }

    @Produces
    @Singleton
    public DebeziumConnectorRegistry engine(DebeziumEngineConfiguration debeziumEngineConfiguration) {
        List<MultiEngineConfiguration> multiEngineConfigurations = agroalParser.parse(debeziumEngineConfiguration, DatabaseKind.POSTGRESQL, POSTGRES);

        return new DebeziumConnectorRegistry() {
            private final Map<String, Debezium> engines = multiEngineConfigurations
                    .stream()
                    .map(engine -> Map.entry(engine.engineId(), debeziumFactory.get(POSTGRES, engine)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            private final Map<String, DebeziumRunner> runners = new ConcurrentHashMap<>();

            @Override
            public Connector connector() {
                return POSTGRES;
            }

            @Override
            public Debezium get(EngineManifest manifest) {
                return engines.get(manifest.id());
            }

            @Override
            public List<Debezium> engines() {
                return engines.values().stream().toList();
            }

            @Override
            public void start(EngineManifest manifest) {
                Debezium debezium = engines.get(manifest.id());

                if (debezium == null) {
                    throw new IllegalArgumentException("No engine found for manifest: " + manifest.id());
                }

                DebeziumRunner runner = new DebeziumRunner(
                        DebeziumThreadHandler.getThreadFactory(debezium), debezium);

                DebeziumRunner existing = runners.putIfAbsent(manifest.id(), runner);
                if (existing != null) {
                    LOGGER.warn("Engine already running for manifest: {}", manifest.id());
                    return;
                }

                try {
                    runner.start();
                } catch (Exception e) {
                    runners.remove(manifest.id());
                    LOGGER.error("Failed to start engine for manifest: {}", manifest.id(), e);
                    throw e;
                }
            }

            @Override
            public void stop(EngineManifest manifest) {
                DebeziumRunner runner = runners.remove(manifest.id());
                if (runner == null) {
                    LOGGER.warn("No running engine found for manifest: {}", manifest.id());
                    return;
                }

                try {
                    runner.shutdown();
                } catch (Exception e) {
                    LOGGER.error("Failed to shutdown engine for manifest: {}", manifest.id(), e);
                }
            }
        };
    }

}
