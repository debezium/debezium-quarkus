/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

import static io.debezium.config.CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS;
import static io.debezium.embedded.EmbeddedEngineConfig.CONNECTOR_CLASS;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.runtime.Connector;
import io.debezium.runtime.ConnectorProducer;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.runtime.configuration.QuarkusDatasourceConfiguration;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser;
import io.quarkus.debezium.configuration.DebeziumConfigurationEngineParser.MultiEngineConfiguration;
import io.quarkus.debezium.configuration.MongoDbDatasourceConfiguration;
import io.quarkus.debezium.configuration.MultiEngineMongoDbDatasourceConfiguration;
import io.quarkus.debezium.notification.QuarkusNotificationChannel;

public class MongoDbEngineProducer implements ConnectorProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbEngineProducer.class);
    public static final Connector MONGODB = new Connector(MongoDbConnector.class.getName());
    private final Map<String, MongoDbDatasourceConfiguration> quarkusDatasourceConfigurations;
    private DebeziumFactory debeziumFactory;
    private final QuarkusNotificationChannel channel;
    private final DebeziumConfigurationEngineParser engineParser = new DebeziumConfigurationEngineParser();

    @Inject
    public MongoDbEngineProducer(MultiEngineMongoDbDatasourceConfiguration multiEngineMongoDbDatasourceConfiguration,
                                 QuarkusNotificationChannel channel,
                                 DebeziumFactory debeziumFactory) {
        this.channel = channel;
        this.quarkusDatasourceConfigurations = multiEngineMongoDbDatasourceConfiguration.get();
        this.debeziumFactory = debeziumFactory;
    }

    @Produces
    @Singleton
    @Override
    public DebeziumConnectorRegistry engine(DebeziumEngineConfiguration debeziumEngineConfiguration) {
        List<MultiEngineConfiguration> multiEngineConfigurations = engineParser.parse(debeziumEngineConfiguration);

        /*
         * enrich Quarkus-like debezium configuration with quarkus datasource configuration
         */
        List<MultiEngineConfiguration> enrichedMultiEngineConfigurations = multiEngineConfigurations
                .stream()
                .map(engine -> merge(engine, quarkusDatasourceConfigurations))
                .toList();

        return new DebeziumConnectorRegistry() {
            private final Map<String, Debezium> engines = enrichedMultiEngineConfigurations
                    .stream()
                    .map(engine -> Map.entry(engine.engineId(), debeziumFactory.get(MONGODB, engine)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            private final Map<String, DebeziumRunner> runners = new ConcurrentHashMap<>();

            @Override
            public Connector connector() {
                return MONGODB;
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
                }
                catch (Exception e) {
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
                }
                catch (Exception e) {
                    LOGGER.error("Failed to shutdown engine for manifest: {}", manifest.id(), e);
                }
            }
        };
    }

    private MultiEngineConfiguration merge(MultiEngineConfiguration engine, Map<String, ? extends QuarkusDatasourceConfiguration> configurations) {
        HashMap<String, String> mutableConfigurations = new HashMap<>(engine.configuration());

        mutableConfigurations.compute(NOTIFICATION_ENABLED_CHANNELS.name(),
                (key, value) -> value == null ? channel.name() : value.concat("," + channel.name()));

        mutableConfigurations.putAll(getQuarkusDatasourceConfigurationByEngineId(engine.engineId(), configurations).asDebezium());
        mutableConfigurations.put(CONNECTOR_CLASS.name(), MONGODB.name());

        return new MultiEngineConfiguration(engine.engineId(), mutableConfigurations);
    }

    private QuarkusDatasourceConfiguration getQuarkusDatasourceConfigurationByEngineId(String engineId,
                                                                                       Map<String, ? extends QuarkusDatasourceConfiguration> configurations) {
        QuarkusDatasourceConfiguration configuration = configurations.get(engineId);

        if (configuration == null) {
            throw new IllegalArgumentException("No datasource configuration found for engine " + engineId);
        }

        return configuration;
    }
}
