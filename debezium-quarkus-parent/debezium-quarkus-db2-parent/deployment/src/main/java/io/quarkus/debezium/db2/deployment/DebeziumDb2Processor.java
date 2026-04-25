/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.db2.deployment;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;

import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Db2Connector;
import io.debezium.connector.db2.Db2ConnectorTask;
import io.debezium.connector.db2.Db2SourceInfoStructMaker;
import io.debezium.connector.db2.Module;
import io.debezium.connector.db2.snapshot.lock.ExclusiveSnapshotLock;
import io.debezium.connector.db2.snapshot.lock.NoSnapshotLock;
import io.debezium.connector.db2.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.quarkus.datasource.common.runtime.DataSourceUtil;
import io.quarkus.datasource.common.runtime.DatabaseKind;
import io.quarkus.datasource.deployment.spi.DatasourceStartable;
import io.quarkus.datasource.deployment.spi.DevServicesDatasourceConfigurationHandlerBuildItem;
import io.quarkus.datasource.deployment.spi.DevServicesDatasourceContainerConfig;
import io.quarkus.datasource.deployment.spi.DevServicesDatasourceProvider;
import io.quarkus.datasource.deployment.spi.DevServicesDatasourceProviderBuildItem;
import io.quarkus.debezium.agroal.configuration.AgroalDatasourceConfiguration;
import io.quarkus.debezium.db2.runtime.DebeziumDb2CdcInstrumentationRecorder;
import io.quarkus.debezium.deployment.QuarkusEngineProcessor;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumExtensionNameBuildItem;
import io.quarkus.debezium.engine.Db2EngineProducer;
import io.quarkus.deployment.Feature;
import io.quarkus.deployment.IsNormal;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.DevServicesComposeProjectBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageConfigBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.dev.devservices.DevServicesConfig;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;
import io.quarkus.runtime.LaunchMode;

class DebeziumDb2Processor implements QuarkusEngineProcessor<AgroalDatasourceConfiguration> {

    private static final String DB2 = Module.name();

    @BuildStep
    @Override
    public DebeziumExtensionNameBuildItem debeziumExtensionNameBuildItem() {
        return new DebeziumExtensionNameBuildItem(DB2);
    }

    @BuildStep
    @Override
    public DebeziumConnectorBuildItem engine() {
        return new DebeziumConnectorBuildItem(DB2, Db2EngineProducer.class, Db2Connector.class);
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    NativeImageConfigBuildItem nativeImageConfiguration() {
        // The DB2 JDBC driver has been updated with conditional checks for the
        // "QuarkusWithJcc" system property which will no-op some code paths that
        // are not needed for T4 JDBC usage and are incompatible with native mode,
        // including setting TCP_KEEPIDLE which is unsupported in GraalVM native image.
        return NativeImageConfigBuildItem.builder()
                .addNativeImageSystemProperty("QuarkusWithJcc", "true")
                .build();
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    @Override
    public void registerClassesThatAreLoadedThroughReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClassBuildItemBuildProducer) {
        reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(
                SchemaHistory.class,
                KafkaSchemaHistory.class,
                Db2Connector.class,
                Db2Connection.class,
                Db2SourceInfoStructMaker.class,
                Db2ConnectorTask.class,
                NoSnapshotLock.class,
                ExclusiveSnapshotLock.class,
                SelectAllSnapshotQuery.class)
                .reason(getClass().getName())
                .build());
    }

    @Override
    public Class<AgroalDatasourceConfiguration> quarkusDatasourceConfiguration() {
        return AgroalDatasourceConfiguration.class;
    }

    @BuildStep
    DevServicesDatasourceConfigurationHandlerBuildItem devDbHandler() {
        return DevServicesDatasourceConfigurationHandlerBuildItem.jdbc(DatabaseKind.DB2);
    }

    @BuildStep(onlyIfNot = IsNormal.class, onlyIf = DevServicesConfig.Enabled.class)
    void devservices(BuildProducer<DevServicesDatasourceProviderBuildItem> devServicesProducer, DebeziumEngineConfiguration debeziumEngineConfiguration) {

        var db2 = debeziumEngineConfiguration.devservices().get("db2");
        var allServices = debeziumEngineConfiguration.devservices().get("*");

        if (db2 != null && !db2.enabled().orElse(true)) {
            return;
        }

        if (allServices != null && !allServices.enabled().orElse(true)) {
            return;
        }

        devServicesProducer.produce(new DevServicesDatasourceProviderBuildItem(DatabaseKind.DB2, new DevServicesDatasourceProvider() {

            @Override
            public String getFeature() {
                return Feature.JDBC_DB2.getName();
            }

            @Override
            public DatasourceStartable createDatasourceStartable(Optional<String> username, Optional<String> password, String datasourceName,
                                                                 DevServicesDatasourceContainerConfig containerConfig, LaunchMode launchMode,
                                                                 boolean useSharedNetwork, Optional<Duration> startupTimeout) {

                String effectiveUsername = containerConfig.getUsername().orElse(username.orElse(DebeziumDb2Container.USER));
                String effectivePassword = containerConfig.getPassword().orElse(password.orElse(DebeziumDb2Container.PASSWORD));
                String effectiveDbName = containerConfig.getDbName().orElse(DataSourceUtil.isDefault(datasourceName) ? DebeziumDb2Container.DATABASE : datasourceName);

                return new DebeziumDb2Container(effectiveUsername, effectivePassword, effectiveDbName);
            }

            @Override
            public Optional<RunningDevServicesDatasource> findRunningComposeDatasource(LaunchMode launchMode, boolean useSharedNetwork,
                                                                                       DevServicesDatasourceContainerConfig containerConfig,
                                                                                       DevServicesComposeProjectBuildItem composeProjectBuildItem) {
                return Optional.empty();
            }
        }));
    }

    @BuildStep(onlyIfNot = IsNormal.class, onlyIf = DevServicesConfig.Enabled.class)
    @Record(ExecutionTime.RUNTIME_INIT)
    void recordCdcSetup(DebeziumDb2CdcInstrumentationRecorder recorder) {
        recorder.setupCdcRegistration(60);
    }

    private static class DebeziumDb2Container extends GenericContainer<DebeziumDb2Container> implements DatasourceStartable {

        public static final String USER = "db2inst1";
        public static final String PASSWORD = "dbz";
        public static final String DATABASE = "TESTDB";
        public static final String LOCALHOST = "127.0.0.1";
        public static final int DB2_PORT = 50000;
        public static final String SERVICE_NAME = "debezium-devservices-db2";

        private final String username;
        private final String password;
        private final String database;

        private DebeziumDb2Container(String username, String password, String database) {
            super(new ImageFromDockerfile("debezium-db2-cdc", false)
                    .withFileFromClasspath("Dockerfile", "db2-cdc-docker/Dockerfile")
                    .withFileFromClasspath("asncdc.c", "db2-cdc-docker/asncdc.c")
                    .withFileFromClasspath("asncdc_UDF.sql", "db2-cdc-docker/asncdc_UDF.sql")
                    .withFileFromClasspath("asncdcaddremove.sql", "db2-cdc-docker/asncdcaddremove.sql")
                    .withFileFromClasspath("asncdctables.sql", "db2-cdc-docker/asncdctables.sql")
                    .withFileFromClasspath("cdcsetup.sh", "db2-cdc-docker/cdcsetup.sh")
                    .withFileFromClasspath("dbsetup.sh", "db2-cdc-docker/dbsetup.sh"));
            this.username = username;
            this.password = password;
            this.database = database;

            withPrivilegedMode(true);
            withExposedPorts(DB2_PORT);
            withEnv(Map.of("LICENSE", "accept", "DBNAME", database, "DB2INSTANCE", USER, "DB2INST1_PASSWORD", password, "ARCHIVE_LOGS", "true", "AUTOCONFIG", "false"));
            waitingFor(new LogMessageWaitStrategy().withRegEx(".*CDC setup completed successfully.*\\n").withStartupTimeout(Duration.ofMinutes(10)));
        }

        @Override
        public String getConnectionInfo() {
            return getEffectiveJdbcUrl();
        }

        @Override
        public String getEffectiveJdbcUrl() {
            return "jdbc:db2://" + LOCALHOST + ":" + getMappedPort(DB2_PORT) + "/" + database;
        }

        @Override
        public String getReactiveUrl() {
            return getEffectiveJdbcUrl().replaceFirst("jdbc:", "vertx-reactive:");
        }

        @Override
        public String getUsername() {
            return username;
        }

        @Override
        public String getPassword() {
            return password;
        }

        @Override
        public void close() {
            super.close();
        }
    }
}
