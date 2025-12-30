/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.postgres.deployment;

import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorTask;
import io.debezium.connector.postgresql.PostgresSourceInfoStructMaker;
import io.debezium.connector.postgresql.snapshot.lock.NoSnapshotLock;
import io.debezium.connector.postgresql.snapshot.lock.SharedSnapshotLock;
import io.debezium.connector.postgresql.snapshot.query.SelectAllSnapshotQuery;
import io.quarkus.datasource.deployment.spi.DevServicesDatasourceResultBuildItem;
import io.quarkus.debezium.agroal.configuration.AgroalDatasourceConfiguration;
import io.quarkus.debezium.deployment.QuarkusEngineProcessor;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumExtensionNameBuildItem;
import io.quarkus.debezium.engine.PostgresEngineProducer;
import io.quarkus.deployment.IsNormal;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.DevServicesResultBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.dev.devservices.DevServicesConfig;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;

public class PostgresEngineProcessor implements QuarkusEngineProcessor<AgroalDatasourceConfiguration> {

    public static final String POSTGRESQL = Module.name();

    @BuildStep
    @Override
    public DebeziumExtensionNameBuildItem debeziumExtensionNameBuildItem() {
        return new DebeziumExtensionNameBuildItem(POSTGRESQL);
    }

    @BuildStep
    @Override
    public DebeziumConnectorBuildItem engine() {
        return new DebeziumConnectorBuildItem(POSTGRESQL, PostgresEngineProducer.class);
    }

    @BuildStep(onlyIfNot = IsNormal.class, onlyIf = DevServicesConfig.Enabled.class)
    void devServices(BuildProducer<DevServicesResultBuildItem> devServicesProducer,
                     DevServicesDatasourceResultBuildItem devServicesDatasourceResultBuildItem) {
        DevServicesDatasourceResultBuildItem.DbResult datasource = devServicesDatasourceResultBuildItem.getDefaultDatasource();

        if (datasource == null) {
            return;
        }

        if (!datasource.getDbType().equals(POSTGRESQL)) {
            return;
        }

        devServicesProducer.produce(new DevServicesResultBuildItem("debezium-postgres",
                "debezium",
                datasource.getConfigProperties()));
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    @Override
    public void registerClassesThatAreLoadedThroughReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClassBuildItemBuildProducer) {
        reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(
                PostgresConnector.class,
                PostgresSourceInfoStructMaker.class,
                PostgresConnectorTask.class,
                NoSnapshotLock.class,
                SharedSnapshotLock.class,
                SelectAllSnapshotQuery.class)
                .reason(getClass().getName())
                .build());
    }

    @Override
    public Class<AgroalDatasourceConfiguration> quarkusDatasourceConfiguration() {
        return AgroalDatasourceConfiguration.class;
    }

}
