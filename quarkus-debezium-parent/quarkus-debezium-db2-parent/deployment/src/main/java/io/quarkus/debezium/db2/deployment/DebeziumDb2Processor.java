/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.db2.deployment;

import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Db2ConnectorTask;
import io.debezium.connector.db2.Db2SourceInfoStructMaker;
import io.debezium.connector.db2.Module;
import io.debezium.connector.db2.snapshot.lock.ExclusiveSnapshotLock;
import io.debezium.connector.db2.snapshot.lock.NoSnapshotLock;
import io.debezium.connector.db2.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.quarkus.debezium.agroal.configuration.AgroalDatasourceConfiguration;
import io.quarkus.debezium.deployment.QuarkusEngineProcessor;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumExtensionNameBuildItem;
import io.quarkus.debezium.engine.Db2EngineProducer;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;

class DebeziumDb2Processor implements QuarkusEngineProcessor<AgroalDatasourceConfiguration> {

    private static final String DB2 = Module.name();

    @BuildStep
    @Override
    public DebeziumExtensionNameBuildItem debeziumExtensionNameBuildItem() {
        return new DebeziumExtensionNameBuildItem(DB2);
    }

    @Override
    public DebeziumConnectorBuildItem engine() {
        return new DebeziumConnectorBuildItem(DB2, Db2EngineProducer.class);
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    @Override
    public void registerClassesThatAreLoadedThroughReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClassBuildItemBuildProducer) {
        reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(
                SchemaHistory.class,
                KafkaSchemaHistory.class,
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
}
