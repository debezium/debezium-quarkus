/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.oracle.deployment;

import io.debezium.connector.oracle.Module;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorTask;
import io.debezium.connector.oracle.OracleSourceInfoStructMaker;
import io.debezium.connector.oracle.StreamingAdapter;
import io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerAdapter;
import io.debezium.connector.oracle.logminer.unbuffered.UnbufferedLogMinerAdapter;
import io.debezium.connector.oracle.olr.OpenLogReplicatorAdapter;
import io.debezium.connector.oracle.snapshot.lock.NoSnapshotLock;
import io.debezium.connector.oracle.snapshot.lock.SharedSnapshotLock;
import io.debezium.connector.oracle.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import io.quarkus.debezium.agroal.configuration.AgroalDatasourceConfiguration;
import io.quarkus.debezium.deployment.QuarkusEngineProcessor;
import io.quarkus.debezium.deployment.items.DebeziumConnectorBuildItem;
import io.quarkus.debezium.deployment.items.DebeziumExtensionNameBuildItem;
import io.quarkus.debezium.engine.OracleEngineProducer;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.nativeimage.NativeImageConfigBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.pkg.steps.NativeOrNativeSourcesBuild;

public class DebeziumOracleProcessor implements QuarkusEngineProcessor<AgroalDatasourceConfiguration> {

    private static final String ORACLE = Module.name();

    @BuildStep
    @Override
    public DebeziumExtensionNameBuildItem debeziumExtensionNameBuildItem() {
        return new DebeziumExtensionNameBuildItem(ORACLE);
    }

    @BuildStep
    @Override
    public DebeziumConnectorBuildItem engine() {
        return new DebeziumConnectorBuildItem(ORACLE, OracleEngineProducer.class);
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    @Override
    public void registerClassesThatAreLoadedThroughReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClassBuildItemBuildProducer) {
        reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(
                SchemaHistory.class,
                KafkaSchemaHistory.class,
                OracleConnection.class,
                OracleSourceInfoStructMaker.class,
                OracleConnectorTask.class,
                OracleConnector.class,
                NoSnapshotLock.class,
                SharedSnapshotLock.class,
                SelectAllSnapshotQuery.class,
                StreamingAdapter.class,
                BufferedLogMinerAdapter.class,
                UnbufferedLogMinerAdapter.class,
                OpenLogReplicatorAdapter.class)
                .reason(getClass().getName())
                .build());

        reflectiveClassBuildItemBuildProducer.produce(ReflectiveClassBuildItem.builder(
                "io.debezium.connector.oracle.xstream.XStreamAdapter")
                .reason(getClass().getName())
                .build());
    }

    @BuildStep(onlyIf = NativeOrNativeSourcesBuild.class)
    NativeImageConfigBuildItem nativeImageConfiguration() {
        return NativeImageConfigBuildItem.builder()
                .addRuntimeInitializedClass("oracle.jdbc.driver.OracleDriver")
                .build();
    }

    @Override
    public Class<AgroalDatasourceConfiguration> quarkusDatasourceConfiguration() {
        return AgroalDatasourceConfiguration.class;
    }
}
