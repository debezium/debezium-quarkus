/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.configuration;

import static io.debezium.connector.mongodb.shared.SharedMongoDbConnectorConfig.CONNECTION_STRING;
import static io.debezium.runtime.configuration.QuarkusDatasourceConfiguration.DEFAULT;

import java.util.Map;
import java.util.function.Supplier;

import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;
import io.debezium.runtime.recorder.DatasourceRecorder;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class MongodbDatasourceCompatibilityRecorder implements DatasourceRecorder<MultiEngineMongoDbDatasourceConfiguration> {

    private final RuntimeValue<DebeziumEngineRuntimeConfiguration> debeziumEngineConfigurationRuntimeValue;

    public MongodbDatasourceCompatibilityRecorder(RuntimeValue<DebeziumEngineRuntimeConfiguration> debeziumEngineConfigurationRuntimeValue) {
        this.debeziumEngineConfigurationRuntimeValue = debeziumEngineConfigurationRuntimeValue;
    }

    @Override
    public Supplier<MultiEngineMongoDbDatasourceConfiguration> convert(String name, boolean isDefault) {
        return () -> new MultiEngineMongoDbDatasourceConfiguration(Map.of(
                DEFAULT, new MongoDbDatasourceConfiguration(
                        debeziumEngineConfigurationRuntimeValue.getValue().defaultConfiguration().get(CONNECTION_STRING.name()),
                        DEFAULT,
                        true)));
    }
}
