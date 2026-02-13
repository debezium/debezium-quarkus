/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.testsuite.deployment.naming;

import java.util.Properties;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.spi.schema.DataCollectionId;

/**
 * A helper naming strategy that uses the schema naming behavior, except all topic names are lowercased. This helps
 * to simplify the test suite by allowing the annotations to use a uniform syntax.
 *
 */
public class LowercaseSchemaTopicNamingStrategy extends SchemaTopicNamingStrategy {
    public LowercaseSchemaTopicNamingStrategy(Properties props) {
        super(props);
    }

    public static LowercaseSchemaTopicNamingStrategy create(CommonConnectorConfig config) {
        return new LowercaseSchemaTopicNamingStrategy(config.getConfig().asProperties());
    }

    @Override
    public String dataChangeTopic(DataCollectionId id) {
        return super.dataChangeTopic(id).toLowerCase();
    }

    @Override
    public String schemaChangeTopic() {
        return super.schemaChangeTopic().toLowerCase();
    }

    @Override
    public String heartbeatTopic() {
        return super.heartbeatTopic().toLowerCase();
    }

    @Override
    public String transactionTopic() {
        return super.transactionTopic().toLowerCase();
    }
}
