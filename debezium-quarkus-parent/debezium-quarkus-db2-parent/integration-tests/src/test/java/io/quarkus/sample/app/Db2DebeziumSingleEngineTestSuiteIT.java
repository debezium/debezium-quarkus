/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app;

import org.junit.platform.suite.api.BeforeSuite;

public class Db2DebeziumSingleEngineTestSuiteIT implements QuarkusDebeziumSingleEngineTestSuite {

    @BeforeSuite
    public static void setupDatabase() {
        Db2IntegrationTestResource.setupNativeDatabase();
    }
}
