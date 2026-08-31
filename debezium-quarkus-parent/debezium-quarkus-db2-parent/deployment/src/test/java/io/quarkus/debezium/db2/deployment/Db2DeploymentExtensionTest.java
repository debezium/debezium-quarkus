/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.db2.deployment;

import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SuiteDisplayName;

import io.quarkus.debezium.testsuite.deployment.QuarkusDebeziumSqlExtensionTestSuite;

@SuiteDisplayName("DB2 Debezium Extensions for Quarkus Test Suite")
public class Db2DeploymentExtensionTest implements QuarkusDebeziumSqlExtensionTestSuite {

    private static final Db2TestResource db2Resource = new Db2TestResource();

    @BeforeSuite
    public static void init() throws Exception {
        db2Resource.start();
    }

    @AfterSuite
    public static void close() {
        db2Resource.stop();
    }
}
