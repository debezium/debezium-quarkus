/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.oracle.deployment;

import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SuiteDisplayName;

import io.quarkus.debezium.testsuite.deployment.QuarkusDebeziumSqlExtensionTestSuite;

@SuiteDisplayName("Oracle Debezium Extensions for Quarkus Test Suite")
public class OracleDeploymentExtensionTest implements QuarkusDebeziumSqlExtensionTestSuite {
    private static final OracleResource oracleResource = new OracleResource();

    @BeforeSuite
    public static void init() throws Exception {
        oracleResource.start();
    }

    @AfterSuite
    public static void close() {
        oracleResource.stop();
    }
}
