/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.postgres.deployment;

import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SuiteDisplayName;

import io.quarkus.debezium.testsuite.deployment.QuarkusDebeziumSqlExtensionTestSuite;

@SuiteDisplayName("Postgres Debezium Extensions for Quarkus Test Suite")
public class PostgresDeploymentExtensionTest implements QuarkusDebeziumSqlExtensionTestSuite {
    private static final PostgresResource postgresResource = new PostgresResource();

    @BeforeSuite
    public static void init() {
        postgresResource.start();
    }

    @AfterSuite
    public static void close() {
        postgresResource.stop();
    }
}
