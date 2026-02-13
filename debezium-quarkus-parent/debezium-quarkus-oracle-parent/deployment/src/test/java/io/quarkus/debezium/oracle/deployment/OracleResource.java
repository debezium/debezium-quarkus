/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.oracle.deployment;

import static io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper.CI_CONTAINER_STARTUP_TIME;

import java.net.URL;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.startupcheck.MinimumDurationRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public class OracleResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleResource.class);

    public static String IMAGE_NAME = "container-registry.oracle.com/database/free:latest-lite";
    public static String DATABASE_CDBROOT = "FREE";
    public static String DATABASE_PDB = "FREEPDB1";
    public static String ADMIN_USERNAME = "sys";
    public static String ADMIN_PASSWORD = "top_secret";
    public static String CONNECTOR_USERNAME = "c##dbzuser";
    public static String CONNECTOR_PASSWORD = "dbz";
    public static String OWNER_USERNAME = "inventory";
    public static String OWNER_PASSWORD = "dbz";

    private static final OracleContainer ORACLE_CONTAINER = new OracleContainer(
            DockerImageName.parse(IMAGE_NAME).asCompatibleSubstituteFor("gvenzl/oracle-xe"))
            .withNetworkAliases("oracle")
            .withUsername(CONNECTOR_USERNAME)
            .withPassword(CONNECTOR_PASSWORD)
            .withDatabaseName(DATABASE_CDBROOT)
            .waitingFor(new LogMessageWaitStrategy()
                    .withRegEx(".*The following output is now a tail of the alert\\.log.*\\s")
                    .withTimes(1)
                    .withStartupTimeout(Duration.of(CI_CONTAINER_STARTUP_TIME * 10, ChronoUnit.SECONDS)))
            .withStartupCheckStrategy(new MinimumDurationRunningStartupCheckStrategy(Duration.ofSeconds(10)))
            .withEnv("ORACLE_SID", DATABASE_CDBROOT)
            .withEnv("ORACLE_PWD", ADMIN_PASSWORD)
            .withEnv("ENABLE_ARCHIVELOG", "true")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER));

    public void start() throws Exception {
        ORACLE_CONTAINER.start();

        setupLogMiner();
        createTables();

        System.setProperty("ORACLE_JDBC", ORACLE_CONTAINER.getJdbcUrl());
        System.setProperty("ORACLE_PASSWORD", ORACLE_CONTAINER.getPassword());
        System.setProperty("ORACLE_USERNAME", ORACLE_CONTAINER.getUsername());
    }

    public void stop() {
        ORACLE_CONTAINER.stop();
    }

    private void setupLogMiner() throws Exception {
        final URL resource = Thread.currentThread().getContextClassLoader().getResource("setup-debezium-logminer.sql");
        executeSql(resource, ADMIN_USERNAME, ADMIN_PASSWORD, DATABASE_CDBROOT, true);
    }

    private void createTables() throws Exception {
        final URL resource = Thread.currentThread().getContextClassLoader().getResource("initialize-oracle-database.sql");
        executeSql(resource, OWNER_USERNAME, OWNER_PASSWORD, DATABASE_PDB, false);
    }

    private void executeSql(URL resource, String userName, String password, String databaseName, boolean sysDba) throws Exception {
        if (resource == null) {
            throw new IllegalArgumentException("Cannot initialize container");
        }

        final String script = IOUtils.toString(resource, Charset.defaultCharset());
        final String command = "sqlplus %s/%s@%s%s <<EOF\n%s\n;EXIT;\nEOF".formatted(userName, password, databaseName, sysDba ? " as sysdba" : "", script);

        final Container.ExecResult result = ORACLE_CONTAINER.execInContainer("bash", "-c", command);
        if (result.getExitCode() != 0) {
            LOGGER.error(result.getStderr());
            throw new IllegalStateException("Failed to initialize container with script");
        }
        LOGGER.info(result.getStdout());
    }
}
