/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.db2.deployment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;

public class Db2TestResource {

    private static final String USER = "db2inst1";
    private static final String PASSWORD = "dbz";
    private static final String DATABASE = "TESTDB";
    private static final int DB2_PORT = 50000;

    private GenericContainer<?> container;

    public void start() {
        container = new GenericContainer<>(new ImageFromDockerfile("debezium-db2-cdc-test", false).withFileFromClasspath("Dockerfile", "db2-cdc-docker/Dockerfile")
                .withFileFromClasspath("asncdc.c", "db2-cdc-docker/asncdc.c").withFileFromClasspath("asncdc_UDF.sql", "db2-cdc-docker/asncdc_UDF.sql")
                .withFileFromClasspath("asncdcaddremove.sql", "db2-cdc-docker/asncdcaddremove.sql")
                .withFileFromClasspath("asncdctables.sql", "db2-cdc-docker/asncdctables.sql").withFileFromClasspath("cdcsetup.sh", "db2-cdc-docker/cdcsetup.sh")
                .withFileFromClasspath("dbsetup.sh", "db2-cdc-docker/dbsetup.sh")).withPrivilegedMode(true).withExposedPorts(DB2_PORT)
                .withEnv(Map.of("LICENSE", "accept", "DBNAME", DATABASE, "DB2INSTANCE", USER, "DB2INST1_PASSWORD", PASSWORD, "ARCHIVE_LOGS", "true", "AUTOCONFIG",
                        "false"))
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*done.*").withStartupTimeout(Duration.ofMinutes(10)));

        container.start();

        String jdbcUrl = "jdbc:db2://127.0.0.1:" + container.getMappedPort(DB2_PORT) + "/" + DATABASE;

        setupDatabase(jdbcUrl);

        System.setProperty("DB2_JDBC", jdbcUrl);
        System.setProperty("DB2_USERNAME", USER);
        System.setProperty("DB2_PASSWORD", PASSWORD);
    }

    private void setupDatabase(String jdbcUrl) {
        // Wait for DB2 to accept JDBC connections
        waitForJdbc(jdbcUrl);

        // Wait for CDC setup (ASNCDC.ADDTABLE procedure created by dbsetup.sh)
        waitForCdc(jdbcUrl);

        // Create schema, tables, seed data and enable CDC on each table
        try (Connection conn = DriverManager.getConnection(jdbcUrl, USER, PASSWORD)) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE SCHEMA inventory AUTHORIZATION db2inst1");
                stmt.execute("CREATE TABLE inventory.PRODUCTS(ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(255) NOT NULL)");
                stmt.execute("CREATE TABLE inventory.GENERAL_TABLE(ID INT NOT NULL PRIMARY KEY)");
                stmt.execute("CREATE TABLE inventory.ORDERS(ID INT NOT NULL PRIMARY KEY, KEY INT NOT NULL, NAME VARCHAR(255) NOT NULL)");
                stmt.execute("CREATE TABLE inventory.USERS(ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(255) NOT NULL, DESCRIPTION VARCHAR(255) NOT NULL)");

                stmt.execute("INSERT INTO inventory.GENERAL_TABLE(ID) VALUES (1)");
                stmt.execute("INSERT INTO inventory.ORDERS (ID, KEY, NAME) VALUES (1, 1, 'one')");
                stmt.execute("INSERT INTO inventory.ORDERS (ID, KEY, NAME) VALUES (2, 2, 'two')");
                stmt.execute("INSERT INTO inventory.USERS (ID, NAME, DESCRIPTION) VALUES (1, 'giovanni', 'developer')");
                stmt.execute("INSERT INTO inventory.USERS (ID, NAME, DESCRIPTION) VALUES (2, 'mario', 'developer')");
                stmt.execute("INSERT INTO inventory.PRODUCTS (ID, NAME) VALUES (1, 't-shirt')");
                stmt.execute("INSERT INTO inventory.PRODUCTS (ID, NAME) VALUES (2, 'thinkpad')");
                conn.commit();

                stmt.execute("CALL ASNCDC.ADDTABLE('INVENTORY', 'PRODUCTS')");
                stmt.execute("CALL ASNCDC.ADDTABLE('INVENTORY', 'GENERAL_TABLE')");
                stmt.execute("CALL ASNCDC.ADDTABLE('INVENTORY', 'ORDERS')");
                stmt.execute("CALL ASNCDC.ADDTABLE('INVENTORY', 'USERS')");
                stmt.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE='A' WHERE STATE='I'");
            }
            conn.commit();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to set up DB2 test database", e);
        }

        // Start asncap capture agent inside the container
        startAsnCap();

        // Wait until asncap has written its first LSN proving it is active
        waitForLsn(jdbcUrl);
    }

    private void startAsnCap() {
        try {
            container.execInContainer("su", "-", "db2inst1", "-s", "/bin/bash", "-c",
                    "nohup /database/config/db2inst1/sqllib/bin/asncap" + " capture_schema=asncdc capture_server=TESTDB" + " > /tmp/asncap.log 2>&1 &");
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start asncap in container", e);
        }
    }

    private void waitForJdbc(String jdbcUrl) {
        for (int i = 0; i < 60; i++) {
            try (Connection conn = DriverManager.getConnection(jdbcUrl, USER, PASSWORD); Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1");
                return;
            }
            catch (Exception e) {
                sleep(5_000);
            }
        }
        throw new RuntimeException("DB2 did not accept JDBC connections within timeout");
    }

    private void waitForCdc(String jdbcUrl) {
        for (int i = 0; i < 60; i++) {
            try (Connection conn = DriverManager.getConnection(jdbcUrl, USER, PASSWORD); Statement stmt = conn.createStatement()) {
                var rs = stmt.executeQuery("SELECT ROUTINENAME FROM SYSCAT.ROUTINES WHERE ROUTINESCHEMA = 'ASNCDC' AND ROUTINENAME = 'ADDTABLE'");
                if (rs.next()) {
                    return;
                }
            }
            catch (Exception ignored) {
            }
            sleep(10_000);
        }
        throw new RuntimeException("DB2 CDC setup did not complete within timeout");
    }

    private void waitForLsn(String jdbcUrl) {
        for (int i = 0; i < 60; i++) {
            try (Connection conn = DriverManager.getConnection(jdbcUrl, USER, PASSWORD); Statement stmt = conn.createStatement()) {
                var rs = stmt.executeQuery("SELECT MAX(t.SYNCHPOINT) FROM (" + " SELECT CD_NEW_SYNCHPOINT AS SYNCHPOINT FROM ASNCDC.IBMSNAP_REGISTER" + " UNION ALL"
                        + " SELECT SYNCHPOINT AS SYNCHPOINT FROM ASNCDC.IBMSNAP_REGISTER) t");
                if (rs.next() && rs.getBytes(1) != null) {
                    return;
                }
            }
            catch (Exception ignored) {
            }
            sleep(5_000);
        }
        throw new RuntimeException("asncap did not populate an LSN within timeout");
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for DB2", ie);
        }
    }

    public void stop() {
        if (container != null) {
            container.stop();
        }
    }
}
