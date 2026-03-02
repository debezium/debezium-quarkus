/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Programmatic DB2 test database setup for integration tests.
 * <p>
 * Replaces the {@code TEST_MODE} shell script block in {@code cdcsetup.sh}.
 * Called from {@code @BeforeSuite} in each IT suite class so that schemas,
 * tables, seed data, and CDC registrations are in place before the Quarkus
 * application (and its Debezium capture engines) start.
 */
public class Db2IntegrationTestResource {

    private static final String USER = "db2inst1";
    private static final String PASSWORD = "dbz";
    private static final String DATABASE = "TESTDB";

    private static final AtomicBoolean nativeInitialized = new AtomicBoolean(false);
    private static final AtomicBoolean alternativeInitialized = new AtomicBoolean(false);

    /**
     * Sets up the {@code NATIVE} schema in the native DB2 container (default port 50000).
     * Safe to call multiple times – executes only once per JVM process.
     */
    public static void setupNativeDatabase() {
        if (!nativeInitialized.compareAndSet(false, true)) {
            return;
        }
        String jdbcUrl = System.getProperty("DB2_JDBC", "jdbc:db2://localhost:50000/" + DATABASE);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, USER, PASSWORD)) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE SCHEMA native AUTHORIZATION db2inst1");
                stmt.execute("CREATE TABLE native.PRODUCTS(" +
                        "ID INT NOT NULL PRIMARY KEY, " +
                        "NAME VARCHAR(255) NOT NULL, " +
                        "DESCRIPTION VARCHAR(255))");
                stmt.execute("CREATE TABLE native.USERS(" +
                        "ID INT NOT NULL PRIMARY KEY, " +
                        "NAME VARCHAR(255) NOT NULL, " +
                        "DESCRIPTION VARCHAR(255) NOT NULL)");

                stmt.execute("INSERT INTO native.PRODUCTS (ID, NAME, DESCRIPTION) VALUES (1, 't-shirt', 'red hat t-shirt')");
                stmt.execute("INSERT INTO native.PRODUCTS (ID, NAME, DESCRIPTION) VALUES (2, 'sweatshirt', 'blue ibm sweatshirt')");
                stmt.execute("INSERT INTO native.USERS (ID, NAME, DESCRIPTION) VALUES (1, 'giovanni', 'developer')");
                stmt.execute("INSERT INTO native.USERS (ID, NAME, DESCRIPTION) VALUES (2, 'mario', 'developer')");
                conn.commit();

                stmt.execute("CALL ASNCDC.ADDTABLE('NATIVE', 'PRODUCTS')");
                stmt.execute("CALL ASNCDC.ADDTABLE('NATIVE', 'USERS')");
                stmt.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE='A' WHERE STATE='I'");
            }
            conn.commit();
        } catch (Exception e) {
            throw new RuntimeException("Failed to set up DB2 native engine test database", e);
        }
    }

    /**
     * Sets up the {@code ALTERNATIVE} schema in the alternative DB2 container (default port 50001).
     * Safe to call multiple times – executes only once per JVM process.
     */
    public static void setupAlternativeDatabase() {
        if (!alternativeInitialized.compareAndSet(false, true)) {
            return;
        }
        String jdbcUrl = System.getProperty("DB2_JDBC_ALTERNATIVE", "jdbc:db2://localhost:50001/" + DATABASE);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, USER, PASSWORD)) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE SCHEMA alternative AUTHORIZATION db2inst1");
                stmt.execute("CREATE TABLE alternative.ORDERS(" +
                        "ID INT NOT NULL PRIMARY KEY, " +
                        "KEY INT NOT NULL, " +
                        "NAME VARCHAR(255) NOT NULL)");

                stmt.execute("INSERT INTO alternative.ORDERS (ID, KEY, NAME) VALUES (1, 1, 'one')");
                stmt.execute("INSERT INTO alternative.ORDERS (ID, KEY, NAME) VALUES (2, 2, 'two')");
                conn.commit();

                stmt.execute("CALL ASNCDC.ADDTABLE('ALTERNATIVE', 'ORDERS')");
                stmt.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE='A' WHERE STATE='I'");
            }
            conn.commit();
        } catch (Exception e) {
            throw new RuntimeException("Failed to set up DB2 alternative engine test database", e);
        }
    }
}
