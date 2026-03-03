/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.db2.runtime;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import io.quarkus.runtime.annotations.Recorder;

/**
 * Quarkus Recorder that sets up automatic CDC registration for DB2 Dev Services.
 * <p>
 * The recorded method runs at {@code RUNTIME_INIT}, after all Dev Services have started and
 * after Flyway / Liquibase / Hibernate DDL have completed. It spawns a short-lived daemon
 * thread that polls DB2 every second and calls {@code ASNCDC.ADDTABLE()} for any user table
 * not yet registered in {@code ASNCDC.IBMSNAP_REGISTER}, then issues an {@code asnccmd reinit}
 * via the {@code ASNCDC.ASNCDCSERVICES} UDF so the running capture agent picks up the change.
 * <p>
 * Operating modes (determined by {@code table.include.list}):
 * <ul>
 *   <li><b>Targeted</b> – only exact {@code SCHEMA.TABLE} pairs; exits as soon as every declared
 *       table has {@code STATE='A'} in the register, or on timeout.</li>
 *   <li><b>Schema-scoped</b> – {@code SCHEMA.*} entries; scans only the declared schemas.</li>
 *   <li><b>Mixed</b> – combination of exact pairs and schema wildcards.</li>
 *   <li><b>Full-scan</b> – no include list; scans all non-system user tables.</li>
 * </ul>
 * Non-targeted modes run for at most {@code retrySeconds} seconds and then exit.
 */
@Recorder
public class DebeziumDb2Recorder {

    private static final Logger LOG = Logger.getLogger(DebeziumDb2Recorder.class);

    private static final String EXCLUDED_SCHEMAS = "'SYSIBM','SYSCAT','SYSSTAT','SYSPROC','SYSIBMADM','SYSTOOLS','ASNCDC','NULLID','SQLJ'";

    // -- SQL constants --------------------------------------------------------

    private static final String SQL_EXISTS_IN_SYSCAT = "SELECT 1 FROM SYSCAT.TABLES WHERE TRIM(TABSCHEMA)=? AND TRIM(TABNAME)=? AND TYPE='T'";

    private static final String SQL_IS_REGISTERED = "SELECT 1 FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER=? AND SOURCE_TABLE=?";

    private static final String SQL_IS_ACTIVE = "SELECT 1 FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER=? AND SOURCE_TABLE=? AND STATE='A'";

    private static final String SQL_UNREGISTERED_IN_SCHEMA = "SELECT TRIM(t.TABSCHEMA), TRIM(t.TABNAME) FROM SYSCAT.TABLES t "
            + "WHERE t.TYPE='T' AND t.TABSCHEMA=? "
            + "AND NOT EXISTS (SELECT 1 FROM ASNCDC.IBMSNAP_REGISTER r "
            + "  WHERE r.SOURCE_OWNER=TRIM(t.TABSCHEMA) AND r.SOURCE_TABLE=TRIM(t.TABNAME)) "
            + "ORDER BY t.TABNAME";

    private static final String SQL_UNREGISTERED_ALL = "SELECT TRIM(t.TABSCHEMA), TRIM(t.TABNAME) FROM SYSCAT.TABLES t "
            + "WHERE t.TYPE='T' AND t.TABSCHEMA NOT IN (" + EXCLUDED_SCHEMAS + ") "
            + "AND NOT EXISTS (SELECT 1 FROM ASNCDC.IBMSNAP_REGISTER r "
            + "  WHERE r.SOURCE_OWNER=TRIM(t.TABSCHEMA) AND r.SOURCE_TABLE=TRIM(t.TABNAME)) "
            + "ORDER BY t.TABSCHEMA, t.TABNAME";

    // -- Value types ----------------------------------------------------------

    /**
     * Immutable schema-qualified table identifier.
     */
    private record TableId(String schema, String table) {
        @Override
        public String toString() {
            return schema + "." + table;
        }
    }

    /**
     * JDBC connection parameters resolved from Quarkus runtime config.
     */
    private record ConnectionInfo(String jdbcUrl, String user, String password) {
    }

    /**
     * Parsed representation of the {@code table.include.list} configuration.
     */
    private record TableFilter(List<TableId> exactTables, Set<String> wildcardSchemas) {

        boolean isTargeted() {
            return !exactTables.isEmpty() && wildcardSchemas.isEmpty();
        }

        boolean isFullScan() {
            return exactTables.isEmpty() && wildcardSchemas.isEmpty();
        }
    }

    // -- Public API -----------------------------------------------------------

    /**
     * Records CDC registration setup to run at {@code RUNTIME_INIT}.
     *
     * @param tableIncludeList the raw value of {@code quarkus.debezium.table.include.list}
     *                         (uppercased by the caller), comma-separated {@code SCHEMA.TABLE}
     *                         and/or {@code SCHEMA.*} entries; may be empty.
     * @param retrySeconds     how long the background thread should keep trying before giving up.
     */
    public void setupCdcRegistration(String tableIncludeList, int retrySeconds) {
        Thread thread = new Thread(() -> {
            try {
                runCdcRegistration(tableIncludeList, retrySeconds);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("[CDC SETUP] Registration thread interrupted.", e);
            }
            catch (Exception e) {
                LOG.error("[CDC SETUP] Registration thread terminated unexpectedly.", e);
            }
        }, "debezium-db2-cdc-setup");
        thread.setDaemon(true);
        thread.start();
    }

    // -- Core logic -----------------------------------------------------------

    private void runCdcRegistration(String tableIncludeList, int retrySeconds) throws InterruptedException {
        Optional<ConnectionInfo> connInfo = resolveConnectionInfo();
        if (connInfo.isEmpty()) {
            return;
        }

        TableFilter filter = parseTableIncludeList(tableIncludeList);
        logMode(filter, retrySeconds);

        long deadline = System.currentTimeMillis() + (retrySeconds * 1000L);
        Connection c = acquireConnection(connInfo.get(), deadline);
        if (c == null) {
            LOG.warn("[CDC SETUP] Could not connect to DB2 within the retry window — CDC auto-registration skipped.");
            return;
        }

        try {
            // Main polling loop — runs one registration cycle per second until done or timed out.
            while (System.currentTimeMillis() < deadline) {
                if (runOneCycle(c, filter)) {
                    fixStateAndReinit(c);
                }
                if (filter.isTargeted() && allActive(c, filter.exactTables())) {
                    LOG.infof("[CDC SETUP] All %d declared table(s) are now registered for CDC capture.", filter.exactTables().size());
                    return;
                }
                Thread.sleep(1000);
            }
            logOutcome(c, filter, retrySeconds);
        }
        finally {
            // DB2 JCC (ERRORCODE=-4471) rejects close() while a transaction is in progress,
            // so roll back any pending work before closing.
            closeConnection(c);
        }
    }

    /**
     * Performs a single detection-and-registration sweep across all configured modes.
     */
    private boolean runOneCycle(Connection c, TableFilter filter) {
        boolean reinitNeeded = false;

        for (TableId tid : filter.exactTables()) {
            if (existsInSyscat(c, tid) && !isRegistered(c, tid)) {
                reinitNeeded |= callAddTable(c, tid);
            }
        }

        for (String schema : filter.wildcardSchemas()) {
            for (TableId tid : findUnregisteredInSchema(c, schema)) {
                reinitNeeded |= callAddTable(c, tid);
            }
        }

        if (filter.isFullScan()) {
            for (TableId tid : findUnregisteredAll(c)) {
                reinitNeeded |= callAddTable(c, tid);
            }
        }

        return reinitNeeded;
    }

    // -- Configuration parsing ------------------------------------------------

    /**
     * Reads DB2 JDBC URL, username, and password from runtime config.
     */
    private Optional<ConnectionInfo> resolveConnectionInfo() {
        var config = ConfigProvider.getConfig();
        String jdbcUrl = config.getOptionalValue("quarkus.datasource.jdbc.url", String.class).orElse(null);
        String user = config.getOptionalValue("quarkus.datasource.username", String.class).orElse("db2inst1");
        String password = config.getOptionalValue("quarkus.datasource.password", String.class).orElse(null);

        if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:db2:")) {
            LOG.warn("[CDC SETUP] DB2 datasource URL not found in runtime config — CDC auto-registration skipped.");
            return Optional.empty();
        }
        if (password == null) {
            LOG.warn("[CDC SETUP] DB2 datasource password not found in runtime config — CDC auto-registration skipped.");
            return Optional.empty();
        }
        return Optional.of(new ConnectionInfo(jdbcUrl, user, password));
    }

    /**
     * Parses {@code SCHEMA.TABLE} and {@code SCHEMA.*} entries into a {@link TableFilter}.
     */
    private TableFilter parseTableIncludeList(String tableIncludeList) {
        Set<TableId> exactTables = new LinkedHashSet<>();
        Set<String> wildcardSchemas = new LinkedHashSet<>();

        if (tableIncludeList != null && !tableIncludeList.isBlank()) {
            for (String entry : tableIncludeList.split(",")) {
                entry = entry.strip();
                int dot = entry.indexOf('.');
                if (dot < 0) {
                    continue;
                }
                String schema = entry.substring(0, dot).strip();
                String table = entry.substring(dot + 1).strip();
                if (table.equals("*")) {
                    wildcardSchemas.add(schema);
                }
                else {
                    exactTables.add(new TableId(schema, table));
                }
            }
        }

        return new TableFilter(List.copyOf(exactTables), Collections.unmodifiableSet(wildcardSchemas));
    }

    // -- JDBC helpers ---------------------------------------------------------

    /**
     * Returns true if the table exists in SYSCAT.TABLES (i.e. has been created).
     */
    private boolean existsInSyscat(Connection c, TableId tid) {
        return queryExists(c, SQL_EXISTS_IN_SYSCAT, tid,
                "Failed to check SYSCAT.TABLES for '%s'.'%s'");
    }

    /**
     * Returns true if the table already has an entry in IBMSNAP_REGISTER.
     */
    private boolean isRegistered(Connection c, TableId tid) {
        return queryExists(c, SQL_IS_REGISTERED, tid,
                "Failed to check ASNCDC.IBMSNAP_REGISTER for '%s'.'%s'");
    }

    /**
     * Returns true if the table has {@code STATE='A'} in IBMSNAP_REGISTER.
     */
    private boolean isActive(Connection c, TableId tid) {
        return queryExists(c, SQL_IS_ACTIVE, tid, null);
    }

    /**
     * Executes a parameterised existence check (schema, table) and returns whether a row was found.
     *
     * @param warnPattern if non-null, used to log a warning on failure (formatted with schema, table, message).
     */
    private boolean queryExists(Connection c, String sql, TableId tid, String warnPattern) {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, tid.schema());
            ps.setString(2, tid.table());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
        catch (SQLException e) {
            if (warnPattern != null) {
                LOG.warnf("[CDC SETUP] " + warnPattern + ": %s", tid.schema(), tid.table(), e.getMessage());
            }
            return false;
        }
    }

    /**
     * Finds tables in the given schema that are not yet in IBMSNAP_REGISTER.
     */
    private List<TableId> findUnregisteredInSchema(Connection c, String schema) {
        List<TableId> result = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(SQL_UNREGISTERED_IN_SCHEMA)) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new TableId(rs.getString(1), rs.getString(2)));
                }
            }
        }
        catch (SQLException e) {
            LOG.warnf("[CDC SETUP] Error scanning schema '%s': %s", schema, e.getMessage());
        }
        return result;
    }

    /**
     * Finds ALL user tables (excluding system schemas) not in IBMSNAP_REGISTER.
     */
    private List<TableId> findUnregisteredAll(Connection c) {
        List<TableId> result = new ArrayList<>();
        try (Statement stmt = c.createStatement();
                ResultSet rs = stmt.executeQuery(SQL_UNREGISTERED_ALL)) {
            while (rs.next()) {
                result.add(new TableId(rs.getString(1), rs.getString(2)));
            }
        }
        catch (SQLException e) {
            LOG.warnf("[CDC SETUP] Error in full-scan query: %s", e.getMessage());
        }
        return result;
    }

    /**
     * Calls {@code ASNCDC.ADDTABLE(schema, table)}.
     *
     * @return true if the call succeeded (table was registered), false otherwise.
     */
    private boolean callAddTable(Connection c, TableId tid) {
        try (CallableStatement cs = c.prepareCall("CALL ASNCDC.ADDTABLE(?, ?)")) {
            cs.setString(1, tid.schema());
            cs.setString(2, tid.table());
            cs.execute();
            c.commit();
            LOG.infof("[CDC SETUP] Registered '%s'.'%s' for CDC capture.", tid.schema(), tid.table());
            return true;
        }
        catch (SQLException e) {
            rollback(c);
            LOG.warnf("[CDC SETUP] ADDTABLE('%s','%s') failed: %s", tid.schema(), tid.table(), e.getMessage());
            return false;
        }
    }

    /**
     * Closes the asncap race window (sets any {@code STATE='I'} rows back to {@code 'A'})
     * then signals the capture agent to reinitialise via the {@code ASNCDCSERVICES} UDF.
     */
    private void fixStateAndReinit(Connection c) {
        try (Statement stmt = c.createStatement()) {
            stmt.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE='A' WHERE STATE='I'");
            c.commit();
            LOG.info("[CDC SETUP] Issuing asncap reinit...");
            stmt.execute("VALUES ASNCDC.ASNCDCSERVICES('reinit', 'asncdc')");
            LOG.info("[CDC SETUP] Reinit completed.");
        }
        catch (SQLException e) {
            LOG.errorf("[CDC SETUP] fixStateAndReinit failed: %s", e.getMessage());
            rollback(c);
        }
    }

    /**
     * Returns true only when every declared exact table has {@code STATE='A'}.
     */
    private boolean allActive(Connection c, List<TableId> tables) {
        return tables.stream().allMatch(tid -> isActive(c, tid));
    }

    /**
     * Rolls back the connection
     */
    private void rollback(Connection c) {
        try {
            c.rollback();
        }
        catch (SQLException e) {
            LOG.debugf("[CDC SETUP] Rollback failed: %s", e.getMessage());
        }
    }

    // -- Connection helpers ---------------------------------------------------

    /**
     * Attempts to open a JDBC connection, retrying every second until the deadline.
     */
    private Connection acquireConnection(ConnectionInfo info, long deadline) throws InterruptedException {
        while (System.currentTimeMillis() < deadline) {
            try {
                Connection c = DriverManager.getConnection(info.jdbcUrl(), info.user(), info.password());
                c.setAutoCommit(false);
                return c;
            }
            catch (SQLException e) {
                LOG.debugf("[CDC SETUP] DB2 not yet accepting connections (%s). Retrying in 1s...", e.getMessage());
                Thread.sleep(1000);
            }
        }
        return null;
    }

    /**
     * Rolls back any pending transaction, then closes the connection.
     * DB2 JCC (ERRORCODE=-4471) rejects {@code close()} while a transaction is in flight.
     */
    private void closeConnection(Connection c) {
        rollback(c);
        try {
            c.close();
        }
        catch (SQLException e) {
            LOG.debugf("[CDC SETUP] Connection close failed: %s", e.getMessage());
        }
    }

    // -- Logging helpers ------------------------------------------------------

    private void logMode(TableFilter filter, int retrySeconds) {
        if (filter.isTargeted()) {
            LOG.infof("[CDC SETUP] Targeted mode: %d declared table(s), timeout %ds.",
                    filter.exactTables().size(), retrySeconds);
        }
        else if (!filter.wildcardSchemas().isEmpty()) {
            String label = filter.exactTables().isEmpty() ? "Schema-scoped" : "Mixed";
            LOG.infof("[CDC SETUP] %s mode: schemas [%s], timeout %ds.",
                    label, String.join(", ", filter.wildcardSchemas()), retrySeconds);
        }
        else {
            LOG.infof("[CDC SETUP] Full-scan mode: all user tables, timeout %ds.", retrySeconds);
        }
    }

    private void logOutcome(Connection c, TableFilter filter, int retrySeconds) {
        if (!filter.isTargeted()) {
            LOG.infof("[CDC SETUP] Watcher exiting after %ds.", retrySeconds);
            return;
        }
        List<String> missing = filter.exactTables().stream()
                .filter(tid -> !isRegistered(c, tid))
                .map(TableId::toString)
                .collect(Collectors.toList());
        if (!missing.isEmpty()) {
            LOG.warnf("[CDC SETUP] Registration timed out after %ds. Still unregistered: %s", retrySeconds, missing);
        }
        else {
            LOG.infof("[CDC SETUP] All declared tables are registered (verified at timeout boundary after %ds).", retrySeconds);
        }
    }
}
