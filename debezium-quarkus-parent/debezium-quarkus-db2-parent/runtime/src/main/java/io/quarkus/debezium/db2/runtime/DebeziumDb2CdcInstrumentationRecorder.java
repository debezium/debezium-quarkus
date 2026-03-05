/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.db2.runtime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
public class DebeziumDb2CdcInstrumentationRecorder {

    private static final Logger LOG = Logger.getLogger(DebeziumDb2CdcInstrumentationRecorder.class);

    /**
     * Records CDC registration setup to run at {@code RUNTIME_INIT}.
     *
     * @param tableIncludeList the raw value of {@code quarkus.debezium.table.include.list}
     *                         (uppercased by the caller), comma-separated {@code SCHEMA.TABLE}
     *                         and/or {@code SCHEMA.*} entries; may be empty.
     * @param retrySeconds     how long the background thread should keep trying before giving up.
     */
    public void setupCdcRegistration(String tableIncludeList, int retrySeconds) {
        try {
            Thread thread = new Thread(() -> runCdcRegistration(tableIncludeList, retrySeconds), "debezium-db2-cdc-setup");
            thread.setDaemon(true);
            thread.start();
        }
        catch (Exception e) {
            LOG.errorf("[CDC SETUP] CDC registration thread Failed: %s", e.getMessage());
        }
    }

    private void runCdcRegistration(String tableIncludeList, int retrySeconds) {
        Optional<ConnectionInfo> connInfo = resolveConnectionInfo();
        if (connInfo.isEmpty()) {
            return;
        }

        TableFilter filter = TableFilter.from(tableIncludeList);

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

        long deadline = System.currentTimeMillis() + (retrySeconds * 1000L);
        Connection c = acquireConnection(connInfo.get(), deadline);
        if (c == null) {
            LOG.warn("[CDC SETUP] Could not connect to DB2 within the retry window — CDC auto-registration skipped.");
            return;
        }

        try {
            Db2CdcOperations ops = new Db2CdcOperations(c);
            while (System.currentTimeMillis() < deadline) {
                if (runOneCycle(ops, filter)) {
                    ops.fixStateAndReinit();
                }
                if (filter.isTargeted() && ops.allActive(filter.exactTables())) {
                    LOG.infof("[CDC SETUP] All %d declared table(s) are now registered for CDC capture.",
                            filter.exactTables().size());
                    return;
                }
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            if (!filter.isTargeted()) {
                LOG.infof("[CDC SETUP] Watcher exiting after %ds.", retrySeconds);
            }
            else {
                List<String> missing = filter.exactTables().stream()
                        .filter(tid -> !ops.isRegistered(tid))
                        .map(TableId::toString)
                        .collect(Collectors.toList());
                if (!missing.isEmpty()) {
                    LOG.warnf("[CDC SETUP] Registration timed out after %ds. Still unregistered: %s",
                            retrySeconds, missing);
                }
                else {
                    LOG.infof("[CDC SETUP] All declared tables are registered (verified at timeout boundary after %ds).",
                            retrySeconds);
                }
            }
        }
        finally {
            closeConnection(c);
        }
    }

    private boolean runOneCycle(Db2CdcOperations ops, TableFilter filter) {
        boolean reinitNeeded = false;

        for (TableId tid : filter.exactTables()) {
            if (ops.existsInSyscat(tid) && !ops.isRegistered(tid)) {
                reinitNeeded |= ops.callAddTable(tid);
            }
        }

        for (String schema : filter.wildcardSchemas()) {
            for (TableId tid : ops.findUnregisteredInSchema(schema)) {
                reinitNeeded |= ops.callAddTable(tid);
            }
        }

        if (filter.isFullScan()) {
            for (TableId tid : ops.findUnregisteredAll()) {
                reinitNeeded |= ops.callAddTable(tid);
            }
        }

        return reinitNeeded;
    }

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

    private Connection acquireConnection(ConnectionInfo info, long deadline) {
        while (System.currentTimeMillis() < deadline) {
            try {
                Connection c = DriverManager.getConnection(info.jdbcUrl(), info.user(), info.password());
                c.setAutoCommit(false);
                return c;
            }
            catch (SQLException e) {
                LOG.debugf("[CDC SETUP] DB2 not yet accepting connections (%s). Retrying in 1s...", e.getMessage());
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }
        return null;
    }

    private void closeConnection(Connection c) {
        try {
            c.rollback();
        }
        catch (SQLException e) {
            LOG.debugf("[CDC SETUP] Rollback failed: %s", e.getMessage());
        }
        try {
            c.close();
        }
        catch (SQLException e) {
            LOG.debugf("[CDC SETUP] Connection close failed: %s", e.getMessage());
        }
    }

    private record ConnectionInfo(String jdbcUrl, String user, String password) {
    }

    private record TableFilter(List<TableId> exactTables, Set<String> wildcardSchemas) {

        static TableFilter from(String tableIncludeList) {
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

        boolean isTargeted() {
            return !exactTables.isEmpty() && wildcardSchemas.isEmpty();
        }

        boolean isFullScan() {
            return exactTables.isEmpty() && wildcardSchemas.isEmpty();
        }
    }

}
