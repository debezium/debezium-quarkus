/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.db2.runtime;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.jboss.logging.Logger;

import io.debezium.relational.TableId;

/**
 * JDBC helper for DB2 CDC operations.
 * <p>
 * Wraps a {@link Connection} and exposes methods to query registration state and
 * invoke the {@code ASNCDC} stored procedures required for CDC capture setup.
 */
class Db2CdcOperations {

    private static final Logger LOG = Logger.getLogger(Db2CdcOperations.class);

    private static final String EXCLUDED_SCHEMAS = "'SYSIBM','SYSCAT','SYSSTAT','SYSPROC','SYSIBMADM','SYSTOOLS','ASNCDC','NULLID','SQLJ'";

    private static final String SQL_UNREGISTERED_USER_TABLES = "SELECT TRIM(t.TABSCHEMA), TRIM(t.TABNAME) FROM SYSCAT.TABLES t "
            + "WHERE t.TYPE='T' AND TRIM(t.TABSCHEMA) NOT IN (" + EXCLUDED_SCHEMAS + ") "
            + "AND NOT EXISTS (SELECT 1 FROM ASNCDC.IBMSNAP_REGISTER r "
            + "  WHERE r.SOURCE_OWNER=TRIM(t.TABSCHEMA) AND r.SOURCE_TABLE=TRIM(t.TABNAME)) "
            + "ORDER BY t.TABSCHEMA, t.TABNAME";

    private static final String SQL_INACTIVE_REGISTERED_TABLES = "SELECT TRIM(SOURCE_OWNER), TRIM(SOURCE_TABLE) FROM ASNCDC.IBMSNAP_REGISTER "
            + "WHERE STATE='I' AND LENGTH(TRIM(SOURCE_OWNER)) > 0 AND LENGTH(TRIM(SOURCE_TABLE)) > 0";

    private final Connection connection;

    Db2CdcOperations(Connection connection) {
        this.connection = connection;
    }

    public List<TableId> findUnregisteredUserTables() {
        return queryTables(SQL_UNREGISTERED_USER_TABLES, "Error scanning user tables");
    }

    public List<TableId> findInactiveRegisteredTables() {
        return queryTables(SQL_INACTIVE_REGISTERED_TABLES, "Error scanning inactive registrations");
    }

    public boolean callAddTable(TableId tid) {
        try (CallableStatement cs = connection.prepareCall("CALL ASNCDC.ADDTABLE(?, ?)")) {
            cs.setString(1, tid.schema());
            cs.setString(2, tid.table());
            cs.execute();
            connection.commit();
            LOG.infof("[CDC SETUP] Registered '%s'.'%s' for CDC capture.", tid.schema(), tid.table());
            return true;
        }
        catch (SQLException e) {
            rollback();
            LOG.warnf("[CDC SETUP] ADDTABLE('%s','%s') failed: %s", tid.schema(), tid.table(), e.getMessage());
            return false;
        }
    }

    public void fixStateAndReinit() {
        try (Statement stmt = connection.createStatement()) {
            int updated = stmt.executeUpdate(
                    "UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE='A' " +
                            "WHERE STATE='I' AND LENGTH(TRIM(SOURCE_OWNER)) > 0 AND LENGTH(TRIM(SOURCE_TABLE)) > 0");
            connection.commit();
            if (updated > 0) {
                LOG.infof("[CDC SETUP] Activated %d registration(s) in IBMSNAP_REGISTER.", updated);
            }
            LOG.info("[CDC SETUP] Issuing asncap start+reinit...");
            stmt.execute("VALUES ASNCDC.ASNCDCSERVICES('start', 'asncdc')");
            stmt.execute("VALUES ASNCDC.ASNCDCSERVICES('reinit', 'asncdc')");
            LOG.info("[CDC SETUP] Reinit signal sent.");
        }
        catch (SQLException e) {
            LOG.errorf("[CDC SETUP] fixStateAndReinit failed: %s", e.getMessage());
            rollback();
        }
    }

    private List<TableId> queryTables(String sql, String errorMessage) {
        List<TableId> result = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                result.add(new TableId(null, rs.getString(1), rs.getString(2)));
            }
        }
        catch (SQLException e) {
            LOG.warnf("[CDC SETUP] %s: %s", errorMessage, e.getMessage());
        }
        return result;
    }

    private void rollback() {
        try {
            connection.rollback();
        }
        catch (SQLException e) {
            LOG.debugf("[CDC SETUP] Rollback failed: %s", e.getMessage());
        }
    }
}
