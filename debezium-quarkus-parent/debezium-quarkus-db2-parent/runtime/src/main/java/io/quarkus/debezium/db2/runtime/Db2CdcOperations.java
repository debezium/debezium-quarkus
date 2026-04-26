/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.db2.runtime;

import org.jboss.logging.Logger;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC helper for DB2 CDC operations.
 * <p>
 * Wraps a {@link Connection} and exposes methods to query registration state and
 * invoke the {@code ASNCDC} stored procedures required for CDC capture setup.
 */
class Db2CdcOperations {

    private static final Logger LOG = Logger.getLogger(Db2CdcOperations.class);

    private static final String EXCLUDED_SCHEMAS = "'SYSIBM','SYSCAT','SYSSTAT','SYSPROC','SYSIBMADM','SYSTOOLS','ASNCDC','NULLID','SQLJ'";

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

    private final Connection connection;

    Db2CdcOperations(Connection connection) {
        this.connection = connection;
    }

    public boolean existsInSyscat(TableId tid) {
        return queryExists(SQL_EXISTS_IN_SYSCAT, tid, "Failed to check SYSCAT.TABLES for '%s'.'%s'");
    }

    public boolean isRegistered(TableId tid) {
        return queryExists(SQL_IS_REGISTERED, tid, "Failed to check ASNCDC.IBMSNAP_REGISTER for '%s'.'%s'");
    }

    public boolean isActive(TableId tid) {
        return queryExists(SQL_IS_ACTIVE, tid, null);
    }

    public boolean allActive(List<TableId> tables) {
        return tables.stream().allMatch(this::isActive);
    }

    public List<TableId> findUnregisteredInSchema(String schema) {
        List<TableId> result = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(SQL_UNREGISTERED_IN_SCHEMA)) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new TableId(rs.getString(1), rs.getString(2)));
                }
            }
        } catch (SQLException e) {
            LOG.warnf("[CDC SETUP] Error scanning schema '%s': %s", schema, e.getMessage());
        }
        return result;
    }

    public List<TableId> findUnregisteredAll() {
        List<TableId> result = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(SQL_UNREGISTERED_ALL)) {
            while (rs.next()) {
                result.add(new TableId(rs.getString(1), rs.getString(2)));
            }
        } catch (SQLException e) {
            LOG.warnf("[CDC SETUP] Error in full-scan query: %s", e.getMessage());
        }
        return result;
    }

    public boolean callAddTable(TableId tid) {
        try (CallableStatement cs = connection.prepareCall("CALL ASNCDC.ADDTABLE(?, ?)")) {
            cs.setString(1, tid.schema());
            cs.setString(2, tid.table());
            cs.execute();
            connection.commit();
            LOG.infof("[CDC SETUP] Registered '%s'.'%s' for CDC capture.", tid.schema(), tid.table());
            return true;
        } catch (SQLException e) {
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
        } catch (SQLException e) {
            LOG.errorf("[CDC SETUP] fixStateAndReinit failed: %s", e.getMessage());
            rollback();
        }
    }

    private boolean queryExists(String sql, TableId tid, String warnPattern) {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tid.schema());
            ps.setString(2, tid.table());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            if (warnPattern != null) {
                LOG.warnf("[CDC SETUP] " + warnPattern + ": %s", tid.schema(), tid.table(), e.getMessage());
            }
            return false;
        }
    }

    private void rollback() {
        try {
            connection.rollback();
        } catch (SQLException e) {
            LOG.debugf("[CDC SETUP] Rollback failed: %s", e.getMessage());
        }
    }
}
