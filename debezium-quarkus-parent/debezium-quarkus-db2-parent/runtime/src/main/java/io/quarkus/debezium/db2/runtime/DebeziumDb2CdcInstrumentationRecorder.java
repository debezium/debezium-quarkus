/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.db2.runtime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.runtime.configuration.DebeziumEngineRuntimeConfiguration;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;

/**
 * The Quarkus Recorder that configures auto CDC registration in DB2 Dev Services.
 * <p>
 * The recorder's method executes during the {@code RUNTIME_INIT} phase and launches a temporary
 * daemon thread which, for {@code retrySeconds} seconds, polls DB2 once per second and invokes
 * {@code ASNCDC.ADDTABLE()} for all tables captured by the connector and not already registered in
 * {@code ASNCDC.IBMSNAP_REGISTER}. After the invocation, the thread performs the {@code asnccmd reinit}
 * command using {@code ASNCDC.ASNCDCSERVICES} UDF to refresh the capture agent.
 * New tables added by the application while the thread works (for instance using Flyway or Hibernate)
 * are also registered automatically. When the timeout expires, the thread logs the tables captured by
 * the connector that are still unregistered.
 * <p>
 * The tables are selected with the table filters provided by the connector, thus the table and schema
 * inclusion/exclusion work exactly as they do in the connector.
 */
@Recorder
public class DebeziumDb2CdcInstrumentationRecorder {

    private static final Logger LOG = Logger.getLogger(DebeziumDb2CdcInstrumentationRecorder.class);

    private final RuntimeValue<DebeziumEngineRuntimeConfiguration> debeziumEngineConfigurationRuntimeValue;

    public DebeziumDb2CdcInstrumentationRecorder(RuntimeValue<DebeziumEngineRuntimeConfiguration> debeziumEngineConfigurationRuntimeValue) {
        this.debeziumEngineConfigurationRuntimeValue = debeziumEngineConfigurationRuntimeValue;
    }

    /**
     * Records CDC registration setup to run at {@code RUNTIME_INIT}.
     *
     * @param retrySeconds how long the background thread should keep trying before giving up.
     */
    public void setupCdcRegistration(int retrySeconds) {
        try {
            TableFilter tableFilter = new Db2ConnectorConfig(Configuration.from(debeziumEngineConfigurationRuntimeValue.getValue().defaultConfiguration()))
                    .getTableFilters().dataCollectionFilter();
            Thread thread = new Thread(() -> runCdcRegistration(retrySeconds, tableFilter), "debezium-db2-cdc-setup");
            thread.setDaemon(true);
            thread.start();
        }
        catch (Exception e) {
            LOG.errorf("[CDC SETUP] CDC registration thread Failed: %s", e.getMessage());
        }
    }

    private void runCdcRegistration(int retrySeconds, TableFilter tableFilter) {
        Config config = ConfigProvider.getConfig();
        Optional<ConnectionInfo> connInfo = ConnectionInfo.from(config);
        if (connInfo.isEmpty()) {
            return;
        }

        LOG.infof("[CDC SETUP] Registering the tables the connector captures, timeout %ds.", retrySeconds);

        long deadline = System.currentTimeMillis() + (retrySeconds * 1000L);
        Connection c = acquireConnection(connInfo.get(), deadline);
        if (c == null) {
            LOG.warn("[CDC SETUP] Could not connect to DB2 within the retry window — CDC auto-registration skipped.");
            return;
        }

        try {
            Db2CdcOperations ops = new Db2CdcOperations(c);
            while (System.currentTimeMillis() < deadline) {
                if (runOneCycle(ops, tableFilter)) {
                    ops.fixStateAndReinit();
                    try {
                        Thread.sleep(3000);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            List<String> unregistered = ops.findUnregisteredUserTables().stream()
                    .filter(tableFilter::isIncluded)
                    .map(TableId::toString)
                    .collect(Collectors.toList());
            if (!unregistered.isEmpty()) {
                LOG.warnf("[CDC SETUP] Registration timed out after %ds. Still unregistered: %s", retrySeconds, unregistered);
            }
            else {
                LOG.infof("[CDC SETUP] Watcher exiting after %ds.", retrySeconds);
            }
        }
        finally {
            closeConnection(c);
        }
    }

    private boolean runOneCycle(Db2CdcOperations ops, TableFilter tableFilter) {
        boolean reinitNeeded = false;

        for (TableId tid : ops.findUnregisteredUserTables()) {
            if (tableFilter.isIncluded(tid)) {
                reinitNeeded |= ops.callAddTable(tid);
            }
        }

        for (TableId tid : ops.findInactiveRegisteredTables()) {
            if (tableFilter.isIncluded(tid)) {
                LOG.infof("[CDC SETUP] '%s'.'%s' is registered but STATE='I'; re-activating.", tid.schema(), tid.table());
                reinitNeeded = true;
            }
        }

        return reinitNeeded;
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

}
