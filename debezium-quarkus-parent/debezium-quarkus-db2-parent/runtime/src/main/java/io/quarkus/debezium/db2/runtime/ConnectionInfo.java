/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.db2.runtime;

import java.util.Optional;

import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

record ConnectionInfo(String jdbcUrl, String user, String password) {

    private static final Logger LOG = Logger.getLogger(ConnectionInfo.class);

    static Optional<ConnectionInfo> from(Config config) {
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
}
