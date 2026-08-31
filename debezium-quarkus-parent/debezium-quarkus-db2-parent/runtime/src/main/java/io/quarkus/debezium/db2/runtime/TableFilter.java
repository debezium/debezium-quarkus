/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.db2.runtime;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

record TableFilter(List<TableId> exactTables, Set<String> wildcardSchemas) {

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
