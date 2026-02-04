/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class PersistentUnitRegistryRecorder {

    public Supplier<PersistenceUnitRegistry> registry(Map<String, PersistenceUnit> persistenceUnits) {
        return () -> (unit, table) -> {
            if (!persistenceUnits.containsKey(unit)) {
                return false;
            }

            Optional<JpaInformation> jpaInformation = persistenceUnits.get(unit)
                    .jpaInformation()
                    .stream().filter(jpa -> jpa.table().equals(table))
                    .findFirst();

            return jpaInformation
                    .map(JpaInformation::cached)
                    .orElse(false);
        };
    }
}
