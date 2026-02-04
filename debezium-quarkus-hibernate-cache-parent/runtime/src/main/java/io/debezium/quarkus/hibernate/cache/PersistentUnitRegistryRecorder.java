/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

import java.util.List;
import java.util.function.Supplier;

import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class PersistentUnitRegistryRecorder {

    public Supplier<PersistenceUnitRegistry> registry(List<PersistenceUnit> persistenceUnits) {
        return () -> {
            return new PersistenceUnitRegistry() {
                @Override
                public boolean isCached(String unit, String table) {
                    return false;
                }

            };
        };
    }
}
