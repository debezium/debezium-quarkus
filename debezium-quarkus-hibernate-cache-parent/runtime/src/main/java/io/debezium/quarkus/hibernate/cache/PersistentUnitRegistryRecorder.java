/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.quarkus.debezium.engine.Quarkus;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class PersistentUnitRegistryRecorder {

    public Supplier<PersistenceUnitRegistry> registry(Map<String, RawPersistenceUnit> rawPersistenceUnits) {
        Map<String, PersistenceUnit> units = rawPersistenceUnits
                .entrySet()
                .stream()
                .map(entry -> Map.entry(sanitize(entry.getKey()),
                        new PersistenceUnit(
                                sanitize(entry.getValue().name()),
                                entry.getValue().rawJpaInfo()
                                        .stream()
                                        .map(raw -> new PersistenceUnit.JpaInfo(
                                                raw.name(),
                                                raw.table(),
                                                raw.hibernateId(),
                                                raw.hibernateIdType(),
                                                raw.cached(),
                                                sanitize(raw.persistentUnit()),
                                                loadClass(raw.fqcn())))
                                        .toList(),
                                entry.getValue().mode())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return () -> new PersistenceUnitRegistry() {
            @Override
            public boolean isCached(String unit, String table) {
                if (!units.containsKey(unit)) {
                    return false;
                }

                Optional<PersistenceUnit.JpaInfo> jpaInformation = retrieveJpa(unit, table);

                return jpaInformation
                        .map(PersistenceUnit.JpaInfo::cached)
                        .orElse(false);
            }

            @Override
            public Optional<Class<?>> retrieve(String unit, String table) {
                return retrieveJpa(unit, table)
                        .map(PersistenceUnit.JpaInfo::clazz);
            }

            private Optional<PersistenceUnit.JpaInfo> retrieveJpa(String unit, String table) {
                return units.get(unit).infos()
                        .stream()
                        .filter(info -> info.table().equals(table))
                        .findFirst();
            }
        };
    }

    private String sanitize(String value) {
        return value.replaceAll(Quarkus.QUARKUS_DATASOURCE_BRACKETS, "");
    }

    private <T> Class<T> loadClass(String name) {
        try {
            return (Class<T>) Class.forName(name, false, Thread.currentThread().getContextClassLoader());
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
