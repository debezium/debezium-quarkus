/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

import java.util.Optional;

/**
 * The registry for the cached hibernate entities found by Debezium
 *
 * @author Giovanni Panice
 */
public interface PersistenceUnitRegistry {
    boolean isCached(String unit, String table);

    Optional<Class<?>> retrieve(String unit, String table);
}
