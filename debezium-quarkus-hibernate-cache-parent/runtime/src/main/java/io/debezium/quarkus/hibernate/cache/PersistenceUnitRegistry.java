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
    /**
     *
     * @param unit Hibernate Persistent Unit
     * @param table Hibernate Persistent Unit table
     * @return if the specific table is cached
     */
    boolean isCached(String unit, String table);

    /**
     *
     * @param unit Hibernate Persistent Unit
     * @param table Hibernate Persistent Unit table
     * @return the class mapped to the specified table
     */
    Optional<Class<?>> retrieve(String unit, String table);
}
