/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import java.util.function.Predicate;

/**
 * Strategy for filtering {@link Debezium} engine instances before they are started.
 * <p>
 * Implement this interface as a CDI bean to control which engine instances should be
 * allowed to run. The {@link #test(Debezium)} method is called for each engine during
 * initialization; returning {@code false} prevents the engine from starting.
 * <p>
 * If no custom implementation is provided, {@link #DEFAULT} is used, which allows all
 * engines to run.
 *
 * @see Debezium
 */
public interface DebeziumEngineFilterStrategy extends Predicate<Debezium> {
    boolean test(Debezium debezium);

    DebeziumEngineFilterStrategy DEFAULT = debezium -> true;
}
