/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.Map;
import java.util.function.Function;

import io.debezium.runtime.Connector;

/**
 *
 * {@link DebeziumConfigurationEnhancer} is used to enhance configuration with values based on
 * particular operational scenarios
 *
 */
public interface DebeziumConfigurationEnhancer extends Function<Map<String, String>, Map<String, String>> {

    /**
     * enrich a Debezium configuration with other values
     * @param configuration Debezium vanilla configuration
     * @return Debezium configuration with additional values
     */
    @Override
    Map<String, String> apply(Map<String, String> configuration);

    /**
     *
     * @return the {@link Connector} in which the Enhancer is valid
     */
    Connector applicableTo();
}
