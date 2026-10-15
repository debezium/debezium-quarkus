/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime.configuration;

import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigDocSection;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;

/**
 * Debezium configuration.
 */
public interface DebeziumEngineConfiguration {
    /**
     * Default Configuration properties for debezium engine
     */
    @WithName("debezium")
    Map<String, String> defaultConfiguration();

    /**
     * Configuration for capturing events
     */
    @WithName("debezium.capturing")
    Map<String, Capturing> capturing();

    /**
     * Dev Services.
     * <p>
     * Dev Services allows Quarkus to automatically start containers in dev and test mode.
     */
    @ConfigDocSection(generated = true)
    @WithName("debezium.devservices")
    Map<String, DevServicesConfig> devservices();

    interface Capturing {

        /**
         * id for the engine assigned to a datasource
         */
        Optional<String> engineId();

        /**
         * destination for which the event is intended
         */
        Optional<String> destination();

        /**
         * deserializers in a single-engine configuration
         */
        Optional<String> deserializer();

        /**
         * deserializers in a multi-engine configuration
         */
        @WithParentName
        Map<String, DeserializerConfiguration> deserializers();

        /**
         * configuration properties for debezium multi-engine
         */
        @WithParentName
        Map<String, String> configurations();

        /**
         * engine builder factory class
         */
        @WithName("engine.factory")
        Optional<String> engineFactory();

        /**
         * error handler bean name or class name
         */
        @WithName("error-handler")
        default Optional<String> errorHandler() {
            return Optional.empty();
        }

        /**
         * max retries for the retry error handler
         */
        @WithName("error-handling.max-retries")
        default Optional<Integer> maxRetries() {
            return Optional.empty();
        }

        /**
         * initial delay in milliseconds for backoff strategy
         */
        @WithName("error-handling.initial-delay-ms")
        default Optional<Long> initialDelayMs() {
            return Optional.empty();
        }

        /**
         * max delay in milliseconds for backoff strategy
         */
        @WithName("error-handling.max-delay-ms")
        default Optional<Long> maxDelayMs() {
            return Optional.empty();
        }

        /**
         * delay multiplier for exponential backoff strategy
         */
        @WithName("error-handling.delay-multiplier")
        default Optional<Double> delayMultiplier() {
            return Optional.empty();
        }
    }

    /**
     * deserializer configuration
     */
    interface DeserializerConfiguration {
        /**
         * destination for which the event is intended
         */
        String destination();

        /**
         * deserializer class for the event associated to a destination
         */
        String deserializer();
    }

}
