/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime.configuration;

import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigDocSection;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;
import io.smallrye.config.WithUnnamedKey;

/**
 * Build-time Debezium configuration using the legacy {@code quarkus.debezium.*} prefix.
 *
 * <p>This configuration is resolved at build time and is used by deployment processors
 * (e.g. for native-image reflection registration and DevServices).
 *
 * <p>The canonical {@code debezium.*} prefix is handled at runtime by
 * {@link DebeziumEngineConfiguration} together with
 * {@link QuarkusDebeziumConfigPrefixInterceptor}.
 */
@ConfigMapping(prefix = "quarkus.debezium")
@ConfigRoot(phase = ConfigPhase.BUILD_TIME)
public interface BuildTimeDebeziumEngineConfiguration {
    /**
     * Default configuration properties for debezium engine
     */
    @WithParentName
    @WithUnnamedKey
    Map<String, String> defaultConfiguration();

    /**
     * Configuration for capturing events
     */
    @WithName("capturing")
    Map<String, Capturing> capturing();

    /**
     * Dev Services.
     * <p>
     * Dev Services allows Quarkus to automatically start containers in dev and test mode.
     */
    @ConfigDocSection(generated = true)
    @WithName("devservices")
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
