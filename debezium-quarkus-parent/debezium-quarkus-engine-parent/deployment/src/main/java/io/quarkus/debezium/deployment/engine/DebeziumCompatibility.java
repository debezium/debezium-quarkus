/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import java.util.function.BooleanSupplier;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Debezium Compatibility configuration
 */
@ConfigMapping(prefix = "debezium.deployment")
@ConfigRoot(phase = ConfigPhase.BUILD_TIME)
public interface DebeziumCompatibility {

    /**
     * global flag that can be used to tweak extension configuration for debezium server compatibility
     */
    @WithDefault("false")
    boolean server();

    class DebeziumServerEnabled implements BooleanSupplier {

        final DebeziumCompatibility debeziumCompatibility;

        public DebeziumServerEnabled(DebeziumCompatibility debeziumCompatibility) {
            this.debeziumCompatibility = debeziumCompatibility;
        }

        @Override
        public boolean getAsBoolean() {
            return debeziumCompatibility.server();
        }
    }

}
