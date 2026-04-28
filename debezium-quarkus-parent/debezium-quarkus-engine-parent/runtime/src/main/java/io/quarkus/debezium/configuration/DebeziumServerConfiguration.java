/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.configuration;

import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "debezium.source")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface DebeziumServerConfiguration extends DebeziumEngineConfiguration {
}
