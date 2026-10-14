/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.inject.Qualifier;

/**
 * Qualifies the {@link io.debezium.runtime.DebeziumConnectorRegistry} of the connector resolved from
 * {@code connector.class} at runtime in compatibility mode, so that it never takes part in the
 * resolution of an unqualified {@code DebeziumConnectorRegistry} injection point: an application with
 * a single connector keeps injecting its registry directly.
 */
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD })
public @interface ConfiguredConnector {
}
