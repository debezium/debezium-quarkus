/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.deployment.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.DebeziumException;
import io.quarkus.test.QuarkusUnitTest;

/**
 * A {@code connector.class} that cannot be loaded is a configuration error: the application must
 * fail to start instead of leaving the engine silently unstarted.
 */
public class UnknownConnectorClassTest {

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .overrideConfigKey("quarkus.debezium.engine.autostart", "false")
            .overrideConfigKey("quarkus.debezium.connector.class", "io.debezium.connector.does.not.Exist")
            .overrideConfigKey("quarkus.debezium.name", "unknown")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "unknown")
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .assertException(throwable -> assertThat(throwable)
                    .hasStackTraceContaining(DebeziumException.class.getName())
                    .hasStackTraceContaining("Unable to load connector class 'io.debezium.connector.does.not.Exist'"));

    @Test
    @DisplayName("should fail to start when the configured connector class cannot be loaded")
    void shouldFailToStart() {
        fail("the application must not start");
    }
}
