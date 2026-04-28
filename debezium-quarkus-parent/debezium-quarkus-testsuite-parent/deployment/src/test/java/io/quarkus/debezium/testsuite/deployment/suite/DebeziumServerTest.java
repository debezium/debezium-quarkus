/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.testsuite.deployment.suite;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import io.smallrye.config.ConfigSourceInterceptorFactory;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.configuration.DebeziumEngineConfiguration;
import io.debezium.runtime.configuration.EngineConfigurationOverride;
import io.quarkus.debezium.configuration.DebeziumServerConfiguration;
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.debezium.testsuite.deployment.TestSuiteConfigurations;
import io.quarkus.runtime.Application;
import io.quarkus.test.QuarkusUnitTest;

@Tag(SuiteTags.DEFAULT)
public class DebeziumServerTest {

    @Inject
    DebeziumConnectorRegistry connectorRegistry;

    @Inject
    HeartbeatTest.HeartbeatHandler heartbeatHandler;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .setArchiveProducer(
                    () -> ShrinkWrap.create(JavaArchive.class)
                            .addClasses(CapturingTest.CaptureProductsHandler.class, DebeziumServerInterceptorFactory.class, HeartbeatTest.class)
                            .addAsServiceProvider(ConfigSourceInterceptorFactory.class, DebeziumServerInterceptorFactory.class)
            )
            .withConfigurationResource("debezium-server-testsuite.properties");

    @Test
    @DisplayName("debezium should be integrated in the quarkus lifecycle")
    void shouldDebeziumBeIntegratedInTheQuarkusLifeCycle() {
        Assertions.assertThat(connectorRegistry.get(new EngineManifest("default")).configuration().get("connector.class"))
                .contains("io.debezium.connector");

        Awaitility.given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertThat(connectorRegistry.get(new EngineManifest("default")).status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));

        Application.currentApplication().close();

        Awaitility.given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertThat(connectorRegistry.get(new EngineManifest("default")).status())
                        .isEqualTo(new DebeziumStatus(DebeziumStatus.State.STOPPED)));
    }

    @Test
    @DisplayName("should observe heartbeat events")
    void shouldObserveHeartbeatEvents() {
        Awaitility.given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(heartbeatHandler.isInvoked()).isTrue());
    }

    @Singleton
    static class DebeziumServerConfigurationOverride implements EngineConfigurationOverride {

        @Override
        public Class<? extends DebeziumEngineConfiguration> get() {
            return DebeziumServerConfiguration.class;
        }
    }

}
