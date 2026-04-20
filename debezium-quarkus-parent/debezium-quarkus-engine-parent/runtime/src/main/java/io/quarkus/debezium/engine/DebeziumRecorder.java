/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.DebeziumConnectorRegistry;
import io.quarkus.arc.runtime.BeanContainer;
import io.quarkus.runtime.ShutdownContext;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class DebeziumRecorder {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumRecorder.class);
    private static final String PROP_ENGINE_AUTOSTART = "quarkus.debezium.engine.autostart";

    public void startEngine(ShutdownContext context, BeanContainer container) {
        boolean autostart = ConfigProvider.getConfig()
                .getOptionalValue(PROP_ENGINE_AUTOSTART, Boolean.class)
                .orElse(true);

        DebeziumConnectorRegistry debeziumConnectorRegistry = container.beanInstance(DebeziumConnectorRegistry.class);

        debeziumConnectorRegistry
                .manifests()
                .forEach(manifest -> {
                    if (autostart) {
                        debeziumConnectorRegistry.start(manifest);
                    }
                    context.addShutdownTask(() -> {
                        try {
                            debeziumConnectorRegistry.stop(manifest);
                        }
                        catch (IllegalDebeziumStateException e) {
                            // Engine may not have been started (e.g. autostart=false and never manually started)
                            LOGGER.warn("Engine was not running at shutdown for manifest: {}", manifest.id());
                        }
                    });
                });
    }
}
