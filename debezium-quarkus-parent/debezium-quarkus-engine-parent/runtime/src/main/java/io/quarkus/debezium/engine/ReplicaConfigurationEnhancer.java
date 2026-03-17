/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

public abstract class ReplicaConfigurationEnhancer implements DebeziumConfigurationEnhancer {

    public static final String QUARKUS_DEBEZIUM_REPLICA = "quarkus.debezium.identifier.naming.strategy";
    public static final long MAX = 0x7FFF_FFFFL;
    private final Config config = ConfigProvider.getConfig();

    @Override
    public Map<String, String> apply(Map<String, String> configuration) {
        if (config.getOptionalValue(property(), String.class).isPresent()) {
            return Map.of();
        }

        Optional<Mode> replicaConfiguration = config.getOptionalValue(QUARKUS_DEBEZIUM_REPLICA, Mode.class);

        if (replicaConfiguration.isEmpty()) {
            return Map.of();
        }

        int value = switch (replicaConfiguration.get()) {
            case Mode.DEFAULT -> config
                    .getOptionalValue("HOSTNAME", String.class)
                    .map(a -> Math.abs(a.hashCode()))
                    .orElseGet(this::calculateRandomly);
            case Mode.RANDOM -> calculateRandomly();
        };

        Map<String, String> enhancedConfiguration = new HashMap<>();

        enhancedConfiguration.put(property(), String.valueOf(value));
        enhancedConfiguration.putAll(additionalValues());

        return enhancedConfiguration;
    }

    private int calculateRandomly() {
        return (int) (UUID.randomUUID().getLeastSignificantBits() & MAX);
    }

    public abstract String property();

    public Map<String, String> additionalValues() {
        return new HashMap<>();
    }

    enum Mode {
        DEFAULT, /* Use the machine HOSTNAME to generate an identifier */
        RANDOM /* random generation of an identifier */
    }
}
