/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.testsuite.deployment.suite;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.config.spi.ConfigSource;

import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import io.smallrye.config.ConfigValue;
import io.smallrye.config.common.MapBackedConfigSource;

public class DebeziumServerConfigSourceFactory implements ConfigSourceFactory {

    static final String DEBEZIUM_SOURCE_PREFIX = "debezium.source.";
    static final String QUARKUS_DEBEZIUM_PREFIX = "quarkus.debezium.";
    static final String DEBEZIUM_DATASOURCE_PREFIX = "debezium.source.datasource.";
    static final String QUARKUS_DATASOURCE_PREFIX = "quarkus.datasource.";
    static final int ORDINAL = 100;

    @Override
    public Iterable<ConfigSource> getConfigSources(ConfigSourceContext context) {
        Map<String, String> remapped = new HashMap<>();

        Iterator<String> names = context.iterateNames();
        while (names.hasNext()) {
            String name = names.next();
            ConfigValue value = context.getValue(name);
            if (value == null || value.getValue() == null) {
                continue;
            }

            if (name.startsWith(DEBEZIUM_SOURCE_PREFIX)) {
                String suffix = name.substring(DEBEZIUM_SOURCE_PREFIX.length());
                remapped.put(QUARKUS_DEBEZIUM_PREFIX + suffix, value.getValue());

                if (name.startsWith(DEBEZIUM_DATASOURCE_PREFIX)) {
                    String dsSuffix = name.substring(DEBEZIUM_DATASOURCE_PREFIX.length());
                    remapped.put(QUARKUS_DATASOURCE_PREFIX + dsSuffix, value.getValue());
                }
            }
            else if (name.startsWith(QUARKUS_DEBEZIUM_PREFIX)) {
                String suffix = name.substring(QUARKUS_DEBEZIUM_PREFIX.length());
                remapped.put(DEBEZIUM_SOURCE_PREFIX + suffix, value.getValue());
            }

            if (name.startsWith(QUARKUS_DATASOURCE_PREFIX)) {
                String dsSuffix = name.substring(QUARKUS_DATASOURCE_PREFIX.length());
                remapped.put(DEBEZIUM_DATASOURCE_PREFIX + dsSuffix, value.getValue());
            }
        }

        if (remapped.isEmpty()) {
            return Collections.emptyList();
        }

        return List.of(new DebeziumRemappedConfigSource(remapped));
    }

    static class DebeziumRemappedConfigSource extends MapBackedConfigSource {
        public DebeziumRemappedConfigSource(Map<String, String> properties) {
            super("DebeziumServerConfigSource", properties, ORDINAL);
        }
    }
}
