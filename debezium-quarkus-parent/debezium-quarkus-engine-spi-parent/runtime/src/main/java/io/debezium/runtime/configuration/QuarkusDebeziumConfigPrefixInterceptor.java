/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime.configuration;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import io.smallrye.config.ConfigSourceInterceptor;
import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigValue;

/**
 * The canonical Debezium Server convention uses {@code debezium.*}. The Quarkus runtime extension historically
 * used {@code quarkus.debezium.*}. This interceptor preserves backward compatibility by resolving
 * {@code debezium.*} configuration keys from their {@code quarkus.debezium.*} counterparts when the
 * canonical property is not present.
 */
public final class QuarkusDebeziumConfigPrefixInterceptor implements ConfigSourceInterceptor {

    private static final String CANONICAL_PREFIX = "debezium.";
    private static final String LEGACY_PREFIX = "quarkus.debezium.";

    @Override
    public ConfigValue getValue(ConfigSourceInterceptorContext context, String name) {
        // Prefer canonical configuration
        ConfigValue direct = context.proceed(name);
        if (direct != null) {
            return direct;
        }

        // Only attempt fallback for canonical Debezium properties
        if (!name.startsWith(CANONICAL_PREFIX)) {
            return null;
        }

        // Resolve legacy property
        String legacyName = LEGACY_PREFIX + name.substring(CANONICAL_PREFIX.length());
        ConfigValue legacy = context.proceed(legacyName);

        // If found, expose it under the canonical key
        return legacy != null ? legacy.withName(name) : null;
    }

    @Override
    public Iterator<String> iterateNames(ConfigSourceInterceptorContext context) {
        Set<String> names = new HashSet<>();
        Iterator<String> iterator = context.iterateNames();
        while (iterator.hasNext()) {
            String name = iterator.next();
            names.add(name);
            if (name.startsWith(LEGACY_PREFIX)) {
                names.add(CANONICAL_PREFIX + name.substring(LEGACY_PREFIX.length()));
            }
        }
        return names.iterator();
    }
}