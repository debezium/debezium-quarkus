/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime.configuration;

import io.smallrye.config.ConfigSourceInterceptor;
import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigValue;

/**
 * The canonical Debezium Server convention uses {@code debezium.*}. The Quarkus runtime extension historically
 * used {@code quarkus.debezium.*}. This interceptor makes existing configurations continue to work by serving
 * {@code debezium.*} values from their {@code quarkus.debezium.*} counterparts when needed.
 */
public final class QuarkusDebeziumConfigPrefixInterceptor implements ConfigSourceInterceptor {
    private static final String CANONICAL_PREFIX = "debezium.";
    private static final String LEGACY_PREFIX = "quarkus.debezium.";

    @Override
    public ConfigValue getValue(final ConfigSourceInterceptorContext context, final String name) {
        final ConfigValue direct = context.proceed(name);
        if (direct != null) {
            return direct;
        }

        if (!name.startsWith(CANONICAL_PREFIX)) {
            return null;
        }

        final String legacyName = LEGACY_PREFIX + name.substring(CANONICAL_PREFIX.length());
        final ConfigValue legacy = context.proceed(legacyName);
        if (legacy == null) {
            return null;
        }

        // Preserve the resolved value/source but expose it under the canonical key
        return ConfigValue.builder()
                .withName(name)
                .withValue(legacy.getValue())
                .withRawValue(legacy.getRawValue())
                .withProfile(legacy.getProfile())
                .withConfigSourceName(legacy.getConfigSourceName())
                .withConfigSourcePosition(legacy.getConfigSourcePosition())
                .withLineNumber(legacy.getLineNumber())
                .withProblems(legacy.getProblems())
                .build();
    }
}
