/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

/**
 * Cache policy for second-level caching.
 *
 */
public enum CacheMode {
    /** No entities are ever stored in the second-level cache. */
    NONE,
    /** All entities are stored in the second-level cache. */
    ALL,
    /**
     * Only entities explicitly marked as cacheable are stored.
     * <p>Use for fine-grained control in mixed read/write systems.</p>
     */
    ENABLE_SELECTIVE,
    /**
     * All entities are cached by default except those explicitly marked as
     * non‑cacheable using {@code @Cacheable(false)}.
     * <p>
     */
    DISABLE_SELECTIVE,
    /**
     * Leaves the shared cache behavior undefined, allowing the persistence
     * provider (e.g., Hibernate) to apply its default caching policy.
     */
    UNSPECIFIED
}
