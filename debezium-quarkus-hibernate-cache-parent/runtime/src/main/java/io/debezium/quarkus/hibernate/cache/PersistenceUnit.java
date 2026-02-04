/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

import java.util.List;

public record PersistenceUnit(
        String name,
        List<JpaInformation> jpaInformation,
        CacheMode mode) {
    public enum CacheMode {
        NONE,
        ALL,
        ENABLE_SELECTIVE,
        DISABLE_SELECTIVE,
        UNSPECIFIED
    }
}
