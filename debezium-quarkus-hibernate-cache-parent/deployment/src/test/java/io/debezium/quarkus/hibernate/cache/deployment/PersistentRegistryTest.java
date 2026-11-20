/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.quarkus.hibernate.cache.PersistenceUnitRegistry;
import io.debezium.quarkus.hibernate.cache.deployment.entities.Fruit;
import io.debezium.quarkus.hibernate.cache.deployment.entities.Order;
import io.debezium.quarkus.hibernate.cache.deployment.entities.Product;
import io.debezium.quarkus.hibernate.cache.deployment.entities.User;
import io.quarkus.test.QuarkusExtensionTest;
import io.quarkus.test.common.QuarkusTestResource;

@QuarkusTestResource(PostgresResource.class)
public class PersistentRegistryTest {

    @Inject
    PersistenceUnitRegistry registry;

    @RegisterExtension
    static QuarkusExtensionTest runner = new QuarkusExtensionTest()
            .withApplicationRoot((jar) -> jar
                    .addClasses(Order.class, Product.class, User.class, Fruit.class))
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "validate")
            .overrideConfigKey("quarkus.hibernate-orm.database.default-schema", "inventory");

    @Test
    @DisplayName("should inject the persistent unit registry")
    void shouldInjectPersistentUnitRegistry() {
        assertThat(registry).isNotNull();
        assertThat(registry.isCached("NOT_AVAILABLE", "NOT_AVAILABLE")).isFalse();
    }

    @Test
    @DisplayName("given entity not cached explicitly when retrieving for it then return false")
    void givenEntityNotCachedExplicitlyWhenRetrievingForItThenReturnFalse() {
        assertThat(registry.isCached("default", "fruit")).isFalse();
    }

    @Test
    @DisplayName("given entity not cached when retrieving for it then return false")
    void givenEntityNotCachedWhenRetrievingForItThenReturnFalse() {
        assertThat(registry.isCached("default", "user")).isFalse();
    }

    @Test
    @DisplayName("given entity cached when retrieving for it then return true")
    void givenEntityCachedWhenRetrievingForItThenReturnTrue() {
        assertThat(registry.isCached("default", "order")).isTrue();
    }

    @Test
    @DisplayName("given entity cached with different name when retrieving for it then return true")
    void givenEntityCachedWithDifferentNameWhenRetrievingForItThenReturnTrue() {
        assertThat(registry.isCached("default", "PRODUCT")).isTrue();
    }
}
