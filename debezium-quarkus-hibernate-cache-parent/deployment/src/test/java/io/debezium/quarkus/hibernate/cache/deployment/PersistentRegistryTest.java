/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache.deployment;

import jakarta.inject.Inject;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.quarkus.hibernate.cache.PersistenceUnitRegistry;
import io.debezium.quarkus.hibernate.cache.deployment.entities.Fruit;
import io.debezium.quarkus.hibernate.cache.deployment.entities.Order;
import io.debezium.quarkus.hibernate.cache.deployment.entities.Product;
import io.debezium.quarkus.hibernate.cache.deployment.entities.User;
import io.quarkus.test.QuarkusUnitTest;
import io.quarkus.test.common.QuarkusTestResource;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTestResource(PostgresResource.class)
public class PersistentRegistryTest {

    @Inject
    PersistenceUnitRegistry registry;

    @RegisterExtension
    static QuarkusUnitTest runner = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar
                    .addClasses(Order.class, Product.class, User.class, Fruit.class))
            .withConfigurationResource("application.properties");

    @Test
    @DisplayName("should inject the persistent unit registry")
    void shouldInjectPersistentUnitRegistry() {
        assertThat(registry).isNotNull();
        assertThat(registry.isCached("NOT_AVAILABLE", "NOT_AVAILABLE")).isFalse();
    }

    @Test
    @DisplayName("given entity not cached when retrieving for it then return false")
    void givenEntityNotCachedWhenRetrievingForItThenReturnFalse() {
        assertThat(registry.isCached("<default>", "Fruit")).isFalse();
    }
}
