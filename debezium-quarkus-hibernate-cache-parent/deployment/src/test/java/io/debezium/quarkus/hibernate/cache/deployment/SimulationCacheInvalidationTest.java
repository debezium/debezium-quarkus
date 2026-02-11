/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.given;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.quarkus.hibernate.cache.deployment.assertions.DebeziumAssertions;
import io.debezium.quarkus.hibernate.cache.deployment.entities.Fruit;
import io.debezium.quarkus.hibernate.cache.deployment.entities.Order;
import io.debezium.quarkus.hibernate.cache.deployment.entities.Product;
import io.debezium.quarkus.hibernate.cache.deployment.entities.User;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.EngineManifest;
import io.quarkus.test.QuarkusUnitTest;
import io.quarkus.test.common.QuarkusTestResource;

@QuarkusTestResource(PostgresResource.class)
public class SimulationCacheInvalidationTest {

    @Inject
    EntityManager entityManager;

    @Inject
    javax.sql.DataSource dataSource;

    @Inject
    DebeziumConnectorRegistry registry;

    private SessionFactory sessionFactory;
    private Statistics stats;

    @RegisterExtension
    static QuarkusUnitTest runner = new QuarkusUnitTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(Order.class, Product.class, User.class, Fruit.class,
                            DebeziumAssertions.class, DebeziumAssertions.Condition.class)
                    .addAsResource("ehcache.xml", "ehcache.xml"))
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "validate")
            .overrideConfigKey("quarkus.hibernate-orm.database.default-schema", "inventory")
            .overrideConfigKey("quarkus.hibernate-orm.\"hibernate.cache.use_second_level_cache\"", "true")
            .overrideConfigKey("quarkus.hibernate-orm.\"hibernate.cache.region.factory_class\"", "jcache")
            .overrideConfigKey("quarkus.hibernate-orm.\"hibernate.javax.cache.provider\"", "org.ehcache.jsr107.EhcacheCachingProvider")
            .overrideConfigKey("quarkus.hibernate-orm.\"hibernate.javax.cache.uri\"", "ehcache.xml")
            .overrideConfigKey("quarkus.hibernate-orm.\"hibernate.javax.cache.missing_cache_strategy\"", "create")
            .overrideConfigKey("quarkus.hibernate-orm.statistics", "true")
            .overrideConfigKey("quarkus.hibernate-orm.\"hibernate.generate_statistics\"", "true");

    @BeforeEach
    void setUp() {
        sessionFactory = entityManager.unwrap(Session.class).getSessionFactory();
        stats = sessionFactory.getStatistics();
        stats.clear();

        assertThat(stats.isStatisticsEnabled()).isTrue();
    }

    @Test
    @DisplayName("given 2LC when updated an entity then update the cache region with Debezium")
    void given2LCWhenUpdatedAnEntityThenUpdateTheCache() {
        DebeziumAssertions
                .given(registry.get(EngineManifest.DEFAULT))
                .atMost(100, TimeUnit.SECONDS)
                .untilIsPolling();

        var firstTimeFetchingOrder = findDetached(1L);

        assertThat(firstTimeFetchingOrder).isNotNull();

        assertThat(stats.getSecondLevelCachePutCount()).isEqualTo(1);
        assertThat(stats.getSecondLevelCacheHitCount()).isEqualTo(0);
        assertThat(stats.getSecondLevelCacheMissCount()).isEqualTo(1);
        assertThat(firstTimeFetchingOrder.getDescription()).isEqualTo("one");

        var secondTimefetchingOrder = findDetached(1L);

        assertThat(secondTimefetchingOrder).isNotNull();

        assertThat(stats.getSecondLevelCachePutCount()).isEqualTo(1);
        assertThat(stats.getSecondLevelCacheHitCount()).isEqualTo(1);
        assertThat(stats.getSecondLevelCacheMissCount()).isEqualTo(1);
        assertThat(firstTimeFetchingOrder.getDescription()).isEqualTo("one");

        rawUpdateOrder(1L, "another description");

        var thirdTimefetchingOrder = findDetached(1L);

        assertThat(thirdTimefetchingOrder.getDescription()).isEqualTo("one");
        assertThat(stats.getSecondLevelCachePutCount()).isEqualTo(1);
        assertThat(stats.getSecondLevelCacheHitCount()).isEqualTo(2);
        assertThat(stats.getSecondLevelCacheMissCount()).isEqualTo(1);

        given().await()
                .atMost(10, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(findDetached(1L)
                        .getDescription())
                        .isEqualTo("another description"));
    }

    @Transactional
    Order findDetached(Long id) {
        Order order = entityManager.find(Order.class, id);
        entityManager.detach(order);

        return order;
    }

    void rawUpdateOrder(Long id, String description) {
        try (var conn = dataSource.getConnection();
                var ps = conn.prepareStatement("UPDATE inventory.order SET description=? WHERE id=?")) {
            ps.setString(1, description);
            ps.setLong(2, id);
            ps.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
