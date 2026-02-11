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

import org.hibernate.Hibernate;
import org.hibernate.Session;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.quarkus.hibernate.cache.deployment.entities.Fruit;
import io.debezium.quarkus.hibernate.cache.deployment.entities.Order;
import io.debezium.quarkus.hibernate.cache.deployment.entities.Product;
import io.debezium.quarkus.hibernate.cache.deployment.entities.User;
import io.debezium.quarkus.test.assertions.DebeziumAssertions;
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

    private HibernateStatistics hibernateStatistics;

    @RegisterExtension
    static QuarkusUnitTest runner = new QuarkusUnitTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(Order.class, Product.class, User.class, Fruit.class)
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
        hibernateStatistics = new HibernateStatistics(entityManager);
        hibernateStatistics.isEnabled();
    }

    @Test
    @DisplayName("given 2LC when updated an entity then update the cache region with Debezium")
    void given2LCWhenUpdatedAnEntityThenUpdateTheCache() {
        DebeziumAssertions
                .given(registry.get(EngineManifest.DEFAULT))
                .atMost(100, TimeUnit.SECONDS)
                .untilIsPolling();

        var firstTimeFetchingOrder = findDetached(1L);

        hibernateStatistics.isCacheLoad();
        assertThat(firstTimeFetchingOrder.getDescription()).isEqualTo("one");

        var secondTimefetchingOrder = findDetached(1L);

        hibernateStatistics.isCacheHit();
        assertThat(secondTimefetchingOrder.getDescription()).isEqualTo("one");

        rawUpdateOrder(1L, "another description");

        var thirdTimefetchingOrder = findDetached(1L);

        hibernateStatistics.isCacheHit();
        assertThat(thirdTimefetchingOrder.getDescription()).isEqualTo("one");

        given().await()
                .atMost(10, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(findDetached(1L)
                        .getDescription())
                        .isEqualTo("another description"));
    }

    public static class HibernateStatistics {
        private final Statistics statistics;
        private int cachePut = 0;
        private int cacheHit = 0;
        private int cacheMiss = 0;

        private HibernateStatistics(Statistics statistics) {
            statistics.clear();
            this.statistics = statistics;
        }

        public HibernateStatistics(EntityManager entityManager) {
            this(entityManager.unwrap(Session.class).getSessionFactory().getStatistics());
        }

        public void isCacheLoad() {
            assertThat(statistics.getSecondLevelCachePutCount()).isEqualTo(++cachePut);
            assertThat(statistics.getSecondLevelCacheHitCount()).isEqualTo(cacheHit);
            assertThat(statistics.getSecondLevelCacheMissCount()).isEqualTo(++cacheMiss);
        }

        public void isCacheHit() {
            assertThat(statistics.getSecondLevelCachePutCount()).isEqualTo(cachePut);
            assertThat(statistics.getSecondLevelCacheHitCount()).isEqualTo(++cacheHit);
            assertThat(statistics.getSecondLevelCacheMissCount()).isEqualTo(cacheMiss);
        }

        public void isEnabled() {
            assertThat(statistics.isStatisticsEnabled()).isTrue();
        }

        public void clear() {
            statistics.clear();
        }
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
