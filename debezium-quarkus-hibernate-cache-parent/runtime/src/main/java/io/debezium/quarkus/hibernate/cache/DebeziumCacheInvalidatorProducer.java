/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.CapturingEvent;

public class DebeziumCacheInvalidatorProducer {

    private final DebeziumEvictionStrategy evictionStrategy;
    private final DebeziumFilterStrategy filterStrategy;

    @Inject
    public DebeziumCacheInvalidatorProducer(SessionFactory sessionFactory,
                                            Instance<DebeziumFilterStrategy> debeziumFilterStrategyInstance,
                                            Instance<DebeziumEvictionStrategy> evictionStrategies,
                                            PersistenceUnitRegistry persistenceUnitRegistry) {
        this.filterStrategy = debeziumFilterStrategyInstance
                .stream()
                .findFirst()
                .orElseGet(DefaultDebeziumFilterStrategy::new);

        this.evictionStrategy = evictionStrategies
                .stream()
                .findFirst()
                .orElseGet(() -> new HibernateRegionEvictionStrategy(sessionFactory, persistenceUnitRegistry));

    }

    @Produces
    public DebeziumCacheInvalidator produce() {
        return new DefaultDebeziumCacheInvalidator(evictionStrategy, filterStrategy);
    }

    private static class HibernateRegionEvictionStrategy implements DebeziumEvictionStrategy {

        private final SessionFactory sessionFactory;
        private final PersistenceUnitRegistry persistenceUnitRegistry;
        private static final Logger LOGGER = LoggerFactory.getLogger(HibernateRegionEvictionStrategy.class);

        private HibernateRegionEvictionStrategy(SessionFactory sessionFactory,
                                                PersistenceUnitRegistry persistenceUnitRegistry) {
            this.sessionFactory = sessionFactory;
            this.persistenceUnitRegistry = persistenceUnitRegistry;
        }

        @Override
        public void evict(InvalidationEvent event) {
            if (!persistenceUnitRegistry.isCached(event.engine(), event.table())) {
                LOGGER.debug("the invalidation event for table {} and unit {} is not registered for cache invalidation",
                        event.table(),
                        event.engine());
                return;
            }

            persistenceUnitRegistry.retrieve(event.engine(), event.table())
                    .ifPresentOrElse(clazz -> sessionFactory.getCache().evictEntityData(clazz), () -> {
                        LOGGER.debug("hibernate entity not found for invalidation event for table {} and unit {}",
                                event.table(),
                                event.engine());
                    });
        }
    }

    private static class DefaultDebeziumFilterStrategy implements DebeziumFilterStrategy {

        @Override
        public boolean filter(CapturingEvent<SourceRecord, SourceRecord> event) {
            return event instanceof CapturingEvent.Create ||
                    event instanceof CapturingEvent.Message ||
                    event instanceof CapturingEvent.Truncate ||
                    event instanceof CapturingEvent.Read;
        }
    }
}
