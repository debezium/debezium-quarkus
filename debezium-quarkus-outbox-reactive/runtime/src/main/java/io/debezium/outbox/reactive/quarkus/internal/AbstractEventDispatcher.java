/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.internal;

import static io.debezium.outbox.quarkus.internal.OutboxConstants.OUTBOX_ENTITY_FULLNAME;

import java.util.Map;

import jakarta.inject.Inject;

import org.hibernate.metamodel.internal.AbstractDynamicMapInstantiator;
import org.hibernate.reactive.mutiny.Mutiny;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.outbox.quarkus.ExportedEvent;
import io.debezium.outbox.quarkus.internal.AbstractEventWriter;
import io.smallrye.mutiny.Uni;

/**
 * Abstract base class for the Debezium Outbox {@link EventDispatcher} contract.
 *
 * @author Chris Cranford
 */
public abstract class AbstractEventDispatcher extends AbstractEventWriter<Uni<Void>> implements EventDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEventDispatcher.class);

    @Inject
    Mutiny.SessionFactory factory;

    /**
     * Debezium runtime configuration
     */
    @Inject
    DebeziumOutboxRuntimeConfig config;

    /**
     * Persists the outbox event, joining the session and transaction that are active in the current
     * Vert.x context. A new session and transaction are only opened when there is none.
     *
     * @param dataMap the outbox event data
     * @return a {@link Uni} that completes once the event is persisted
     */
    protected Uni<Void> persist(Map<String, Object> dataMap) {
        return factory.withTransaction((session, tx) -> persist(session, dataMap));
    }

    /**
     * Persists the outbox event with the given session, without beginning or committing a transaction.
     * If configured, the event is removed again with the same session right after the insert.
     *
     * @param session the session to use, its transaction is owned by the caller
     * @param dataMap the outbox event data
     * @return a {@link Uni} that completes once the event is persisted
     */
    protected Uni<Void> persist(Mutiny.Session session, Map<String, Object> dataMap) {
        return session.persist(dataMap)
                .invoke(() -> LOGGER.debug("outbox event persisted"))
                .call(() -> removeFromOutbox(session, dataMap));
    }

    protected Uni<Integer> removeFromOutbox(Mutiny.Session session, Map<String, Object> dataMap) {
        if (config.removeAfterInsert()) {
            LOGGER.debug("removing outbox event");
            return session.flush()
                    .chain(() -> session.createMutationQuery("delete from " + OUTBOX_ENTITY_FULLNAME + " where " + AGGREGATE_ID + "=:aggregateId")
                            .setParameter("aggregateId", dataMap.get(AGGREGATE_ID))
                            .executeUpdate());
        }
        return Uni.createFrom().item(-1);
    }

    @Override
    protected Map<String, Object> createDataMap(ExportedEvent<?, ?> event) {
        final Map<String, Object> dataMap = super.createDataMap(event);
        dataMap.put(AbstractDynamicMapInstantiator.TYPE_KEY, OUTBOX_ENTITY_FULLNAME);
        return dataMap;
    }

}
