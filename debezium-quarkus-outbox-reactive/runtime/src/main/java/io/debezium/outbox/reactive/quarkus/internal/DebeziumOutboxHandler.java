/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.internal;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.hibernate.reactive.mutiny.Mutiny;

import io.debezium.outbox.quarkus.ExportedEvent;
import io.smallrye.mutiny.Uni;

/**
 * Entry point for writing {@link ExportedEvent} events to the outbox table.
 *
 * The outbox write is executed in the caller's Vert.x context, so that it takes part in the
 * caller's transaction and is committed or rolled back together with the caller's changes.
 */
@ApplicationScoped
public class DebeziumOutboxHandler {

    @Inject
    EventDispatcher dispatcher;

    /**
     * Persists the event to the outbox table.
     *
     * If a Hibernate Reactive session and transaction were opened by {@link Mutiny.SessionFactory} in the
     * current Vert.x context, for example with {@link Mutiny.SessionFactory#withTransaction}, the write joins
     * them. Otherwise, the event is persisted in a new session and transaction.
     *
     * Sessions that are not opened through {@link Mutiny.SessionFactory}, such as the one managed by Hibernate
     * Reactive with Panache, are not guaranteed to be found; pass the session explicitly with
     * {@link #persistToOutbox(ExportedEvent, Mutiny.Session)} in that case.
     *
     * @param incomingEvent the event to persist
     * @return a {@link Uni} that completes once the event is persisted
     */
    public Uni<Object> persistToOutbox(ExportedEvent<?, ?> incomingEvent) {
        return dispatcher.onExportedEvent(incomingEvent).onItem().castTo(Object.class);
    }

    /**
     * Persists the event to the outbox table using the given session.
     *
     * No transaction is started or committed; the event is written as part of the transaction that the
     * session is in, and is committed or rolled back with it.
     *
     * @param incomingEvent the event to persist
     * @param session the caller's session
     * @return a {@link Uni} that completes once the event is persisted
     */
    public Uni<Void> persistToOutbox(ExportedEvent<?, ?> incomingEvent, Mutiny.Session session) {
        return dispatcher.onExportedEvent(incomingEvent, session);
    }
}
