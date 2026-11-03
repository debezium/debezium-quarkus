/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.internal;

import org.hibernate.reactive.mutiny.Mutiny;

import io.debezium.outbox.quarkus.ExportedEvent;
import io.smallrye.mutiny.Uni;

/**
 * Contract for a Debezium Outbox event dispatcher.
 *
 * @author Chris Cranford
 */
public interface EventDispatcher {
    /**
     * Persists the {@link ExportedEvent} to the outbox table.
     *
     * The write joins the session and transaction that are active in the current Vert.x context,
     * if any; otherwise a new session and transaction are opened for the write.
     *
     * @param event
     *            the exported event
     */
    Uni<Void> onExportedEvent(Object event);

    /**
     * Persists the {@link ExportedEvent} to the outbox table using the supplied session.
     *
     * The write is part of whatever transaction the session is in; the dispatcher does neither
     * begin nor commit a transaction, so the outbox row is committed or rolled back together
     * with the caller's changes.
     *
     * @param event
     *            the exported event
     * @param session
     *            the caller's reactive session
     */
    default Uni<Void> onExportedEvent(ExportedEvent<?, ?> event, Mutiny.Session session) {
        throw new UnsupportedOperationException("Persisting an outbox event with an explicit session is not supported by " + getClass().getName());
    }
}
