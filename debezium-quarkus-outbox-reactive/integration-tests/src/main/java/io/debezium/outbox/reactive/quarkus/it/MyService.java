/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.it;

import java.util.HashMap;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.hibernate.reactive.mutiny.Mutiny;

import io.debezium.outbox.reactive.quarkus.internal.DebeziumOutboxHandler;
import io.smallrye.mutiny.Uni;

@ApplicationScoped
public class MyService {

    @Inject
    DebeziumOutboxHandler handler;

    @Inject
    Mutiny.SessionFactory sessionFactory;

    public Uni<Object> doSomething() {
        return handler.persistToOutbox(createEvent());
    }

    /**
     * Persists a {@link TestEntity} and an outbox event in a transaction started by the caller.
     *
     * @param passSession whether the caller's session is handed to the outbox handler explicitly
     * @param rollback whether the caller's transaction should be rolled back
     */
    public Uni<Void> persistEntityAndEventInCallerTransaction(boolean passSession, boolean rollback) {
        return sessionFactory.withTransaction((session, tx) -> session.persist(new TestEntity())
                .chain(() -> passSession
                        ? handler.persistToOutbox(createEvent(), session)
                        : handler.persistToOutbox(createEvent()).replaceWithVoid())
                .invoke(() -> {
                    if (rollback) {
                        tx.markForRollback();
                    }
                }));
    }

    /**
     * Persists a {@link TestEntity} and an outbox event with the caller's session, then fails
     * the caller's transaction.
     */
    public Uni<Void> persistEntityAndEventThenFail() {
        return sessionFactory.withTransaction(session -> session.persist(new TestEntity())
                .chain(() -> handler.persistToOutbox(createEvent(), session))
                .chain(() -> Uni.createFrom().<Void> failure(new IllegalStateException("Simulated business failure"))));
    }

    private MyOutboxEvent createEvent() {
        final Map<String, Object> values = new HashMap<>();
        values.put("name", "John Doe"); // illustrates additional field with no converter
        values.put("name_upper", "John Doe"); // illustrates additional field with converter
        values.put("name_no_columndef", "Jane Doe"); // illustrates default behavior with no column definition specified
        return new MyOutboxEvent(values);
    }
}
