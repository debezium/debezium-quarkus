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
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
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

    /**
     * Persists a {@link TestEntity} and an outbox event in a transaction started with
     * {@link Panache#withTransaction}, without passing the session to the outbox handler.
     *
     * @param fail whether the caller's transaction should fail and be rolled back
     */
    public Uni<Void> persistEntityAndEventInPanacheTransaction(boolean fail) {
        return Panache.withTransaction(() -> persistEntityAndEventWithPanacheSession()
                .chain(() -> failIfRequested(fail)));
    }

    /**
     * Persists a {@link TestEntity} and an outbox event in a transaction started by {@link WithTransaction},
     * without passing the session to the outbox handler.
     *
     * @param fail whether the caller's transaction should fail and be rolled back
     */
    @WithTransaction
    public Uni<Void> persistEntityAndEventWithTransactionAnnotation(boolean fail) {
        return persistEntityAndEventWithPanacheSession()
                .chain(() -> failIfRequested(fail));
    }

    private Uni<Void> persistEntityAndEventWithPanacheSession() {
        return Panache.getSession()
                .chain(session -> session.persist(new TestEntity()))
                .chain(() -> handler.persistToOutbox(createEvent()))
                .replaceWithVoid();
    }

    private MyOutboxEvent createEvent() {
        final Map<String, Object> values = new HashMap<>();
        values.put("name", "John Doe"); // illustrates additional field with no converter
        values.put("name_upper", "John Doe"); // illustrates additional field with converter
        values.put("name_no_columndef", "Jane Doe"); // illustrates default behavior with no column definition specified
        return new MyOutboxEvent(values);
    }

    private static Uni<Void> failIfRequested(boolean fail) {
        return fail
                ? Uni.createFrom().failure(new IllegalStateException("Simulated business failure"))
                : Uni.createFrom().voidItem();
    }
}
