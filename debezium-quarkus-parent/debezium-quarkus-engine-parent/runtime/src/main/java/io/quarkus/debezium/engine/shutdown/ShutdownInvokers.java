/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.shutdown;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.embedded.async.DebeziumShutdown;
import io.debezium.engine.DebeziumEngine;
import io.debezium.runtime.DebeziumShutdown.Phase;

/**
 * Connects the {@link io.debezium.runtime.DebeziumShutdown} methods to the engine builder.
 */
public final class ShutdownInvokers {

    private ShutdownInvokers() {
    }

    /**
     * Sets the shutdown strategies on the builder. Without invokers the builder is returned untouched, because some
     * builder factories do not support {@link DebeziumEngine.Builder#shutdown(DebeziumEngine.Shutdown)}.
     *
     * @param builder the engine builder
     * @param invokers the invokers of the annotated methods; at most one per phase
     * @return the same builder, for chaining
     * @param <R> the record type of the engine
     */
    public static <R> DebeziumEngine.Builder<R> apply(DebeziumEngine.Builder<R> builder, List<ShutdownInvoker> invokers) {
        if (invokers.isEmpty()) {
            return builder;
        }

        return builder.shutdown(create(invokers));
    }

    /**
     * The engine evaluates every record of a batch, also after a match, and stops itself again on every further match.
     * The strategies share a flag, so that the engine is stopped once: after the first match the annotated methods are
     * no longer called.
     */
    static <R> DebeziumEngine.Shutdown<R> create(List<ShutdownInvoker> invokers) {
        DebeziumShutdown.Builder<R> builder = new DebeziumShutdown.Builder<>();
        AtomicBoolean stopping = new AtomicBoolean(false);

        ShutdownInvoker before = find(invokers, Phase.BEFORE);
        if (before != null) {
            builder.before().custom(strategy(before, stopping));
        }

        ShutdownInvoker after = find(invokers, Phase.AFTER);
        if (after != null) {
            builder.after().custom(strategy(after, stopping));
        }

        return builder.build();
    }

    private static ShutdownInvoker find(List<ShutdownInvoker> invokers, Phase phase) {
        List<ShutdownInvoker> matching = invokers.stream()
                .filter(invoker -> invoker.phase() == phase)
                .toList();

        if (matching.size() > 1) {
            throw new IllegalStateException("only one @DebeziumShutdown(" + phase + ") method is allowed, found " + matching.size());
        }

        return matching.isEmpty() ? null : matching.getFirst();
    }

    private static <R> DebeziumEngine.ShutdownStrategy<DebeziumEngine.Shutdown.ShutdownContext<R>> strategy(ShutdownInvoker invoker,
                                                                                                            AtomicBoolean stopping) {
        return context -> !stopping.get()
                && invoker.shouldShutdown(toSourceRecord(context.record()))
                && stopping.compareAndSet(false, true);
    }

    /**
     * The engine record is a {@link SourceRecord} for the plain Connect format and an {@link EmbeddedEngineChangeEvent}
     * when the records are converted (e.g. the Quarkus consumers): the annotated method always receives the source record.
     */
    static SourceRecord toSourceRecord(Object record) {
        if (record instanceof EmbeddedEngineChangeEvent<?, ?, ?> event) {
            return event.sourceRecord();
        }

        return (SourceRecord) record;
    }
}
