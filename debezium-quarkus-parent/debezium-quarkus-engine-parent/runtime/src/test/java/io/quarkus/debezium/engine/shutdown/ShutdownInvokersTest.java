/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.shutdown;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.engine.DebeziumEngine;
import io.debezium.runtime.DebeziumShutdown.Phase;

class ShutdownInvokersTest {

    @Test
    @DisplayName("should set only the strategy of the declared phase")
    void shouldSetOnlyTheDeclaredPhase() {
        DebeziumEngine.Shutdown<SourceRecord> shutdown = ShutdownInvokers.create(List.of(new CountingInvoker(Phase.AFTER, true)));

        assertThat(shutdown.before()).isNull();
        assertThat(shutdown.after()).isNotNull();
    }

    @Test
    @DisplayName("should stop once: after the first match the annotated method is no longer called")
    void shouldStopOnce() {
        CountingInvoker after = new CountingInvoker(Phase.AFTER, true);
        DebeziumEngine.Shutdown<SourceRecord> shutdown = ShutdownInvokers.create(List.of(after));

        assertThat(shutdown.after().test(context(record("topic.a")))).isTrue();
        assertThat(shutdown.after().test(context(record("topic.b")))).isFalse();
        assertThat(after.calls()).isEqualTo(1);
    }

    @Test
    @DisplayName("should not evaluate AFTER once BEFORE has stopped the engine")
    void shouldShareTheStopBetweenPhases() {
        CountingInvoker before = new CountingInvoker(Phase.BEFORE, true);
        CountingInvoker after = new CountingInvoker(Phase.AFTER, true);
        DebeziumEngine.Shutdown<SourceRecord> shutdown = ShutdownInvokers.create(List.of(before, after));

        assertThat(shutdown.before().test(context(record("topic.a")))).isTrue();
        assertThat(shutdown.after().test(context(record("topic.a")))).isFalse();
        assertThat(after.calls()).isZero();
    }

    @Test
    @DisplayName("should keep evaluating while the method returns false")
    void shouldKeepEvaluatingUntilMatch() {
        CountingInvoker after = new CountingInvoker(Phase.AFTER, false);
        DebeziumEngine.Shutdown<SourceRecord> shutdown = ShutdownInvokers.create(List.of(after));

        assertThat(shutdown.after().test(context(record("topic.a")))).isFalse();
        assertThat(shutdown.after().test(context(record("topic.b")))).isFalse();
        assertThat(after.calls()).isEqualTo(2);
    }

    @Test
    @DisplayName("should reject more than one invoker for the same phase")
    void shouldRejectDuplicatePhase() {
        assertThatThrownBy(() -> ShutdownInvokers.create(List.of(new CountingInvoker(Phase.AFTER, true), new CountingInvoker(Phase.AFTER, true))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only one @DebeziumShutdown(AFTER) method is allowed");
    }

    private static SourceRecord record(String topic) {
        return new SourceRecord(null, null, topic, null, "value");
    }

    private static DebeziumEngine.Shutdown.ShutdownContext<SourceRecord> context(SourceRecord record) {
        return new DebeziumEngine.Shutdown.ShutdownContext<>() {
            @Override
            public Optional<String> configuration(String key) {
                return Optional.empty();
            }

            @Override
            public SourceRecord record() {
                return record;
            }
        };
    }

    private static final class CountingInvoker implements ShutdownInvoker {
        private final Phase phase;
        private final boolean result;
        private final AtomicInteger calls = new AtomicInteger();

        private CountingInvoker(Phase phase, boolean result) {
            this.phase = phase;
            this.result = result;
        }

        @Override
        public Phase phase() {
            return phase;
        }

        @Override
        public boolean shouldShutdown(SourceRecord record) {
            calls.incrementAndGet();
            return result;
        }

        int calls() {
            return calls.get();
        }
    }
}
