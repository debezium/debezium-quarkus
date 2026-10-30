/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.Connector;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.EngineManifest;

class DebeziumThreadHandlerTest {

    private static final EngineManifest ENGINE_A = new EngineManifest("engine-a");
    private static final EngineManifest ENGINE_B = new EngineManifest("engine-b");

    @Test
    @DisplayName("a thread outside any engine is not an engine thread")
    void shouldNotBeAnEngineThreadWhenNoContextIsSet() {
        assertThat(DebeziumThreadHandler.isEngineThread(ENGINE_A)).isFalse();
    }

    @Test
    @DisplayName("an engine thread matches only its own manifest, not another engine's")
    void shouldMatchOnlyItsOwnManifest() throws InterruptedException {
        AtomicBoolean matchesSelf = new AtomicBoolean();
        AtomicBoolean matchesOther = new AtomicBoolean();

        Thread engineThread = engineThreadFor(ENGINE_A, () -> {
            matchesSelf.set(DebeziumThreadHandler.isEngineThread(ENGINE_A));
            matchesOther.set(DebeziumThreadHandler.isEngineThread(ENGINE_B));
        });
        engineThread.start();
        engineThread.join();

        assertThat(matchesSelf).isTrue();
        assertThat(matchesOther).isFalse();
    }

    @Test
    @DisplayName("a worker thread spawned by an engine thread inherits the engine context")
    void shouldInheritContextOnChildThreads() throws InterruptedException {
        AtomicBoolean childMatches = new AtomicBoolean();

        Thread engineThread = engineThreadFor(ENGINE_A, () -> {
            Thread child = new Thread(() -> childMatches.set(DebeziumThreadHandler.isEngineThread(ENGINE_A)));
            child.start();
            try {
                child.join();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        engineThread.start();
        engineThread.join();

        assertThat(childMatches).isTrue();
    }

    private static Thread engineThreadFor(EngineManifest manifest, Runnable task) {
        Debezium debezium = mock(Debezium.class);
        when(debezium.manifest()).thenReturn(manifest);
        when(debezium.connector()).thenReturn(new Connector("test"));
        ThreadFactory factory = DebeziumThreadHandler.getThreadFactory(debezium);
        return factory.newThread(task);
    }
}
