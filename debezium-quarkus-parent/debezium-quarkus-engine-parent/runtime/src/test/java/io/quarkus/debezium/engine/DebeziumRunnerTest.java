/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.Connector;
import io.debezium.runtime.EngineManifest;

class DebeziumRunnerTest {

    private static final EngineManifest MANIFEST = new EngineManifest("engine-a");

    @Test
    @DisplayName("stopping an engine from its own thread does not block on a self-join")
    void shouldNotDeadlockWhenStoppedFromItsOwnThread() throws Exception {
        RunnableDebezium engine = mock(RunnableDebezium.class);
        when(engine.manifest()).thenReturn(MANIFEST);
        when(engine.connector()).thenReturn(new Connector("test"));

        ThreadFactory factory = DebeziumThreadHandler.getThreadFactory(engine);
        DebeziumRunner runner = new DebeziumRunner(factory, engine);

        CountDownLatch runReturned = new CountDownLatch(1);
        AtomicReference<Thread> engineThread = new AtomicReference<>();

        // Reproduce a SnapshotCompleted observer that stops its own engine: shutdown() is
        // invoked synchronously on the engine thread itself. The pre-fix code joined that
        // very thread, which stalled until the join timeout elapsed.
        doAnswer(invocation -> {
            engineThread.set(Thread.currentThread());
            runner.shutdown();
            runReturned.countDown();
            return null;
        }).when(engine).run();

        runner.start();

        assertThat(runReturned.await(5, TimeUnit.SECONDS))
                .as("shutdown() invoked on the engine thread must not block on a self-join")
                .isTrue();
        verify(engine).close();
    }
}
