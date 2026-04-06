/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.Debezium;

class DebeziumRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumRunner.class);

    private final ThreadFactory threadFactory;
    private final RunnableDebezium engine;
    private Thread debeziumThread;

    DebeziumRunner(ThreadFactory threadFactory, Debezium debezium) {
        this.threadFactory = threadFactory;
        this.engine = (RunnableDebezium) debezium;
    }

    public void start() {
        debeziumThread = threadFactory.newThread(engine::run);
        LOGGER.info("Starting Debezium Engine {}", debeziumThread.getName());
        debeziumThread.start();
    }

    public void shutdown() {
        if (debeziumThread == null) {
            LOGGER.warn("Shutting down before starting Debezium Engine {}", engine.manifest());
            return;
        }

        LOGGER.info("Shutting down Debezium Engine {}", debeziumThread.getName());
        try {
            engine.close();
            debeziumThread.join();
        }
        catch (IOException e) {
            throw new RuntimeException("Impossible to shutdown Debezium Engine " + debeziumThread.getName(), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while waiting for Debezium Engine {} to stop", debeziumThread.getName());
        }
        finally {
            debeziumThread.interrupt();
            LOGGER.info("Shutdown complete for Debezium Engine {}", debeziumThread.getName());
        }
    }
}
