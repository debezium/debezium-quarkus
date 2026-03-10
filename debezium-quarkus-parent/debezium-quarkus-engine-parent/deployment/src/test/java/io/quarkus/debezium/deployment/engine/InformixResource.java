/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import static io.debezium.testing.testcontainers.testhelper.TestInfrastructureHelper.CI_CONTAINER_STARTUP_TIME;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.MinimumDurationRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class InformixResource implements QuarkusTestResourceLifecycleManager {

    private static final ImageFromDockerfile informixImage = new ImageFromDockerfile()
            .withFileFromPath("Dockerfile", Paths.get("src/test/resources/docker/Dockerfile"))
            .withFileFromPath("informix_post_init.sh", Paths.get("src/test/resources/docker/informix_post_init.sh"))
            .withFileFromPath("testdb.sql", Paths.get("src/test/resources/docker/testdb.sql"))
            .withFileFromPath("sql", Paths.get("src/test/resources/docker/sql"));

    private static final GenericContainer<?> informix = new GenericContainer<>(informixImage)
            .withExposedPorts(9088, 9089, 27017, 27018, 27883)
            .withEnv("LICENSE", "accept")
            .waitingFor(new LogMessageWaitStrategy()
                    .withRegEx(".*Informix - Setup Completed for CDC.*")
                    .withTimes(1)
                    .withStartupTimeout(Duration.of(CI_CONTAINER_STARTUP_TIME * 3, ChronoUnit.SECONDS)))
            .withStartupCheckStrategy(new MinimumDurationRunningStartupCheckStrategy(Duration.ofSeconds(10)));

    @Override
    public Map<String, String> start() {
        informix.start();

        return Map.of(
                "quarkus.debezium.database.hostname", informix.getHost(),
                "quarkus.debezium.database.port", informix.getMappedPort(9088).toString());
    }

    public void stop() {
        informix.stop();
    }

}
