/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.postgres.deployment;

import java.time.Duration;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class PostgresResource {

    private static final String POSTGRES_IMAGE = "quay.io/debezium/postgres:15";

    private static final DockerImageName POSTGRES_DOCKER_IMAGE_NAME = DockerImageName.parse(POSTGRES_IMAGE)
            .asCompatibleSubstituteFor("postgres");

    private static final PostgreSQLContainer<?> POSTGRES_CONTAINER = new PostgreSQLContainer<>(POSTGRES_DOCKER_IMAGE_NAME)
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
            .withEnv("POSTGRES_INITDB_ARGS", "-E UTF8")
            .withEnv("LANG", "en_US.utf8")
            .withUsername("postgres")
            .withPassword("postgres")
            .withDatabaseName("postgres")
            .withInitScript("initialize-postgres-database.sql")
            .withStartupTimeout(Duration.ofSeconds(30));

    public void start() {
        POSTGRES_CONTAINER.start();

        System.setProperty("POSTGRES_JDBC", POSTGRES_CONTAINER.getJdbcUrl());
        System.setProperty("POSTGRES_PASSWORD", POSTGRES_CONTAINER.getPassword());
        System.setProperty("POSTGRES_USERNAME", POSTGRES_CONTAINER.getUsername());
    }

    public void stop() {
        POSTGRES_CONTAINER.stop();
    }
}
