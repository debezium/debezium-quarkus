/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.general.single;

import static io.restassured.RestAssured.get;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.events.ConnectorStartedEvent;
import io.debezium.runtime.events.PollingStartedEvent;
import io.debezium.runtime.events.TasksStartedEvent;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;

@Tag("external-suite-only")
@QuarkusIntegrationTest
@TestProfile(ManualStartSingleEngineIT.AutostartDisabledProfile.class)
public class ManualStartSingleEngineIT {

    public static class AutostartDisabledProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.debezium.engine.autostart", "false");
        }
    }

    @Test
    @DisplayName("Debezium should not auto start engine when autostart is false")
    void shouldNotAutoStart() {
        await().untilAsserted(() -> assertThat(
                get("/engine/status")
                        .then()
                        .statusCode(200)
                        .extract().body().as(DebeziumStatus.class))
                .isEqualTo(new DebeziumStatus(DebeziumStatus.State.STOPPED)));
    }

    @Test
    @DisplayName("Debezium should start engine manually when autostart is false")
    void shouldStartManually() {
        RestAssured.given().post("/engine/start").then().statusCode(200);

        await().untilAsserted(() -> assertThat(
                get("/engine/status")
                        .then()
                        .statusCode(200)
                        .extract().body().as(DebeziumStatus.class))
                .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));
    }

    @Test
    @DisplayName("Debezium should fire lifecycle events after manual start")
    void shouldFireLifecycleEventsAfterManualStart() {
        RestAssured.given().post("/engine/start").then().statusCode(200);

        await().untilAsserted(() -> assertThat(
                get("/lifecycle-events")
                        .then()
                        .statusCode(200)
                        .extract().body().jsonPath().getList(".", String.class))
                .containsExactly(
                        ConnectorStartedEvent.class.getName(),
                        TasksStartedEvent.class.getName(),
                        PollingStartedEvent.class.getName()));
    }

    @Test
    @DisplayName("Debezium should restart engine after stop")
    void shouldRestartEngine() {
        RestAssured.given().post("/engine/start").then().statusCode(200);
        await().untilAsserted(() -> assertThat(
                get("/engine/status").then().statusCode(200)
                        .extract().body().as(DebeziumStatus.class))
                .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));

        RestAssured.given().post("/engine/stop").then().statusCode(200);
        await().untilAsserted(() -> assertThat(
                get("/engine/status").then().statusCode(200)
                        .extract().body().as(DebeziumStatus.class))
                .isEqualTo(new DebeziumStatus(DebeziumStatus.State.STOPPED)));

        RestAssured.given().post("/engine/start").then().statusCode(200);
        await().untilAsserted(() -> assertThat(
                get("/engine/status").then().statusCode(200)
                        .extract().body().as(DebeziumStatus.class))
                .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));
    }

    @Test
    @DisplayName("Debezium should shutdown gracefully when engine was never started")
    void shouldShutdownGracefullyWhenNeverStarted() {
        RestAssured.given().post("/engine/stop").then().statusCode(200);

        await().untilAsserted(() -> assertThat(
                get("/engine/status").then().statusCode(200)
                        .extract().body().as(DebeziumStatus.class))
                .isEqualTo(new DebeziumStatus(DebeziumStatus.State.STOPPED)));
    }
}
