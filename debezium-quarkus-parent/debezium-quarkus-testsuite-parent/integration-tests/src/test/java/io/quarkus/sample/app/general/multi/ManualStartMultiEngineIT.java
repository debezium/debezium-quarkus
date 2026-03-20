/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.general.multi;

import static io.restassured.RestAssured.get;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.runtime.DebeziumStatus;
import io.quarkus.sample.app.conditions.DisableIfSingleEngine;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;

@Tag("external-suite-only")
@QuarkusIntegrationTest
@TestProfile(ManualStartMultiEngineIT.AutostartDisabledProfile.class)
public class ManualStartMultiEngineIT {
    public static class AutostartDisabledProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.debezium.engine.autostart", "false");
        }
    }

    @Test
    @DisplayName("Debezium should not auto start engines when autostart is false")
    @DisableIfSingleEngine
    void shouldNotAutoStart() {
        await().untilAsserted(() -> assertThat(
                get("/engine/statuses")
                        .then()
                        .statusCode(200)
                        .extract().jsonPath().getList("state", String.class)
                        .stream()
                        .allMatch(s -> s.equals("STOPPED")))
                .isTrue());
    }

    @Test
    @DisplayName("Debezium should start single engine manually")
    @DisableIfSingleEngine
    void shouldStartSingleEngineManually() {

        RestAssured.given().post("/engine/start/default").then().statusCode(200);

        await().untilAsserted(() -> assertThat(
                get("/engine/status")
                        .then()
                        .statusCode(200)
                        .extract().body().as(DebeziumStatus.class))
                .isEqualTo(new DebeziumStatus(DebeziumStatus.State.POLLING)));

        await().untilAsserted(() -> assertThat(
                get("/engine/statuses")
                        .then()
                        .statusCode(200)
                        .extract().jsonPath().getList("state", String.class)
                        .stream()
                        .filter(s -> s.equals("STOPPED"))
                        .count())
                .isEqualTo(1L));
    }

    @Test
    @DisplayName("Debezium should start all engines manually")
    @DisableIfSingleEngine
    void shouldStartAllEnginesManually() {
        RestAssured.given().post("/engine/start").then().statusCode(200);

        await().untilAsserted(() -> assertThat(
                get("/engine/statuses")
                        .then()
                        .statusCode(200)
                        .extract().jsonPath().getList("state", String.class)
                        .stream()
                        .allMatch(s -> s.equals("POLLING")))
                .isTrue());
    }

    @Test
    @DisplayName("Debezium should stop single engine and keep other polling")
    @DisableIfSingleEngine
    void shouldStopSingleEngine() {
        RestAssured.given().post("/engine/start").then().statusCode(200);

        await().untilAsserted(() -> assertThat(
                get("/engine/statuses")
                        .then()
                        .statusCode(200)
                        .extract().jsonPath().getList("state", String.class)
                        .stream()
                        .allMatch(s -> s.equals("POLLING")))
                .isTrue());

        RestAssured.given().post("/engine/stop/default").then().statusCode(200);

        await().untilAsserted(() -> assertThat(
                get("/engine/statuses")
                        .then()
                        .statusCode(200)
                        .extract().jsonPath().getList("state", String.class)
                        .stream()
                        .filter(s -> s.equals("STOPPED"))
                        .count())
                .isEqualTo(1L));

        await().untilAsserted(() -> assertThat(
                get("/engine/statuses")
                        .then()
                        .statusCode(200)
                        .extract().jsonPath().getList("state", String.class)
                        .stream()
                        .filter(s -> s.equals("POLLING"))
                        .count())
                .isEqualTo(1L));
    }

    @Test
    @DisplayName("Debezium should stop all engines manually")
    @DisableIfSingleEngine
    void shouldStopAllEnginesManually() {
        RestAssured.given().post("/engine/start").then().statusCode(200);

        await().untilAsserted(() -> assertThat(
                get("/engine/statuses")
                        .then()
                        .statusCode(200)
                        .extract().jsonPath().getList("state", String.class)
                        .stream()
                        .allMatch(s -> s.equals("POLLING")))
                .isTrue());

        RestAssured.given().post("/engine/stop").then().statusCode(200);

        await().untilAsserted(() -> assertThat(
                get("/engine/statuses")
                        .then()
                        .statusCode(200)
                        .extract().jsonPath().getList("state", String.class)
                        .stream()
                        .allMatch(s -> s.equals("STOPPED")))
                .isTrue());
    }

}
