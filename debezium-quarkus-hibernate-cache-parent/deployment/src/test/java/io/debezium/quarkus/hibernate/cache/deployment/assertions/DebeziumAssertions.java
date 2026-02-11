/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache.deployment.assertions;

import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumStatus;

public class DebeziumAssertions {
    private DebeziumAssertions() {
    }

    public static Condition given(Debezium debezium) {
        return new Condition(debezium);
    }

    public static class Condition {
        private final Debezium debezium;
        private final long timeout;
        private final TimeUnit timeUnit;

        public Condition(Debezium debezium, long timeout, TimeUnit timeUnit) {
            this.debezium = debezium;
            this.timeout = timeout;
            this.timeUnit = timeUnit;
        }

        public Condition(Debezium debezium) {
            this.debezium = debezium;
            this.timeout = 100;
            this.timeUnit = TimeUnit.SECONDS;
        }

        public Condition atMost(long timeout, TimeUnit timeUnit) {
            return new Condition(this.debezium, timeout, timeUnit);
        }

        public void untilIsPolling() {
            Awaitility.given().atMost(timeout, timeUnit)
                    .untilAsserted(() -> Assertions.assertThat(debezium
                            .status()
                            .state())
                            .isEqualTo(DebeziumStatus.State.POLLING));
        }
    }
}
