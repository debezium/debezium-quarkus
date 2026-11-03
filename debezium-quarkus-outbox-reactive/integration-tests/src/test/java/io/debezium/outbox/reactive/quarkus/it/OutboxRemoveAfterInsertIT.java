/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration tests for the Debezium Outbox extension, removing outbox events right after they are inserted.
 */
@QuarkusTest
@TestProfile(OutboxProfiles.RemoveAfterInsert.class)
@QuarkusTestResource(DatabaseTestResource.class)
public class OutboxRemoveAfterInsertIT extends AbstractOutboxTest {

    @Override
    @Test
    public void firedEventGetsPersistedInOutboxTable() {
        myService.doSomething().await().atMost(Duration.ofSeconds(10));

        assertEquals(0L, countRows("OutboxEvent"));
    }

    @Override
    protected long expectedOutboxRowsAfterCommit() {
        return 0L;
    }
}
