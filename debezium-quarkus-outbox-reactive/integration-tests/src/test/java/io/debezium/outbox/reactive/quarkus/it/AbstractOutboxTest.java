/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.it;

import static io.debezium.outbox.reactive.quarkus.it.TestAsserts.assertIsType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import jakarta.inject.Inject;

import org.hibernate.metamodel.spi.MappingMetamodelImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.mutiny.Mutiny;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.outbox.quarkus.internal.OutboxConstants;
import io.quarkus.test.common.QuarkusTestResource;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;

/**
 * Abstract base class for the Debezium Outbox extension test suite.  Each subclass implementation can
 * specify custom profiles to execute the same tests with differing behavior.
 *
 * @author Chris Cranford
 */
@QuarkusTestResource(DatabaseTestResource.class)
public abstract class AbstractOutboxTest {

    @Inject
    Mutiny.SessionFactory sessionFactory;

    @Inject
    MyService myService;

    @BeforeEach
    public void clearTables() {
        sessionFactory.withTransaction(session -> session.createMutationQuery("delete from OutboxEvent").executeUpdate()
                .chain(() -> session.createMutationQuery("delete from TestEntity").executeUpdate()))
                .await().indefinitely();
    }

    @Test
    public void outboxInsertRolledBackWithCallerTransaction() {
        myService.persistEntityAndEventInCallerTransaction(false, true).await().atMost(Duration.ofSeconds(10));

        assertEquals(0L, countRows("TestEntity"));
        assertEquals(0L, countRows("OutboxEvent"));
    }

    @Test
    public void outboxInsertCommittedWithCallerTransaction() {
        myService.persistEntityAndEventInCallerTransaction(false, false).await().atMost(Duration.ofSeconds(10));

        assertEquals(1L, countRows("TestEntity"));
        assertEquals(expectedOutboxRowsAfterCommit(), countRows("OutboxEvent"));
    }

    @Test
    public void outboxInsertWithSessionRolledBackWithCallerTransaction() {
        myService.persistEntityAndEventInCallerTransaction(true, true).await().atMost(Duration.ofSeconds(10));

        assertEquals(0L, countRows("TestEntity"));
        assertEquals(0L, countRows("OutboxEvent"));
    }

    @Test
    public void outboxInsertWithSessionCommittedWithCallerTransaction() {
        myService.persistEntityAndEventInCallerTransaction(true, false).await().atMost(Duration.ofSeconds(10));

        assertEquals(1L, countRows("TestEntity"));
        assertEquals(expectedOutboxRowsAfterCommit(), countRows("OutboxEvent"));
    }

    @Test
    public void outboxInsertWithSessionRolledBackWhenCallerFails() {
        assertThrows(IllegalStateException.class,
                () -> myService.persistEntityAndEventThenFail().await().atMost(Duration.ofSeconds(10)));

        assertEquals(0L, countRows("TestEntity"));
        assertEquals(0L, countRows("OutboxEvent"));
    }

    @Test
    public void testOutboxEntityMetamodelExists() throws Exception {
        final MappingMetamodelImplementor metadata = (MappingMetamodelImplementor) sessionFactory.getMetamodel();
        final EntityPersister persister = metadata.findEntityDescriptor(OutboxConstants.OUTBOX_ENTITY_FULLNAME);
        assertNotNull(persister);
        // this assumes the default mapping settings, no custom converters or column types
        assertEquals(UUID.class, persister.getIdentifierType().getReturnedClass());
        assertIsType(persister, String.class, "aggregateType");
        assertIsType(persister, Long.class, "aggregateId");
        assertIsType(persister, String.class, "type");
        assertIsType(persister, Instant.class, "timestamp");
        assertIsType(persister, JsonNode.class, "payload");
        assertIsType(persister, String.class, "name");
        assertIsType(persister, String.class, "name_upper");
        assertIsType(persister, String.class, "name_no_columndef");
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void firedEventGetsPersistedInOutboxTable() {
        var finished = this.myService.doSomething()
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem(Duration.ofSeconds(5))
                .getItem();

        // to test jsonnode
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj;
        try {
            actualObj = mapper.readValue("{\"something\":\"Some amazing payload\"}", JsonNode.class);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        final Map row = sessionFactory.withSession(
                session -> session.createSelectionQuery("FROM OutboxEvent", Map.class).getSingleResult())
                .await().indefinitely();
        assertNotNull(row.get("id"));
        assertEquals(1L, row.get("aggregateId"));
        assertEquals("MyOutboxEvent", row.get("aggregateType"));
        assertEquals("SomeType", row.get("type"));
        assertTrue(((Instant) row.get("timestamp")).isBefore(Instant.now()));
        assertEquals(actualObj, row.get("payload"));
        assertEquals("John Doe", row.get("name"));
        assertEquals("JOHN DOE", row.get("name_upper"));
        assertEquals("Jane Doe", row.get("name_no_columndef"));
    }

    /**
     * @return the number of outbox rows expected after a single event was committed
     */
    protected long expectedOutboxRowsAfterCommit() {
        return 1L;
    }

    protected long countRows(String entityName) {
        return sessionFactory.withSession(
                session -> session.createSelectionQuery("select count(*) from " + entityName, Long.class).getSingleResult())
                .await().indefinitely();
    }
}
