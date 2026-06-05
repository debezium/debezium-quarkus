/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.heartbeat;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import jakarta.enterprise.event.Event;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.connector.SnapshotRecord;
import io.debezium.engine.DebeziumEngine;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.runtime.Connector;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.events.DebeziumHeartbeat;
import io.debezium.spi.schema.DataCollectionId;

class QuarkusHeartbeatEmitterTest {

    public static final DebeziumStatus DEBEZIUM_STATUS = new DebeziumStatus(DebeziumStatus.State.POLLING);
    public static final Connector CONNECTOR = new Connector("test.connector");
    public static final Map<String, String> PARTITION = Map.of("key", "value");

    private final DebeziumConnectorRegistry registry = Mockito.mock(DebeziumConnectorRegistry.class);
    private final Event event = Mockito.mock(Event.class);
    private final QuarkusHeartbeatEmitter underTest = new QuarkusHeartbeatEmitter(Collections.singletonList(registry), event);

    @BeforeEach
    void setUp() {
        when(event.select(any())).thenReturn(event);
    }

    @Test
    @DisplayName("should fire an event when called")
    void shouldFireEventWhenCalled() {
        when(registry.runningEngines()).thenReturn(Collections.singletonList(generate(DEBEZIUM_STATUS)));

        underTest.emit(PARTITION, OFFSET);

        verify(event).fire(new DebeziumHeartbeat(CONNECTOR, DEBEZIUM_STATUS, PARTITION, Map.of("offset", "value")));
    }

    @Test
    @DisplayName("should only fire event for the engine matching the current thread context")
    void shouldOnlyFireForEngineMatchingContext() {
        Connector otherConnector = new Connector("other.connector");
        DebeziumStatus otherStatus = new DebeziumStatus(DebeziumStatus.State.CREATING);

        Debezium matching = generate("testing", DEBEZIUM_STATUS, CONNECTOR);
        Debezium other = generate("alternative", otherStatus, otherConnector);

        when(registry.runningEngines()).thenReturn(List.of(matching, other));

        underTest.emit(PARTITION, OFFSET);

        verify(event).fire(new DebeziumHeartbeat(CONNECTOR, DEBEZIUM_STATUS, PARTITION, Map.of("offset", "value")));
        verify(event, never()).fire(new DebeziumHeartbeat(otherConnector, otherStatus, PARTITION, Map.of("offset", "value")));
    }

    public Debezium generate(DebeziumStatus status) {
        return generate("testing", status, CONNECTOR);
    }

    public Debezium generate(String manifestId, DebeziumStatus status, Connector connector) {
        return new Debezium() {
            @Override
            public DebeziumEngine.Signaler signaler() {
                return null;
            }

            @Override
            public Map<String, String> configuration() {
                return Map.of();
            }

            @Override
            public DebeziumStatus status() {
                return status;
            }

            @Override
            public Connector connector() {
                return connector;
            }

            @Override
            public EngineManifest manifest() {
                return new EngineManifest(manifestId);
            }
        };
    }

    public static final OffsetContext OFFSET = new OffsetContext() {
        @Override
        public Map<String, ?> getOffset() {
            return Map.of("offset", "value");
        }

        @Override
        public Schema getSourceInfoSchema() {
            return null;
        }

        @Override
        public Struct getSourceInfo() {
            return null;
        }

        @Override
        public boolean isInitialSnapshotRunning() {
            return false;
        }

        @Override
        public void markSnapshotRecord(SnapshotRecord record) {

        }

        @Override
        public void preSnapshotStart(boolean onDemand) {

        }

        @Override
        public void preSnapshotCompletion() {

        }

        @Override
        public void postSnapshotCompletion() {

        }

        @Override
        public void event(DataCollectionId collectionId, Instant timestamp) {

        }

        @Override
        public TransactionContext getTransactionContext() {
            return null;
        }
    };
}
