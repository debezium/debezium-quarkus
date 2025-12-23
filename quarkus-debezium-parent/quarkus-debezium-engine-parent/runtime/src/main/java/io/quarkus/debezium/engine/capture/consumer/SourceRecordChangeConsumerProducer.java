/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import io.debezium.DebeziumException;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.quarkus.debezium.engine.capture.CapturingEventsInvokerRegistry;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SourceRecordChangeConsumerProducer {

    private final CapturingEventsInvokerRegistry<CapturingEvents> registry;

    public SourceRecordChangeConsumerProducer(CapturingEventsInvokerRegistry<CapturingEvents> registry) {
        this.registry = registry;
    }

    @Produces
    @Singleton
    public ChangeConsumerHandler produce() {
      return manifest -> new GeneralChangeConsumer() {
          private final Logger logger = LoggerFactory.getLogger(GeneralChangeConsumer.class);

          @Override
          public void handleBatch(List<ChangeEvent<Object, Object>> records,
                                  RecordCommitter<ChangeEvent<Object, Object>> committer) {
              logger.trace("receiving events for engine id {}", manifest.id());

              Map<String, List<BatchEvent>> collect = records.stream()
                      .map(record -> Map.entry(record.destination(), new BatchEvent() {
                          @Override
                          public Object key() {
                              return record.key();
                          }

                          @Override
                          public Object value() {
                              return record.value();
                          }

                          @Override
                          public int partition() {
                              return record.partition();
                          }

                          @Override
                          public SourceRecord record() {
                              return ((EmbeddedEngineChangeEvent<Object, Object, Object>) record).sourceRecord();
                          }

                          @Override
                          public void commit() {
                              try {
                                  committer.markProcessed(record);
                              }
                              catch (InterruptedException e) {
                                  throw new DebeziumException(e);
                              }
                          }
                      }))
                      .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

              List<CapturingEvents<BatchEvent>> capturingEvents = collect
                      .entrySet()
                      .stream()
                      .map(entry -> new CapturingEvents<BatchEvent>() {
                          @Override
                          public List<BatchEvent> records() {
                              return entry.getValue();
                          }

                          @Override
                          public String destination() {
                              return entry.getKey();
                          }

                          @Override
                          public String source() {
                              return "NOT_AVAILABLE";
                          }

                          @Override
                          public String engine() {
                              return manifest.id();
                          }
                      })
                      .collect(Collectors.toList());

              capturingEvents
                      .stream()
                      .map(events -> Map.entry(events, registry.get(events)))
                      .forEach(entry -> entry.getValue().capture(entry.getKey()));

              try {
                  committer.markBatchFinished();
              }
              catch (InterruptedException e) {
                  throw new DebeziumException(e);
              }
          }
      };
    }
}
