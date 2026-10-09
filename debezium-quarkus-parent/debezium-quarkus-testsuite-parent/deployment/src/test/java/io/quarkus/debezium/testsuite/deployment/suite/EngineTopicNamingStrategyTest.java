/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.testsuite.deployment.suite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.given;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;
import io.debezium.runtime.events.Engine;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.debezium.testsuite.deployment.TestSuiteConfigurations;
import io.quarkus.test.QuarkusUnitTest;

@Tag(SuiteTags.DEFAULT)
public class EngineTopicNamingStrategyTest {

    private static final String ENGINE_PREFIX = "engine.";

    @Inject
    CaptureHandler captureHandler;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(
                    CaptureHandler.class,
                    PrefixTopicNamingStrategy.class,
                    DefaultEngineTopicNamingStrategy.class,
                    OtherEngineTopicNamingStrategy.class,
                    SharedTopicNamingStrategy.class))
            .withConfigurationResource("debezium-quarkus-testsuite.properties");

    @Test
    @DisplayName("should use the topic naming strategy bean qualified for the engine")
    void shouldUseTopicNamingStrategyBeanQualifiedForEngine() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(captureHandler.destinations())
                        .isNotEmpty()
                        .allMatch(destination -> destination.startsWith(ENGINE_PREFIX)));
    }

    @ApplicationScoped
    static class CaptureHandler {
        private final Set<String> destinations = ConcurrentHashMap.newKeySet();

        @Capturing()
        public void capture(CapturingEvent<SourceRecord, SourceRecord> event) {
            destinations.add(event.destination());
        }

        public Set<String> destinations() {
            return destinations;
        }
    }

    @Dependent
    @Engine("default")
    static class DefaultEngineTopicNamingStrategy extends PrefixTopicNamingStrategy {
        DefaultEngineTopicNamingStrategy() {
            super(ENGINE_PREFIX);
        }
    }

    @Dependent
    @Engine("other")
    static class OtherEngineTopicNamingStrategy extends PrefixTopicNamingStrategy {
        OtherEngineTopicNamingStrategy() {
            super("other.");
        }
    }

    @Dependent
    static class SharedTopicNamingStrategy extends PrefixTopicNamingStrategy {
        SharedTopicNamingStrategy() {
            super("shared.");
        }
    }

    abstract static class PrefixTopicNamingStrategy implements TopicNamingStrategy<DataCollectionId> {
        private final String prefix;

        PrefixTopicNamingStrategy(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public void configure(Properties props) {
            // nothing to configure
        }

        @Override
        public String dataChangeTopic(DataCollectionId id) {
            return prefix + id.identifier();
        }

        @Override
        public String schemaChangeTopic() {
            return prefix + "schema";
        }

        @Override
        public String heartbeatTopic() {
            return prefix + "heartbeat";
        }

        @Override
        public String transactionTopic() {
            return prefix + "transaction";
        }

        @Override
        public String sanitizedTopicName(String topicName) {
            return topicName;
        }
    }
}
