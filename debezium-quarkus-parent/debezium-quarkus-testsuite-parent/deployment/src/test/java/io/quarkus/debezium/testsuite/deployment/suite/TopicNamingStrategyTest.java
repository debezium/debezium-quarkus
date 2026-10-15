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
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.quarkus.debezium.testsuite.deployment.SuiteTags;
import io.quarkus.debezium.testsuite.deployment.TestSuiteConfigurations;
import io.quarkus.test.QuarkusUnitTest;

@Tag(SuiteTags.DEFAULT)
public class TopicNamingStrategyTest {

    private static final String BEAN_PREFIX = "bean.";

    @Inject
    CaptureHandler captureHandler;

    @RegisterExtension
    static final QuarkusUnitTest setup = new QuarkusUnitTest()
            .withApplicationRoot((jar) -> jar.addClasses(CaptureHandler.class, BeanTopicNamingStrategy.class))
            .withConfigurationResource("debezium-quarkus-testsuite.properties");

    @Test
    @DisplayName("should use the topic naming strategy defined as a bean")
    void shouldUseTopicNamingStrategyBean() {
        given().await()
                .atMost(TestSuiteConfigurations.TIMEOUT, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(captureHandler.destinations())
                        .isNotEmpty()
                        .allMatch(destination -> destination.startsWith(BEAN_PREFIX)));
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
    static class BeanTopicNamingStrategy implements TopicNamingStrategy<DataCollectionId> {
        private String prefix;

        @Override
        public void configure(Properties props) {
            // a bean is not configured through its constructor, the prefix is only set when Debezium configures it
            this.prefix = BEAN_PREFIX;
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
