/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.quarkus.debezium.engine.service.ArcInstanceResolver.Candidate;

class ArcInstanceResolverTest {

    private static final String CONFIG_KEY = "a.config.key";
    private static final String ENGINE = "orders";

    private final Configuration configuration = Configuration.create().with(CONFIG_KEY, "a.configured.ClassName").build();

    @Test
    @DisplayName("should use the fallback when there is no bean for the contract")
    void shouldUseFallbackWhenThereIsNoBean() {
        List<String> initialized = new ArrayList<>();
        ArcInstanceResolver underTest = resolverWith();

        assertThat(underTest.resolve(CharSequence.class, CONFIG_KEY, () -> "fallback", instance -> initialized.add(instance.toString())))
                .isEqualTo("fallback");
        assertThat(initialized).isEmpty();
    }

    @Test
    @DisplayName("should use and initialize the shared bean even when a class is configured")
    void shouldUseAndInitializeSharedBean() {
        List<String> initialized = new ArrayList<>();
        ArcInstanceResolver underTest = resolverWith(shared("shared"));

        assertThat(underTest.resolve(CharSequence.class, CONFIG_KEY, () -> "fallback", instance -> initialized.add(instance.toString())))
                .isEqualTo("shared");
        assertThat(initialized).containsExactly("shared");
    }

    @Test
    @DisplayName("should use the bean when there is no initializer")
    void shouldUseBeanWithoutInitializer() {
        ArcInstanceResolver underTest = resolverWith(shared("shared"));

        assertThat(underTest.resolve(CharSequence.class, CONFIG_KEY, () -> "fallback")).isEqualTo("shared");
    }

    @Test
    @DisplayName("should prefer the bean qualified for the engine over the shared bean")
    void shouldPreferBeanQualifiedForEngine() {
        ArcInstanceResolver underTest = resolverWith(shared("shared"), qualified(ENGINE, "orders"), qualified("payments", "payments"));

        assertThat(underTest.resolve(CharSequence.class, CONFIG_KEY, () -> "fallback")).isEqualTo("orders");
    }

    @Test
    @DisplayName("should use the shared bean when no bean is qualified for the engine")
    void shouldUseSharedBeanWhenNoBeanIsQualifiedForEngine() {
        ArcInstanceResolver underTest = resolverWith(shared("shared"), qualified("payments", "payments"));

        assertThat(underTest.resolve(CharSequence.class, CONFIG_KEY, () -> "fallback")).isEqualTo("shared");
    }

    @Test
    @DisplayName("should use the fallback when the only bean is qualified for another engine")
    void shouldUseFallbackWhenBeanIsQualifiedForAnotherEngine() {
        ArcInstanceResolver underTest = resolverWith(qualified("payments", "payments"));

        assertThat(underTest.resolve(CharSequence.class, CONFIG_KEY, () -> "fallback")).isEqualTo("fallback");
    }

    @Test
    @DisplayName("should only instantiate the selected bean")
    void shouldOnlyInstantiateSelectedBean() {
        List<String> instantiated = new ArrayList<>();
        ArcInstanceResolver underTest = resolverWith(
                new Candidate(null, () -> instantiate(instantiated, "shared")),
                new Candidate(ENGINE, () -> instantiate(instantiated, "orders")));

        underTest.resolve(CharSequence.class, CONFIG_KEY, () -> "fallback");

        assertThat(instantiated).containsExactly("orders");
    }

    @Test
    @DisplayName("should fail when more than one bean is qualified for the engine")
    void shouldFailWhenMoreThanOneBeanIsQualifiedForEngine() {
        ArcInstanceResolver underTest = resolverWith(qualified(ENGINE, "one"), qualified(ENGINE, "two"));

        assertThatThrownBy(() -> underTest.resolve(CharSequence.class, CONFIG_KEY, () -> "fallback"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining(CharSequence.class.getName())
                .hasMessageContaining(ENGINE);
    }

    @Test
    @DisplayName("should fail when there is more than one shared bean")
    void shouldFailWhenThereIsMoreThanOneSharedBean() {
        ArcInstanceResolver underTest = resolverWith(shared("one"), shared("two"));

        assertThatThrownBy(() -> underTest.resolve(CharSequence.class, CONFIG_KEY, () -> "fallback"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining(CharSequence.class.getName());
    }

    @Test
    @DisplayName("should resolve all only from the fallback when there is no bean for the contract")
    void shouldResolveAllFromFallbackWhenThereIsNoBean() {
        ArcInstanceResolver underTest = resolverWith();

        assertThat(underTest.resolveAll(CharSequence.class, () -> List.of("configured"))).containsExactly("configured");
    }

    @Test
    @DisplayName("should add the shared beans and the beans qualified for the engine to the fallback")
    void shouldResolveAllAddingBeansToFallback() {
        ArcInstanceResolver underTest = resolverWith(shared("shared"), qualified(ENGINE, "orders"), qualified("payments", "payments"));

        assertThat(underTest.resolveAll(CharSequence.class, () -> List.of("configured"))).containsExactly("configured", "shared", "orders");
    }

    @Test
    @DisplayName("should initialize only the beans when resolving all")
    void shouldInitializeOnlyBeansWhenResolvingAll() {
        List<String> initialized = new ArrayList<>();
        ArcInstanceResolver underTest = resolverWith(shared("shared"), qualified(ENGINE, "orders"));

        underTest.resolveAll(CharSequence.class, () -> List.of("configured"), instance -> initialized.add(instance.toString()));

        assertThat(initialized).containsExactly("shared", "orders");
    }

    private ArcInstanceResolver resolverWith(Candidate... candidates) {
        return new ArcInstanceResolver(configuration, () -> ENGINE, contract -> List.of(candidates));
    }

    private static Candidate shared(String instance) {
        return new Candidate(null, () -> instance);
    }

    private static Candidate qualified(String engine, String instance) {
        return new Candidate(engine, () -> instance);
    }

    private static String instantiate(List<String> instantiated, String instance) {
        instantiated.add(instance);
        return instance;
    }
}
