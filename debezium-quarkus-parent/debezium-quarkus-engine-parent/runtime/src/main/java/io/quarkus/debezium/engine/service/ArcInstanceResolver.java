/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.service;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.runtime.EngineManifest;
import io.debezium.runtime.events.DefaultEngine;
import io.debezium.runtime.events.Engine;
import io.debezium.service.spi.InstanceResolver;
import io.quarkus.arc.Arc;
import io.quarkus.arc.ArcContainer;
import io.quarkus.debezium.engine.DebeziumThreadHandler;

/**
 * Quarkus based {@link InstanceResolver}, it supplies the instance from a bean managed by Quarkus Arc
 * when the application defines one for the requested contract, otherwise Debezium creates the instance
 * from the configuration.
 * <p>
 * The bean is selected for the engine that resolves the instance:
 * <ol>
 * <li>a bean qualified with {@link Engine} for the id of the engine</li>
 * <li>a bean without an engine qualifier, which is shared by every engine</li>
 * </ol>
 * A bean takes precedence over the class named in the configuration. A bean is configured each time it
 * is resolved, so a bean should be {@code @Dependent}.
 * <p>
 * When all the instances of a contract are resolved, the beans qualified for the engine and the shared
 * beans are added to the instances that Debezium creates.
 */
public class ArcInstanceResolver implements InstanceResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArcInstanceResolver.class);

    private final Configuration configuration;
    private final Supplier<String> engine;
    private final Function<Class<?>, List<Candidate>> beans;

    public ArcInstanceResolver(Configuration configuration) {
        this(configuration, () -> DebeziumThreadHandler.context().manifest().id(), ArcInstanceResolver::findBeans);
    }

    ArcInstanceResolver(Configuration configuration, Supplier<String> engine, Function<Class<?>, List<Candidate>> beans) {
        this.configuration = configuration;
        this.engine = engine;
        this.beans = beans;
    }

    @Override
    public <T> T resolve(Class<T> contract, String configKey, Supplier<? extends T> fallback, Consumer<? super T> initializer) {
        String engineId = engine.get();
        List<Candidate> candidates = beans.apply(contract);

        List<Candidate> selected = candidates.stream()
                .filter(candidate -> engineId.equals(candidate.engine()))
                .toList();

        if (selected.isEmpty()) {
            selected = candidates.stream()
                    .filter(candidate -> candidate.engine() == null)
                    .toList();
        }

        if (selected.isEmpty()) {
            return fallback.get();
        }

        if (selected.size() > 1) {
            throw new DebeziumException("Found " + selected.size() + " beans of type " + contract.getName()
                    + " for the engine '" + engineId + "' but only one is allowed");
        }

        T instance = contract.cast(selected.getFirst().instance().get());

        if (configuration.hasKey(configKey)) {
            LOGGER.info("Using the bean {} instead of the class configured by '{}' for the engine '{}'",
                    instance.getClass().getName(), configKey, engineId);
        }

        if (initializer != null) {
            initializer.accept(instance);
        }

        return instance;
    }

    @Override
    public <T> List<T> resolveAll(Class<T> contract, Supplier<? extends Collection<? extends T>> fallback, Consumer<? super T> initializer) {
        String engineId = engine.get();
        List<T> instances = new ArrayList<>(fallback.get());

        beans.apply(contract)
                .stream()
                .filter(candidate -> candidate.engine() == null || engineId.equals(candidate.engine()))
                .map(candidate -> contract.cast(candidate.instance().get()))
                .forEach(instance -> {
                    if (initializer != null) {
                        initializer.accept(instance);
                    }
                    instances.add(instance);
                });

        return instances;
    }

    /**
     * Finds the beans by the raw type of the contract, as a contract like {@code TopicNamingStrategy<TableId>}
     * is not assignable to the raw type under the CDI typesafe resolution rules. The reference is then
     * obtained with the bean type that matched, as the raw type is not one of the bean types.
     */
    private static List<Candidate> findBeans(Class<?> contract) {
        ArcContainer container = Arc.container();

        if (container == null) {
            return List.of();
        }

        BeanManager beanManager = container.beanManager();

        return beanManager.getBeans(Object.class, Any.Literal.INSTANCE)
                .stream()
                .flatMap(bean -> bean.getTypes()
                        .stream()
                        .filter(type -> rawType(type) == contract)
                        .findFirst()
                        .map(type -> new Candidate(engineOf(bean), () -> beanManager.getReference(bean, type, beanManager.createCreationalContext(bean))))
                        .stream())
                .toList();
    }

    private static String engineOf(Bean<?> bean) {
        return bean.getQualifiers()
                .stream()
                .map(qualifier -> {
                    if (qualifier instanceof Engine engine) {
                        return engine.value();
                    }
                    return qualifier instanceof DefaultEngine ? EngineManifest.DEFAULT.id() : null;
                })
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    private static Type rawType(Type type) {
        return type instanceof ParameterizedType parameterizedType ? parameterizedType.getRawType() : type;
    }

    /**
     * A bean for a contract.
     *
     * @param engine the id of the engine the bean is qualified for, {@code null} if it is shared by every engine
     * @param instance supplies the bean instance, so that only the selected bean is instantiated
     */
    record Candidate(String engine, Supplier<?> instance) {
    }
}
