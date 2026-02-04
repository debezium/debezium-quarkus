/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache.deployment;

import static io.debezium.quarkus.hibernate.cache.PersistenceUnit.CacheMode;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.Cacheable;
import jakarta.persistence.Table;

import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;

import io.debezium.quarkus.hibernate.cache.JpaInformation;
import io.debezium.quarkus.hibernate.cache.PersistenceUnit;
import io.debezium.quarkus.hibernate.cache.PersistenceUnitRegistry;
import io.debezium.quarkus.hibernate.cache.PersistentUnitRegistryRecorder;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.hibernate.orm.deployment.JpaModelPersistenceUnitMappingBuildItem;
import io.quarkus.hibernate.orm.deployment.PersistenceUnitDescriptorBuildItem;
import io.quarkus.hibernate.orm.runtime.boot.QuarkusPersistenceUnitDefinition;

class DebeziumQuarkusHibernateCacheProcessor {

    private static final String FEATURE = "debezium-hibernate-cache";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    /**
     * TODO: properties that should I know
     * - name strategy (Snake, camel)
     * - schema
     * @return
     */
    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void discoverEntities(
                          BuildProducer<SyntheticBeanBuildItem> syntheticBeanBuildItemBuildProducer,
                          PersistentUnitRegistryRecorder recorder,
                          List<PersistenceUnitDescriptorBuildItem> persistenceUnitDescriptorBuildItems,
                          JpaModelPersistenceUnitMappingBuildItem jpaModelBuildItem,
                          CombinedIndexBuildItem indexBuildItem) {

        var entities = jpaModelBuildItem
                .getEntityToPersistenceUnits()
                .entrySet()
                .stream()
                .map(unit -> Map.entry(indexBuildItem.getIndex().getClassByName(unit.getKey()), unit.getValue()))
                .flatMap(entry -> entry
                        .getValue()
                        .stream()
                        .map(persistentUnit -> new JpaInformation(
                                entry.getKey().simpleName(),
                                entry.getKey().annotation(DotName.createSimple(Table.class)) != null &&
                                        entry.getKey().annotation(DotName.createSimple(Table.class)).value("name") != null
                                                ? entry.getKey().annotation(DotName.createSimple(Table.class)).value("name").asString()
                                                : entry.getKey().simpleName(),
                                isCached(entry.getKey()),
                                persistentUnit)))
                .collect(Collectors.groupingBy(JpaInformation::persistentUnit));

        List<PersistenceUnit> persistenceUnits = persistenceUnitDescriptorBuildItems
                .stream()
                .map(unit -> unit.asOutputPersistenceUnitDefinition(Collections.emptyList()))
                .map(QuarkusPersistenceUnitDefinition::getPersistenceUnitDescriptor)
                .map(unit -> new PersistenceUnit(
                        unit.getName(),
                        entities.get(unit.getName()),
                        CacheMode.valueOf(unit.getProperties().get("jakarta.persistence.sharedCache.mode").toString())))
                .toList();

        syntheticBeanBuildItemBuildProducer
                .produce(
                        SyntheticBeanBuildItem.configure(PersistenceUnitRegistry.class)
                                .setRuntimeInit()
                                .scope(ApplicationScoped.class)
                                .unremovable()
                                .supplier(recorder.registry(persistenceUnits))
                                .named("PersistenceUnitRegistry")
                                .done());
    }

    private boolean isCached(ClassInfo classInfo) {
        if (classInfo.hasDeclaredAnnotation(DotName.createSimple(Cacheable.class))
                && classInfo.annotation(DotName.createSimple(Cacheable.class)).value() != null) {
            return classInfo.annotation(DotName.createSimple(Cacheable.class)).value().asBoolean();
        }

        return classInfo.hasDeclaredAnnotation(DotName.createSimple(Cacheable.class))
                && classInfo.annotation(DotName.createSimple(Cacheable.class)).value() == null;
    }

}
