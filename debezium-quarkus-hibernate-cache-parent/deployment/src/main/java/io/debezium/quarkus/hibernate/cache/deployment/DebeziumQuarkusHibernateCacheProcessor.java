/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache.deployment;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.Cacheable;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.FieldInfo;
import org.jboss.jandex.Type;

import io.debezium.quarkus.hibernate.cache.CacheMode;
import io.debezium.quarkus.hibernate.cache.DebeziumCacheInvalidatorProducer;
import io.debezium.quarkus.hibernate.cache.HibernateCacheHandler;
import io.debezium.quarkus.hibernate.cache.PersistenceUnitRegistry;
import io.debezium.quarkus.hibernate.cache.PersistentUnitRegistryRecorder;
import io.debezium.quarkus.hibernate.cache.RawPersistenceUnit;
import io.debezium.quarkus.hibernate.cache.RawPersistenceUnit.RawJpaInfo;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.hibernate.orm.deployment.JpaModelPersistenceUnitMappingBuildItem;
import io.quarkus.hibernate.orm.deployment.PersistenceUnitDescriptorBuildItem;
import io.quarkus.hibernate.orm.runtime.boot.QuarkusPersistenceUnitDefinition;

public class DebeziumQuarkusHibernateCacheProcessor {

    private static final String FEATURE = "debezium-hibernate-cache";
    private final PhysicalNamingStrategyStandard physicalNamingStrategyStandard = new PhysicalNamingStrategyStandard();

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void discoverEntities(BuildProducer<SyntheticBeanBuildItem> syntheticBeanBuildItemBuildProducer,
                          PersistentUnitRegistryRecorder recorder,
                          List<PersistenceUnitDescriptorBuildItem> persistenceUnitDescriptorBuildItems,
                          JpaModelPersistenceUnitMappingBuildItem jpaModelBuildItem,
                          CombinedIndexBuildItem indexBuildItem) {

        Map<String, List<RawJpaInfo>> entities = Optional.ofNullable(jpaModelBuildItem)
                .map(JpaModelPersistenceUnitMappingBuildItem::getEntityToPersistenceUnits)
                .orElse(Collections.emptyMap())
                .entrySet()
                .stream()
                .map(unit -> Map.entry(indexBuildItem.getIndex().getClassByName(unit.getKey()), unit.getValue()))
                .flatMap(entry -> entry
                        .getValue()
                        .stream()
                        .map(persistentUnit -> new RawJpaInfo(
                                entry.getKey().simpleName(),
                                entry.getKey().annotation(DotName.createSimple(Table.class)) != null &&
                                        entry.getKey().annotation(DotName.createSimple(Table.class)).value("name") != null
                                                ? entry.getKey().annotation(DotName.createSimple(Table.class)).value("name").asString()
                                                : physicalNamingStrategyStandard.apply(entry.getKey().simpleName()),
                                Optional.ofNullable(entry.getKey().annotation(DotName.createSimple(Id.class)))
                                        .map(AnnotationInstance::target)
                                        .map(AnnotationTarget::asField)
                                        .map(FieldInfo::name),
                                Optional.ofNullable(entry.getKey().annotation(DotName.createSimple(Id.class)))
                                        .map(AnnotationInstance::target)
                                        .map(AnnotationTarget::asField)
                                        .map(FieldInfo::type)
                                        .map(Type::toString),
                                isCached(entry.getKey()),
                                persistentUnit,
                                entry.getKey().name().toString())))
                .collect(Collectors.groupingBy(RawJpaInfo::persistentUnit));

        var persistenceUnits = persistenceUnitDescriptorBuildItems
                .stream()
                .map(unit -> unit.asOutputPersistenceUnitDefinition(Collections.emptyList()))
                .map(QuarkusPersistenceUnitDefinition::getPersistenceUnitDescriptor)
                .map(unit -> new RawPersistenceUnit(
                        unit.getName(),
                        entities.get(unit.getName()),
                        CacheMode.valueOf(unit.getProperties().get("jakarta.persistence.sharedCache.mode").toString())))
                .collect(Collectors.toMap(RawPersistenceUnit::name, Function.identity()));

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

    @BuildStep
    public void additionalItem(BuildProducer<AdditionalBeanBuildItem> producer) {
        producer.produce(
                AdditionalBeanBuildItem
                        .builder()
                        .addBeanClasses(DebeziumCacheInvalidatorProducer.class)
                        .setUnremovable()
                        .setDefaultScope(DotNames.SINGLETON)
                        .build());

        producer.produce(
                AdditionalBeanBuildItem
                        .builder()
                        .setRemovable()
                        .addBeanClass(HibernateCacheHandler.class)
                        .setDefaultScope(DotNames.SINGLETON)
                        .build());
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
