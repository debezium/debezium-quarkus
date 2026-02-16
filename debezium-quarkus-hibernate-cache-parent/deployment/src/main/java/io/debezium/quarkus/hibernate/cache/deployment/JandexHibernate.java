/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.quarkus.hibernate.cache.deployment;

import io.debezium.quarkus.hibernate.cache.RawPersistenceUnit;
import jakarta.persistence.Cacheable;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.FieldInfo;
import org.jboss.jandex.Type;

import java.util.Optional;
import java.util.function.Function;

public class JandexHibernate {

    private final Function<ClassInfo, String> namingStrategyStandard;

    public JandexHibernate(Function<ClassInfo, String> namingStrategyStandard) {
        this.namingStrategyStandard = namingStrategyStandard;
    }

    public RawPersistenceUnit.RawJpaInfo transform(ClassInfo classInfo, String persistentUnit) {
        return new RawPersistenceUnit.RawJpaInfo(
                classInfo.simpleName(),
                getExplicitTableOrImplicit(classInfo, namingStrategyStandard),
                getHibernateId(classInfo),
                getHibernateIdType(classInfo),
                isCached(classInfo),
                persistentUnit,
                classInfo.name().toString());
    }

    private Optional<String> getHibernateIdType(ClassInfo classInfo) {
        return Optional.ofNullable(classInfo.annotation(DotName.createSimple(Id.class)))
                .map(AnnotationInstance::target)
                .map(AnnotationTarget::asField)
                .map(FieldInfo::type)
                .map(Type::toString);
    }

    private Optional<String> getHibernateId(ClassInfo classInfo) {
        return Optional.ofNullable(classInfo.annotation(DotName.createSimple(Id.class)))
                .map(AnnotationInstance::target)
                .map(AnnotationTarget::asField)
                .map(FieldInfo::name);
    }

    private String getExplicitTableOrImplicit(ClassInfo classInfo, Function<ClassInfo, String> implicit) {
        return classInfo.annotation(DotName.createSimple(Table.class)) != null &&
                classInfo.annotation(DotName.createSimple(Table.class)).value("name") != null
                ? classInfo.annotation(DotName.createSimple(Table.class)).value("name").asString()
                : implicit.apply(classInfo);
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
