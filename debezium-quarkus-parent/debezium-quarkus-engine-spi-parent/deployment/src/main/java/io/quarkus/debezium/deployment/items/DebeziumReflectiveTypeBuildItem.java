/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.deployment.items;

import org.jboss.jandex.DotName;

import io.quarkus.builder.item.MultiBuildItem;

/**
 * Declares a type whose implementations Debezium instantiates reflectively at runtime, either because
 * the implementation class is named in the configuration or because it is loaded through a ServiceLoader.
 * <p>
 * During a native build, the type and every concrete implementation or subclass of it, found either in
 * the Debezium artifacts or in the application, is registered for reflection.
 */
public final class DebeziumReflectiveTypeBuildItem extends MultiBuildItem {

    private final DotName type;

    public DebeziumReflectiveTypeBuildItem(Class<?> type) {
        this.type = DotName.createSimple(type.getName());
    }

    public DotName type() {
        return type;
    }
}
