/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.shutdown;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

import io.quarkus.arc.Arc;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class DynamicShutdownInvokerSupplier {

    public static final String BASE_NAME = "shutdown";

    public Supplier<ShutdownInvoker> createInvoker(Class<?> mediatorClazz, Class<? extends ShutdownInvoker> invokerClazz) {
        try {
            Object mediator = Arc.container().instance(mediatorClazz).get();
            ShutdownInvoker instance = invokerClazz
                    .getDeclaredConstructor(Object.class)
                    .newInstance(mediator);

            return () -> instance;
        }
        catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
