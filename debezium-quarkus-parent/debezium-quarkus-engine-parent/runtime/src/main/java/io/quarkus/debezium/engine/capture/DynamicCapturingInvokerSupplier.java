/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

import io.quarkus.arc.Arc;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class DynamicCapturingInvokerSupplier {

    public static final String BASE_NAME = "invoker";

    public Supplier<CapturingInvoker> createInvoker(Class<?> mediatorClazz, Class<?> filterClazz, Class<? extends CapturingInvoker> invokerClazz) {
        try {
            Object mediator = Arc.container().instance(mediatorClazz).get();

            if (filterClazz != null) {
                return createInvokerWithFilter(filterClazz, invokerClazz, mediator);
            }

            return createInvokerWithoutFilter(invokerClazz, mediator);

        }
        catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Supplier<CapturingInvoker> createInvokerWithoutFilter(Class<? extends CapturingInvoker> invokerClazz, Object mediator)
            throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        CapturingInvoker instance = invokerClazz
                .getDeclaredConstructor(Object.class)
                .newInstance(mediator);
        return () -> instance;
    }

    private static Supplier<CapturingInvoker> createInvokerWithFilter(Class<?> filterClazz, Class<? extends CapturingInvoker> invokerClazz, Object mediator)
            throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        Object filter = Arc.container().instance(filterClazz).get();
        CapturingInvoker instance = invokerClazz
                .getDeclaredConstructor(Object.class, Object.class)
                .newInstance(mediator, filter);
        return () -> instance;
    }
}
