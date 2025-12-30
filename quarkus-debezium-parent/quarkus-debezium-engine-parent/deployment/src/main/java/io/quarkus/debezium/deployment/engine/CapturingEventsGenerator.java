/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import java.util.Optional;
import java.util.UUID;

import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvents;
import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.debezium.deployment.dotnames.DebeziumDotNames;
import io.quarkus.debezium.engine.capture.CapturingEventsInvoker;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.runtime.util.HashUtil;

public class CapturingEventsGenerator implements GizmoBasedCapturingInvokerGenerator {
    private final ClassOutput output;

    public CapturingEventsGenerator(ClassOutput output) {
        this.output = output;
    }

    @Override
    public boolean isCompatible(Type type) {
        return ParameterizedType.create(CapturingEvents.class, Type.create(BatchEvent.class)).equals(type);
    }

    /**
     * it generates concrete classes based on the {@link io.quarkus.debezium.engine.capture.CapturingEventInvoker} interface using gizmo:
     * <p>
     * public class GeneratedCapturingInvoker {
     * private final Object beanInstance;
     * <p>
     * void capture(CapturingEvents<BatchEvent> event) {
     * beanInstance.method(event);
     * }
     * <p>
     * }
     *
     * @param methodInfo
     * @param beanInfo
     * @return
     */
    @Override
    public GeneratedClassMetaData generate(MethodInfo methodInfo, BeanInfo beanInfo) {
        String name = generateClassName(beanInfo, methodInfo);

        try (ClassCreator invoker = ClassCreator.builder()
                .classOutput(this.output)
                .className(name)
                .interfaces(CapturingEventsInvoker.class)
                .build()) {

            FieldDescriptor beanInstanceField = constructorWithObjectField(methodInfo, invoker, "beanInstance");
            createCaptureMethod(methodInfo, invoker, beanInstanceField, CapturingEvents.class);

            try (MethodCreator destination = invoker.getMethodCreator("destination", String.class)) {
                Optional.ofNullable(methodInfo
                        .annotation(DebeziumDotNames.CAPTURING)
                        .value("destination"))
                        .map(AnnotationValue::asString)
                        .ifPresentOrElse(s -> {
                            if (s.isEmpty()) {
                                throw new IllegalArgumentException("empty destination are not allowed for @Capturing annotation  " + methodInfo.declaringClass());
                            }
                            destination.returnValue(destination.load(s));
                        },
                                () -> destination.returnValue(destination.load(Capturing.ALL)));
            }

            createEngineMethod(methodInfo, invoker);

            return new GeneratedClassMetaData(UUID.randomUUID(), name.replace('/', '.'), beanInfo, CapturingEventsInvoker.class);
        }
    }

    private String generateClassName(BeanInfo bean, MethodInfo methodInfo) {
        return DotNames.internalPackageNameWithTrailingSlash(bean.getImplClazz().name())
                + DotNames.simpleName(bean.getImplClazz().name())
                + "_DebeziumBatchInvoker" + "_"
                + methodInfo.name() + "_"
                + HashUtil.sha1(methodInfo.name() + "_" + methodInfo.returnType().name().toString());
    }
}
