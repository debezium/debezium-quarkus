/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import java.lang.reflect.Modifier;
import java.util.UUID;

import org.apache.kafka.connect.source.SourceRecord;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.PrimitiveType;
import org.jboss.jandex.Type;

import io.debezium.runtime.DebeziumShutdown.Phase;
import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.debezium.engine.shutdown.ShutdownInvoker;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;
import io.quarkus.runtime.util.HashUtil;

public class ShutdownInvokerGenerator {
    private final ClassOutput output;

    public ShutdownInvokerGenerator(ClassOutput output) {
        this.output = output;
    }

    /**
     * Checks the signature required by {@link io.debezium.runtime.DebeziumShutdown}:
     * a non-static, non-private method returning {@code boolean} with a single {@link SourceRecord} parameter.
     *
     * @param methodInfo the annotated method
     * @throws IllegalArgumentException when the signature is not supported
     */
    public static void validate(MethodInfo methodInfo) {
        boolean returnsBoolean = methodInfo.returnType().kind() == Type.Kind.PRIMITIVE
                && methodInfo.returnType().asPrimitiveType().primitive() == PrimitiveType.Primitive.BOOLEAN;
        boolean singleSourceRecord = methodInfo.parameterTypes().size() == 1
                && methodInfo.parameterType(0).name().toString().equals(SourceRecord.class.getName());
        boolean invokable = !Modifier.isStatic(methodInfo.flags()) && !Modifier.isPrivate(methodInfo.flags());

        if (!(returnsBoolean && singleSourceRecord && invokable)) {
            throw new IllegalArgumentException(
                    "@DebeziumShutdown on method " + methodInfo.declaringClass().name() + "." + methodInfo.name()
                            + " must be a non-static, non-private method that returns boolean and takes a single "
                            + SourceRecord.class.getName() + " parameter");
        }
    }

    /**
     * it generates a concrete class based on the {@link ShutdownInvoker} interface using gizmo:
     * <p>
     * public class GeneratedShutdownInvoker implements ShutdownInvoker {
     *     private final Object beanInstance;
     *
     *     Phase phase() {
     *         return Phase.AFTER;
     *     }
     *
     *     boolean shouldShutdown(SourceRecord record) {
     *         return beanInstance.method(record);
     *     }
     * }
     *
     * @param methodInfo the annotated method, already validated
     * @param beanInfo the bean that declares the method
     * @param phase the phase declared in the annotation
     * @return the metadata of the generated class
     */
    public GeneratedClassMetaData generate(MethodInfo methodInfo, BeanInfo beanInfo, Phase phase) {
        String name = generateClassName(beanInfo, methodInfo, phase);
        try (ClassCreator invoker = ClassCreator.builder()
                .classOutput(this.output)
                .className(name)
                .interfaces(ShutdownInvoker.class)
                .build()) {

            FieldDescriptor beanInstanceField = invoker.getFieldCreator("beanInstance", methodInfo
                    .declaringClass()
                    .name()
                    .toString())
                    .setModifiers(Modifier.PRIVATE)
                    .getFieldDescriptor();

            try (MethodCreator constructor = invoker.getMethodCreator("<init>", void.class, Object.class)) {
                constructor.setModifiers(Modifier.PUBLIC);
                constructor.invokeSpecialMethod(MethodDescriptor.ofConstructor(Object.class), constructor.getThis());
                ResultHandle constructorThis = constructor.getThis();
                ResultHandle beanInstance = constructor.getMethodParam(0);
                constructor.writeInstanceField(beanInstanceField, constructorThis, beanInstance);
                constructor.returnValue(null);
            }

            try (MethodCreator phaseMethod = invoker.getMethodCreator("phase", Phase.class)) {
                phaseMethod.setModifiers(Modifier.PUBLIC);
                phaseMethod.returnValue(phaseMethod.readStaticField(FieldDescriptor.of(Phase.class, phase.name(), Phase.class)));
            }

            try (MethodCreator shouldShutdown = invoker.getMethodCreator("shouldShutdown", boolean.class, SourceRecord.class)) {
                shouldShutdown.setModifiers(Modifier.PUBLIC);
                ResultHandle delegate = shouldShutdown.readInstanceField(beanInstanceField, shouldShutdown.getThis());
                ResultHandle record = shouldShutdown.getMethodParam(0);

                MethodDescriptor methodDescriptor = MethodDescriptor.ofMethod(
                        methodInfo.declaringClass().name().toString(),
                        methodInfo.name(),
                        "boolean",
                        SourceRecord.class.getName());

                shouldShutdown.returnValue(shouldShutdown.invokeVirtualMethod(methodDescriptor, delegate, record));
            }
        }

        return new GeneratedClassMetaData(UUID.randomUUID(), name.replace('/', '.'), beanInfo, ShutdownInvoker.class);
    }

    private String generateClassName(BeanInfo bean, MethodInfo methodInfo, Phase phase) {
        return DotNames.internalPackageNameWithTrailingSlash(bean.getImplClazz().name())
                + DotNames.simpleName(bean.getImplClazz().name())
                + "_DebeziumShutdown" + "_"
                + phase.name() + "_"
                + methodInfo.name() + "_"
                + HashUtil.sha1(phase.name() + "_" + methodInfo.name() + "_" + methodInfo.returnType().name().toString());
    }
}
