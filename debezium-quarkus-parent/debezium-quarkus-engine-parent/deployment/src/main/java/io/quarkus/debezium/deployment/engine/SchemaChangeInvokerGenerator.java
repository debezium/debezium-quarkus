/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import java.lang.reflect.Modifier;
import java.util.Optional;
import java.util.UUID;

import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.SchemaChange;
import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.debezium.deployment.dotnames.DebeziumDotNames;
import io.quarkus.debezium.engine.schemachange.SchemaChangeInvoker;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;
import io.quarkus.runtime.util.HashUtil;

public class SchemaChangeInvokerGenerator {
    private final ClassOutput output;

    public SchemaChangeInvokerGenerator(ClassOutput output) {
        this.output = output;
    }

    /**
     * Checks the signature required by {@link io.debezium.runtime.CapturingSchemaChange}:
     * a non-static, non-private method returning {@code void} with a single {@link SchemaChange} parameter.
     *
     * @param methodInfo the annotated method
     * @throws IllegalArgumentException when the signature is not supported
     */
    public static void validate(MethodInfo methodInfo) {
        boolean returnsVoid = methodInfo.returnType().kind() == Type.Kind.VOID;
        boolean singleSchemaChange = methodInfo.parameterTypes().size() == 1
                && methodInfo.parameterType(0).name().toString().equals(SchemaChange.class.getName());
        boolean invokable = !Modifier.isStatic(methodInfo.flags()) && !Modifier.isPrivate(methodInfo.flags());

        if (!(returnsVoid && singleSchemaChange && invokable)) {
            throw new IllegalArgumentException(
                    "@CapturingSchemaChange on method " + methodInfo.declaringClass().name() + "." + methodInfo.name()
                            + " must be a non-static, non-private method that returns void and takes a single "
                            + SchemaChange.class.getName() + " parameter");
        }
    }

    /**
     * it generates a concrete class based on the {@link SchemaChangeInvoker} interface using gizmo:
     * <p>
     * public class GeneratedSchemaChangeInvoker implements SchemaChangeInvoker {
     *     private final Object beanInstance;
     *
     *     String engine() {
     *         return "default";
     *     }
     *
     *     void capture(SchemaChange change) {
     *         beanInstance.method(change);
     *     }
     * }
     *
     * @param methodInfo the annotated method, already validated
     * @param beanInfo the bean that declares the method
     * @return the metadata of the generated class
     */
    public GeneratedClassMetaData generate(MethodInfo methodInfo, BeanInfo beanInfo) {
        String engine = Optional.ofNullable(methodInfo.annotation(DebeziumDotNames.CAPTURING_SCHEMA_CHANGE).value("engine"))
                .map(AnnotationValue::asString)
                .orElse(Capturing.DEFAULT);
        String name = generateClassName(beanInfo, methodInfo);

        try (ClassCreator invoker = ClassCreator.builder()
                .classOutput(this.output)
                .className(name)
                .interfaces(SchemaChangeInvoker.class)
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

            try (MethodCreator engineMethod = invoker.getMethodCreator("engine", String.class)) {
                engineMethod.setModifiers(Modifier.PUBLIC);
                engineMethod.returnValue(engineMethod.load(engine));
            }

            try (MethodCreator capture = invoker.getMethodCreator("capture", void.class, SchemaChange.class)) {
                capture.setModifiers(Modifier.PUBLIC);
                ResultHandle delegate = capture.readInstanceField(beanInstanceField, capture.getThis());
                ResultHandle change = capture.getMethodParam(0);

                MethodDescriptor methodDescriptor = MethodDescriptor.ofMethod(
                        methodInfo.declaringClass().name().toString(),
                        methodInfo.name(),
                        void.class,
                        SchemaChange.class);

                capture.invokeVirtualMethod(methodDescriptor, delegate, change);
                capture.returnValue(null);
            }
        }

        return new GeneratedClassMetaData(UUID.randomUUID(), name.replace('/', '.'), beanInfo, SchemaChangeInvoker.class);
    }

    private String generateClassName(BeanInfo bean, MethodInfo methodInfo) {
        return DotNames.internalPackageNameWithTrailingSlash(bean.getImplClazz().name())
                + DotNames.simpleName(bean.getImplClazz().name())
                + "_CapturingSchemaChange" + "_"
                + methodInfo.name() + "_"
                + HashUtil.sha1(methodInfo.name() + "_" + methodInfo.returnType().name().toString());
    }
}
