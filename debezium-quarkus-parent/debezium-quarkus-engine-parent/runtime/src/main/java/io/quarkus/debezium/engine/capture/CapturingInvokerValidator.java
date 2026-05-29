/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import java.util.Collections;
import java.util.List;

import io.quarkus.debezium.engine.capture.CapturingInvoker.CapturingInvokerKey;

class CapturingInvokerValidator<T extends CapturingInvoker<?>> {
    void validate(List<T> invokers) {
        List<CapturingInvokerKey> keys = invokers
                .stream()
                .map(CapturingInvoker::generateKey)
                .toList();

        invokers.forEach(invoker -> {
            if (Collections.frequency(keys, CapturingInvoker.generateKey(invoker)) > 1) {
                throw new IllegalArgumentException(
                        "Two or more methods are annotated with @Capturing and have the same destination '"
                                + invoker.destination() + "' and engine '" + invoker.engine() + "'");
            }
        });
    }
}
