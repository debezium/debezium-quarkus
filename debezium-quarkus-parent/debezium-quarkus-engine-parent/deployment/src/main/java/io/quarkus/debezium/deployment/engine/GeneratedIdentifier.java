/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import java.util.UUID;

final class GeneratedIdentifier {

    private GeneratedIdentifier() {
    }

    /**
     * Builds an identifier usable in synthetic bean names from the whole UUID (32 hex characters),
     * so that two generated classes in the same build don't collide.
     */
    static String of(UUID id) {
        return id.toString().replace("-", "");
    }
}
