/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.engine;

public class IllegalDebeziumStateException extends RuntimeException {

    public IllegalDebeziumStateException(String message) {
        super(message);
    }

    public IllegalDebeziumStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
