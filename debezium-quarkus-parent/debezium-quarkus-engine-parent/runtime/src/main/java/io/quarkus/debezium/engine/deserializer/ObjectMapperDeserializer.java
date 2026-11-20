/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.deserializer;

import tools.jackson.databind.JavaType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Jackson deserializer for a change data capture event
 * @param <T>
 */
public class ObjectMapperDeserializer<T> implements Deserializer<T> {

    public static final String PAYLOAD = "payload";
    private final JavaType type;
    private final ObjectMapper objectMapper;

    public ObjectMapperDeserializer(Class<T> type) {
        this(type, ObjectMapperProducer.get());
    }

    public ObjectMapperDeserializer(Class<T> type, ObjectMapper objectMapper) {
        this.type = objectMapper.constructType(type);
        this.objectMapper = objectMapper;
    }

    @Override
    public T deserialize(byte[] data, String path) {
        if (data == null) {
            return null;
        }

        if (path == null) {
            return deserializeFlattenEvent(data);
        }

        return deserializeDebeziumEvent(data, path);
    }

    private T deserializeFlattenEvent(byte[] data) {
        JsonNode root = objectMapper.readTree(data);

        return objectMapper.treeToValue(root.path(PAYLOAD), type);
    }

    private T deserializeDebeziumEvent(byte[] data, String path) {
        JsonNode root = objectMapper.readTree(data);
        JsonNode afterNode = root.path(PAYLOAD).path(path);

        if (containsJson(afterNode)) {
            return objectMapper.treeToValue(objectMapper.readTree(afterNode.stringValue()), type);
        }

        return objectMapper.treeToValue(afterNode, type);
    }

    /**
     *
     * @param afterNode Json tree model to check
     * @return if the json node is a string that contains JSON
     */
    private boolean containsJson(JsonNode afterNode) {
        return afterNode.isString();
    }

    @Override
    public void close() {

    }
}
