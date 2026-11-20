/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import jakarta.persistence.AttributeConverter;

import org.hibernate.HibernateException;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Converter that knows how to convert between a {@link String} and {@link JsonNode}.
 *
 * @author Chris Cranford
 */
public class JsonNodeAttributeConverter implements AttributeConverter<JsonNode, String> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(JsonNode jsonNode) {
        if (jsonNode == null) {
            return null;
        }
        try {
            return mapper.writeValueAsString(jsonNode);
        }
        catch (JacksonException e) {
            throw new HibernateException("Failed to convert JsonNode to String", e);
        }
    }

    @Override
    public JsonNode convertToEntityAttribute(String databaseValue) {
        if (databaseValue == null) {
            return null;
        }
        try {
            return mapper.readValue(databaseValue, JsonNode.class);
        }
        catch (JacksonException e) {
            throw new HibernateException("Failed to convert String to JsonNode", e);
        }
    }
}
