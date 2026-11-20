/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app.deserializer;

import io.quarkus.debezium.engine.deserializer.ObjectMapperDeserializer;
import io.quarkus.sample.app.dto.Product;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

public class ProductDeserializer extends ObjectMapperDeserializer<Product> {
    private static final ObjectMapper configuredMapper = JsonMapper.builder()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
            .build();

    public ProductDeserializer() {
        super(Product.class, configuredMapper);
    }
}
