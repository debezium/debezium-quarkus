/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.relational.converter;

import java.util.List;
import java.util.Properties;

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.relational.CustomConverterRegistry;
import io.debezium.spi.converter.ConvertedField;
import io.debezium.spi.converter.CustomConverter;

/**
 * The {@link CustomConverter} that Debezium resolves from Quarkus Arc, it applies every
 * {@link QuarkusCustomConverter} bean that accepts the converted field.
 *
 * @author Giovanni Panice
 */
@Dependent
public class QuarkusCustomConverters implements CustomConverter<SchemaBuilder, ConvertedField> {

    private final List<QuarkusCustomConverter> converters;

    @Inject
    public QuarkusCustomConverters(Instance<QuarkusCustomConverter> converters) {
        this(converters.stream().toList());
    }

    QuarkusCustomConverters(List<QuarkusCustomConverter> converters) {
        this.converters = converters;
    }

    @Override
    public void configure(Properties props) {
        // the converters are configured by Quarkus
    }

    @Override
    public void converterFor(ConvertedField field, ConverterRegistration<SchemaBuilder> registration) {
        converters.forEach(converter -> {
            if (!converter.filter(field)) {
                return;
            }
            CustomConverterRegistry.ConverterDefinition<SchemaBuilder> definition = converter.bind(field);

            registration.register(definition.fieldSchema, definition.converter);
        });
    }
}
