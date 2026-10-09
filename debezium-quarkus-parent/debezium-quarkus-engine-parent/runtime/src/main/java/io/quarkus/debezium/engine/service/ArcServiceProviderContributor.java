/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.service;

import io.debezium.config.Configuration;
import io.debezium.service.spi.InstanceResolver;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceProviderContributor;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.service.spi.ServiceRegistryBuilder;

/**
 * Contributes the services based on Quarkus Arc to the Debezium service registry.
 */
public class ArcServiceProviderContributor implements ServiceProviderContributor {

    @Override
    public void contribute(ServiceRegistryBuilder registryBuilder) {
        registryBuilder.registerServiceProvider(new ArcInstanceResolverProvider());
    }

    private static class ArcInstanceResolverProvider implements ServiceProvider<InstanceResolver> {

        @Override
        public Class<InstanceResolver> getServiceClass() {
            return InstanceResolver.class;
        }

        @Override
        public InstanceResolver createService(Configuration configuration, ServiceRegistry serviceRegistry) {
            return new ArcInstanceResolver(configuration);
        }
    }
}
