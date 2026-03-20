/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.resources;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;

import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;
import io.quarkus.sample.app.dto.EngineInformation;

@Path("engine")
@ApplicationScoped
public class EngineResource {

    private final DebeziumConnectorRegistry registry;

    public EngineResource(DebeziumConnectorRegistry registry) {
        this.registry = registry;
    }

    @GET
    @Path("manifest")
    public Response engines() {
        return Response.ok(registry.engines()
                .stream()
                .map(engine -> new EngineInformation(engine.manifest().id(), engine.connector().name()))
                .toList()).build();
    }

    @GET
    @Path("status")
    public DebeziumStatus getState() {
        return registry.engines().getFirst().status();
    }

    @GET
    @Path("statuses")
    public List<DebeziumStatus> getStatuses() {
        return registry.engines().stream()
                .map(e -> e.status())
                .toList();
    }

    @POST
    @Path("start")
    public Response start() {
        registry.engines().forEach(e -> registry.start(e.manifest()));
        return Response.ok().build();
    }

    @POST
    @Path("start/{id}")
    public Response start(@PathParam("id") String id) {
        registry.engines().stream()
                .filter(e -> e.manifest().id().equals(id))
                .findFirst()
                .ifPresent(e -> registry.start(e.manifest()));
        return Response.ok().build();
    }

    @POST
    @Path("stop")
    public Response stop() {
        registry.engines().forEach(e -> registry.stop(e.manifest()));
        return Response.ok().build();
    }

    @POST
    @Path("stop/{id}")
    public Response stop(@PathParam("id") String id) {
        registry.engines().stream()
                .filter(e -> e.manifest().id().equals(id))
                .findFirst()
                .ifPresent(e -> registry.stop(e.manifest()));
        return Response.ok().build();
    }
}
