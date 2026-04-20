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

import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;
import io.quarkus.debezium.engine.IllegalDebeziumStateException;
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
        return Response.ok(registry.manifests()
                .stream()
                .map(manifest -> new EngineInformation(manifest.id(), registry.connector().name()))
                .toList()).build();
    }

    @GET
    @Path("status")
    public DebeziumStatus getState() {
        return registry.engines().stream()
                .findFirst()
                .map(Debezium::status)
                .orElse(new DebeziumStatus(DebeziumStatus.State.STOPPED));
    }

    @GET
    @Path("statuses")
    public List<DebeziumStatus> getStatuses() {
        return registry.manifests().stream()
                .map(m -> {
                    Debezium engine = registry.get(m);
                    return engine != null ? engine.status() : new DebeziumStatus(DebeziumStatus.State.STOPPED);
                })
                .toList();
    }

    @POST
    @Path("start")
    public Response start() {
        registry.manifests().forEach(m -> registry.start(m));
        return Response.ok().build();
    }

    @POST
    @Path("start/{id}")
    public Response start(@PathParam("id") String id) {
        registry.manifests().stream()
                .filter(m -> m.id().equals(id))
                .findFirst()
                .ifPresent(m -> registry.start(m));
        return Response.ok().build();
    }

    @POST
    @Path("stop")
    public Response stop() {
        try {
            List<Debezium> running = registry.engines();
            if (running.isEmpty()) {
                throw new IllegalDebeziumStateException("No running engines found");
            }
            running.forEach(e -> registry.stop(e.manifest()));
            return Response.ok().build();
        }
        catch (IllegalDebeziumStateException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e.getMessage())
                    .build();
        }
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
