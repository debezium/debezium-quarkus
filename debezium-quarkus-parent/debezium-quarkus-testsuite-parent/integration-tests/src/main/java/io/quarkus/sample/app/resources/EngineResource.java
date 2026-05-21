/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.resources;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumConnectorRegistry;
import io.debezium.runtime.DebeziumStatus;
import io.quarkus.sample.app.dto.EngineInformation;

@Path("engine")
@ApplicationScoped
public class EngineResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(EngineResource.class);

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
        Set<String> runningIds = registry.engines().stream()
                .map(e -> e.manifest().id())
                .collect(Collectors.toSet());
        return registry.manifests().stream()
                .map(m -> {
                    Debezium engine = runningIds.contains(m.id()) ? registry.get(m) : null;
                    return engine != null ? engine.status() : new DebeziumStatus(DebeziumStatus.State.STOPPED);
                })
                .toList();
    }

    @POST
    @Path("start")
    public Response start() {
        LOGGER.info("[engine/start] called; running engines before start: {}",
                registry.engines().stream().map(e -> e.manifest().id()).toList());
        try {
            registry.manifests().forEach(m -> registry.start(m));
            LOGGER.info("[engine/start] success");
            return Response.ok().build();
        }
        catch (RuntimeException e) {
            LOGGER.error("[engine/start] FAILED ({}: {})", e.getClass().getName(), e.getMessage(), e);
            throw e;
        }
    }

    @POST
    @Path("start/{id}")
    public Response start(@PathParam("id") String id) {
        LOGGER.info("[engine/start/{}] called", id);
        try {
            registry.manifests().stream()
                    .filter(m -> m.id().equals(id))
                    .findFirst()
                    .ifPresent(m -> registry.start(m));
            LOGGER.info("[engine/start/{}] success", id);
            return Response.ok().build();
        }
        catch (RuntimeException e) {
            LOGGER.error("[engine/start/{}] FAILED ({}: {})", id, e.getClass().getName(), e.getMessage(), e);
            throw e;
        }
    }

    @POST
    @Path("stop")
    public Response stop() {
        List<Debezium> running = registry.engines();
        LOGGER.info("[engine/stop] called; running engines: {}",
                running.stream().map(e -> e.manifest().id()).toList());
        if (running.isEmpty()) {
            LOGGER.info("[engine/stop] no running engines, returning 500");
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("No running engines found")
                    .build();
        }
        try {
            running.forEach(e -> registry.stop(e.manifest()));
            LOGGER.info("[engine/stop] success");
            return Response.ok().build();
        }
        catch (RuntimeException e) {
            LOGGER.error("[engine/stop] FAILED ({}: {})", e.getClass().getName(), e.getMessage(), e);
            throw e;
        }
    }

    @POST
    @Path("stop/{id}")
    public Response stop(@PathParam("id") String id) {
        LOGGER.info("[engine/stop/{}] called", id);
        try {
            registry.engines().stream()
                    .filter(e -> e.manifest().id().equals(id))
                    .findFirst()
                    .ifPresent(e -> registry.stop(e.manifest()));
            LOGGER.info("[engine/stop/{}] success", id);
            return Response.ok().build();
        }
        catch (RuntimeException e) {
            LOGGER.error("[engine/stop/{}] FAILED ({}: {})", id, e.getClass().getName(), e.getMessage(), e);
            throw e;
        }
    }
}
