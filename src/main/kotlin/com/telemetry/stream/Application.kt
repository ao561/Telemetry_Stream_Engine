package com.telemetry.stream

import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionServiceV1
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.send
import kotlinx.serialization.json.Json

private const val GRPC_PORT = 50051
private const val HTTP_PORT = 8080

fun main() {
    val registry = TopologyRegistry()
    val json = Json { encodeDefaults = true }

    val grpcServer: Server = ServerBuilder.forPort(GRPC_PORT)
        .addService(TelemetryGrpcService(registry))
        .addService(ProtoReflectionServiceV1.newInstance())
        .build()
        .start()

    Runtime.getRuntime().addShutdownHook(Thread { grpcServer.shutdown() })

    embeddedServer(Netty, port = HTTP_PORT) {
        install(WebSockets)
        install(ContentNegotiation) { json(json) }

        routing {
            get("/health") {
                call.respond(mapOf("status" to "ok"))
            }

            get("/api/topology") {
                call.respond(registry.topology().toDto())
            }

            // Live tail of the ingest feed for the visualiser front end.
            webSocket("/ws/metrics") {
                registry.stream.collect { metric ->
                    send(json.encodeToString(metric.toDto()))
                }
            }
        }
    }.start(wait = true)

    grpcServer.shutdown()
}
