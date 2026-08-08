package com.telemetry.stream

import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionServiceV1
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.cors.routing.CORS
import io.ktor.server.response.respond
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.send
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.time.Duration.Companion.seconds

private val log = LoggerFactory.getLogger("com.telemetry.stream.Main")

private const val GRPC_PORT = 50051
private const val HTTP_PORT = 8080
private val TOPOLOGY_INTERVAL = 1.seconds
private val WS_KEEPALIVE = 15.seconds

fun main() {
    val telemetry = TelemetryServiceImpl()
    val controls = ControlPlane()
    val json = Json { encodeDefaults = true }

    val grpcServer: Server = ServerBuilder.forPort(GRPC_PORT)
        .addService(telemetry)
        .addService(ProtoReflectionServiceV1.newInstance())
        .build()
        .start()

    log.info("gRPC listening on {}", GRPC_PORT)

    /*
     * One ticker serialises the graph once per interval and every connected client shares
     * that snapshot, rather than each socket recomputing it. replay = 1 hands a new client
     * the latest graph immediately instead of making it wait for the next tick, and
     * DROP_OLDEST means a slow client falls behind on its own without stalling the ticker.
     */
    val broadcaster = CoroutineScope(Dispatchers.Default + SupervisorJob())
    val topologyFeed = MutableSharedFlow<String>(
        replay = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
    )

    broadcaster.launch {
        while (isActive) {
            topologyFeed.emit(json.encodeToString(telemetry.topology().toDto()))
            delay(TOPOLOGY_INTERVAL)
        }
    }

    val httpServer = embeddedServer(Netty, port = HTTP_PORT) {
        install(CORS) {
            // The Vite dev server is a different origin, and fetch (unlike WebSocket) is
            // subject to CORS. Dev hosts only - tighten before this leaves a laptop.
            allowHost("localhost:5173")
            allowHost("127.0.0.1:5173")
            allowMethod(HttpMethod.Post)
            allowMethod(HttpMethod.Delete)
            allowHeader(HttpHeaders.ContentType)
        }
        install(WebSockets) {
            pingPeriodMillis = WS_KEEPALIVE.inWholeMilliseconds
            timeoutMillis = WS_KEEPALIVE.inWholeMilliseconds
        }
        install(ContentNegotiation) { json(json) }

        routing {
            get("/health") {
                call.respond(mapOf("status" to "ok"))
            }

            get("/api/topology") {
                call.respond(telemetry.topology().toDto())
            }

            // Operator controls. The generator polls /api/controls and does the real work;
            // the server only records what was asked for.
            get("/api/controls") {
                call.respond(controls.state())
            }

            post("/api/outage") {
                val durationMs = call.request.queryParameters["durationMs"]?.toLongOrNull()
                    ?: ControlPlane.DEFAULT_OUTAGE_MS
                controls.requestOutage(durationMs)
                log.info("outage requested for {}ms", durationMs)
                call.respond(controls.state())
            }

            delete("/api/outage") {
                controls.clearOutage()
                call.respond(controls.state())
            }

            post("/api/circuit-breakers") {
                val enabled = call.request.queryParameters["enabled"]?.toBooleanStrictOrNull() ?: true
                controls.setCircuitBreakersEnabled(enabled)
                log.info("circuit breakers {}", if (enabled) "enabled" else "disabled")
                call.respond(controls.state())
            }

            // Pushes the current graph topology to the visualiser once per second.
            webSocket("/ws/metrics") {
                topologyFeed.collect { snapshot -> send(snapshot) }
            }
        }
    }

    Runtime.getRuntime().addShutdownHook(
        Thread {
            broadcaster.cancel()
            grpcServer.shutdown()
        }
    )

    log.info("HTTP/WebSocket listening on {}", HTTP_PORT)
    httpServer.start(wait = true)

    broadcaster.cancel()
    grpcServer.shutdown()
}
