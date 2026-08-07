package com.telemetry.stream

import com.google.protobuf.Empty
import com.telemetry.stream.proto.ServiceMetric
import com.telemetry.stream.proto.TelemetryServiceGrpcKt
import com.telemetry.stream.proto.serviceMetric
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

/** Exercises the generated grpc-kotlin stubs over an in-process transport. */
class TelemetryGrpcServiceTest {

    private lateinit var registry: TopologyRegistry
    private lateinit var server: Server
    private lateinit var channel: ManagedChannel
    private lateinit var stub: TelemetryServiceGrpcKt.TelemetryServiceCoroutineStub

    @BeforeTest
    fun setUp() {
        val name = InProcessServerBuilder.generateName()
        registry = TopologyRegistry()
        server = InProcessServerBuilder.forName(name)
            .directExecutor()
            .addService(TelemetryGrpcService(registry))
            .build()
            .start()
        channel = InProcessChannelBuilder.forName(name).directExecutor().build()
        stub = TelemetryServiceGrpcKt.TelemetryServiceCoroutineStub(channel)
    }

    @AfterTest
    fun tearDown() {
        channel.shutdownNow()
        server.shutdownNow()
    }

    private fun metric(
        source: String,
        target: String,
        latencyMs: Double = 10.0,
        errorRate: Double = 0.0,
    ): ServiceMetric = serviceMetric {
        this.serviceId = source
        this.targetService = target
        this.timestamp = 1_700_000_000_000
        this.latencyMs = latencyMs
        this.errorRate = errorRate
    }

    @Test
    fun `streamMetrics reports health for the streaming service`() = runBlocking {
        val health = stub.streamMetrics(
            flowOf(
                metric("checkout", "payments", errorRate = 0.5),
                metric("checkout", "payments", errorRate = 0.5),
            )
        )

        assertEquals("checkout", health.serviceId)
        assertEquals(STATUS_CRITICAL, health.status)
    }

    @Test
    fun `streamMetrics flags slow but error-free traffic as warning`() = runBlocking {
        val health = stub.streamMetrics(flowOf(metric("checkout", "payments", latencyMs = 800.0)))
        assertEquals(STATUS_WARNING, health.status)
    }

    @Test
    fun `streamMetrics reports healthy traffic as healthy`() = runBlocking {
        val health = stub.streamMetrics(flowOf(metric("checkout", "payments")))
        assertEquals(STATUS_HEALTHY, health.status)
    }

    @Test
    fun `getGraphTopology returns nodes and averaged dependency edges`() = runBlocking {
        stub.streamMetrics(
            flowOf(
                metric("checkout", "payments", latencyMs = 10.0, errorRate = 0.0),
                metric("checkout", "payments", latencyMs = 30.0, errorRate = 0.2),
                metric("payments", "ledger"),
            )
        )

        val topology = stub.getGraphTopology(Empty.getDefaultInstance())

        assertEquals(listOf("checkout", "ledger", "payments"), topology.nodesList.map { it.serviceId })
        assertEquals(
            listOf("checkout->payments", "payments->ledger"),
            topology.edgesList.map { "${it.sourceServiceId}->${it.targetServiceId}" },
        )

        val checkoutEdge = topology.edgesList.first { it.sourceServiceId == "checkout" }
        assertEquals(2L, checkoutEdge.callCount)
        assertEquals(20.0, checkoutEdge.avgLatencyMs)
        assertEquals(0.1, checkoutEdge.avgErrorRate, absoluteTolerance = 1e-9)
    }

    @Test
    fun `blast radius counts transitive dependents`() = runBlocking {
        // checkout -> payments -> ledger
        stub.streamMetrics(flowOf(metric("checkout", "payments"), metric("payments", "ledger")))

        val byService = stub.getGraphTopology(Empty.getDefaultInstance())
            .nodesList.associate { it.serviceId to it.blastRadiusScore }

        // Nothing calls checkout; payments is reached by checkout; ledger by both.
        assertEquals(0.0, byService.getValue("checkout"))
        assertEquals(0.5, byService.getValue("payments"))
        assertEquals(1.0, byService.getValue("ledger"))
    }

    @Test
    fun `ingested metrics are broadcast to live subscribers`() = runBlocking {
        val received = mutableListOf<ServiceMetric>()
        val collector = launch(Dispatchers.Default) {
            registry.stream.take(2).toList(received)
        }

        // The broadcast has no replay, so wait until the subscriber is actually attached.
        withTimeout(5_000) { registry.subscriberCount.first { it > 0 } }

        stub.streamMetrics(flowOf(metric("checkout", "payments"), metric("payments", "ledger")))

        withTimeout(5_000) { collector.join() }
        assertEquals(listOf("checkout", "payments"), received.map { it.serviceId })
    }
}
