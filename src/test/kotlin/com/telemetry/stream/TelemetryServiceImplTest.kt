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
import kotlinx.coroutines.flow.asFlow
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

/** Exercises [TelemetryServiceImpl] through the generated grpc-kotlin stubs, in-process. */
class TelemetryServiceImplTest {

    private lateinit var telemetry: TelemetryServiceImpl
    private lateinit var server: Server
    private lateinit var channel: ManagedChannel
    private lateinit var stub: TelemetryServiceGrpcKt.TelemetryServiceCoroutineStub

    @BeforeTest
    fun setUp() {
        val name = InProcessServerBuilder.generateName()
        telemetry = TelemetryServiceImpl()
        server = InProcessServerBuilder.forName(name)
            .directExecutor()
            .addService(telemetry)
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

    // ------------------------------------------------------- status rules

    @Test
    fun `error rate above 0_15 is critical`() = runBlocking {
        val health = stub.streamMetrics(flowOf(metric("checkout", "payments", errorRate = 0.2)))

        assertEquals("checkout", health.serviceId)
        assertEquals(STATUS_CRITICAL, health.status)
    }

    @Test
    fun `latency above 200ms is critical even with no errors`() = runBlocking {
        val health = stub.streamMetrics(flowOf(metric("checkout", "payments", latencyMs = 250.0)))
        assertEquals(STATUS_CRITICAL, health.status)
    }

    @Test
    fun `error rate above 0_05 is a warning`() = runBlocking {
        val health = stub.streamMetrics(flowOf(metric("checkout", "payments", errorRate = 0.1)))
        assertEquals(STATUS_WARNING, health.status)
    }

    @Test
    fun `clean traffic is healthy`() = runBlocking {
        val health = stub.streamMetrics(flowOf(metric("checkout", "payments")))
        assertEquals(STATUS_HEALTHY, health.status)
    }

    @Test
    fun `thresholds are exclusive at the boundary`() = runBlocking {
        // 0.15 is not > 0.15, so this lands in WARNING rather than CRITICAL.
        val atCritical = stub.streamMetrics(flowOf(metric("a", "b", errorRate = 0.15)))
        assertEquals(STATUS_WARNING, atCritical.status)

        // 200.0 is not > 200.0, and 0.05 is not > 0.05.
        val atWarning = stub.streamMetrics(
            flowOf(metric("c", "d", latencyMs = 200.0, errorRate = 0.05))
        )
        assertEquals(STATUS_HEALTHY, atWarning.status)
    }

    // ---------------------------------------------------- rolling window

    @Test
    fun `status follows the rolling average, not a single sample`() = runBlocking {
        // One bad call among ten: mean error rate is 0.02, below the 0.05 warning line.
        val metrics = List(9) { metric("checkout", "payments", errorRate = 0.0) } +
            metric("checkout", "payments", errorRate = 0.2)

        val health = stub.streamMetrics(metrics.asFlow())

        assertEquals(STATUS_HEALTHY, health.status)
        assertEquals(0.02, telemetry.rollingStats("checkout").avgErrorRate, absoluteTolerance = 1e-9)
    }

    @Test
    fun `window retains only the last 50 metrics per service`() = runBlocking {
        // 10 terrible calls, then 50 clean ones - the bad ones should all be evicted.
        val metrics = List(10) { metric("checkout", "payments", latencyMs = 5_000.0, errorRate = 1.0) } +
            List(METRIC_WINDOW_SIZE) { metric("checkout", "payments", latencyMs = 10.0, errorRate = 0.0) }

        val health = stub.streamMetrics(metrics.asFlow())

        val stats = telemetry.rollingStats("checkout")
        assertEquals(METRIC_WINDOW_SIZE, stats.sampleCount)
        assertEquals(10.0, stats.avgLatencyMs)
        assertEquals(0.0, stats.avgErrorRate)
        assertEquals(STATUS_HEALTHY, health.status)
    }

    @Test
    fun `each service gets its own window`() = runBlocking {
        stub.streamMetrics(flowOf(metric("checkout", "payments", errorRate = 0.9)))
        stub.streamMetrics(flowOf(metric("search", "index", errorRate = 0.0)))

        assertEquals(1, telemetry.rollingStats("checkout").sampleCount)
        assertEquals(0.9, telemetry.rollingStats("checkout").avgErrorRate)
        assertEquals(1, telemetry.rollingStats("search").sampleCount)
        assertEquals(0.0, telemetry.rollingStats("search").avgErrorRate)
    }

    // ------------------------------------------------------ blast radius

    @Test
    fun `blast radius counts transitive dependents`() = runBlocking {
        // checkout -> payments -> ledger
        stub.streamMetrics(flowOf(metric("checkout", "payments"), metric("payments", "ledger")))

        val byService = stub.getGraphTopology(Empty.getDefaultInstance())
            .nodesList.associate { it.serviceId to it.blastRadiusScore }

        assertEquals(0.0, byService.getValue("checkout")) // nothing depends on it
        assertEquals(1.0, byService.getValue("payments")) // checkout
        assertEquals(2.0, byService.getValue("ledger"))   // payments, and checkout via payments
    }

    @Test
    fun `blast radius terminates on dependency cycles`() = runBlocking {
        stub.streamMetrics(flowOf(metric("a", "b"), metric("b", "c"), metric("c", "a")))

        val byService = stub.getGraphTopology(Empty.getDefaultInstance())
            .nodesList.associate { it.serviceId to it.blastRadiusScore }

        // Every node reaches every other node, itself excluded.
        assertEquals(listOf(2.0, 2.0, 2.0), listOf("a", "b", "c").map { byService.getValue(it) })
    }

    // --------------------------------------------------------- topology

    @Test
    fun `topology returns nodes with statuses and averaged dependency links`() = runBlocking {
        stub.streamMetrics(
            flowOf(
                metric("checkout", "payments", latencyMs = 10.0, errorRate = 0.0),
                metric("checkout", "payments", latencyMs = 30.0, errorRate = 0.4),
                metric("checkout", "search", latencyMs = 500.0, errorRate = 0.0),
                metric("payments", "ledger"),
            )
        )

        val topology = stub.getGraphTopology(Empty.getDefaultInstance())

        assertEquals(
            listOf("checkout", "ledger", "payments", "search"),
            topology.nodesList.map { it.serviceId },
        )
        assertEquals(
            listOf("checkout->payments", "checkout->search", "payments->ledger"),
            topology.edgesList.map { "${it.sourceServiceId}->${it.targetServiceId}" },
        )

        val paymentsEdge = topology.edgesList.first { it.targetServiceId == "payments" }
        assertEquals(2L, paymentsEdge.callCount)
        assertEquals(20.0, paymentsEdge.avgLatencyMs)
        assertEquals(0.2, paymentsEdge.avgErrorRate, absoluteTolerance = 1e-9)

        // checkout's rolling latency is (10+30+500)/3 = 180 -> under the 200ms line,
        // but its error rate averages 0.133 -> WARNING.
        val checkout = topology.nodesList.first { it.serviceId == "checkout" }
        assertEquals(STATUS_WARNING, checkout.status)

        // A service seen only as a target has no metrics of its own.
        val ledger = topology.nodesList.first { it.serviceId == "ledger" }
        assertEquals(STATUS_HEALTHY, ledger.status)
    }

    @Test
    fun `dependency links survive their metrics ageing out of the window`() = runBlocking {
        stub.streamMetrics(flowOf(metric("checkout", "legacy-billing")))
        // Push the legacy-billing call out of checkout's 50-entry window.
        stub.streamMetrics(List(METRIC_WINDOW_SIZE) { metric("checkout", "payments") }.asFlow())

        val edges = stub.getGraphTopology(Empty.getDefaultInstance())
            .edgesList.associateBy { it.targetServiceId }

        // Edge is still in the graph, but no retained samples back it.
        assertEquals(0L, edges.getValue("legacy-billing").callCount)
        assertEquals(METRIC_WINDOW_SIZE.toLong(), edges.getValue("payments").callCount)
    }

    // -------------------------------------------------------- broadcast

    @Test
    fun `ingested metrics are broadcast to live subscribers`() = runBlocking {
        val received = mutableListOf<ServiceMetric>()
        val collector = launch(Dispatchers.Default) {
            telemetry.stream.take(2).toList(received)
        }

        // The broadcast has no replay, so wait until the subscriber is actually attached.
        withTimeout(5_000) { telemetry.subscriberCount.first { it > 0 } }

        stub.streamMetrics(flowOf(metric("checkout", "payments"), metric("payments", "ledger")))

        withTimeout(5_000) { collector.join() }
        assertEquals(listOf("checkout", "payments"), received.map { it.serviceId })
    }
}
