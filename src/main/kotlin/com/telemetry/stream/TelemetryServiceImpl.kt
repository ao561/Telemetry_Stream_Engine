package com.telemetry.stream

import com.google.protobuf.Empty
import com.telemetry.stream.proto.GraphTopologyResponse
import com.telemetry.stream.proto.HealthStatus
import com.telemetry.stream.proto.ServiceMetric
import com.telemetry.stream.proto.TelemetryServiceGrpcKt
import com.telemetry.stream.proto.dependencyEdge
import com.telemetry.stream.proto.graphTopologyResponse
import com.telemetry.stream.proto.healthStatus
import com.telemetry.stream.proto.serviceNode
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import java.util.concurrent.ConcurrentHashMap

const val STATUS_HEALTHY = "HEALTHY"
const val STATUS_WARNING = "WARNING"
const val STATUS_CRITICAL = "CRITICAL"

/** How many of the most recent metrics are retained per service. */
const val METRIC_WINDOW_SIZE = 50

private const val CRITICAL_ERROR_RATE = 0.15
private const val CRITICAL_LATENCY_MS = 200.0
private const val WARNING_ERROR_RATE = 0.05

/** Rolling averages over a service's retained window. */
data class RollingStats(
    val sampleCount: Int,
    val avgLatencyMs: Double,
    val avgErrorRate: Double,
) {
    companion object {
        val EMPTY = RollingStats(sampleCount = 0, avgLatencyMs = 0.0, avgErrorRate = 0.0)
    }
}

/**
 * Keeps the last [METRIC_WINDOW_SIZE] metrics per service, derives health from their
 * rolling averages, and serves the dependency graph built from the ingest stream.
 *
 * All state is in-memory and lives for the process lifetime.
 */
class TelemetryServiceImpl(
    replay: Int = 0,
    extraBufferCapacity: Int = 512,
) : TelemetryServiceGrpcKt.TelemetryServiceCoroutineImplBase() {

    /** service_id -> bounded window of that service's most recent metrics. */
    private val metricsByService = ConcurrentHashMap<String, MetricWindow>()

    /** service_id -> the services it calls. Retained even once their metrics age out. */
    private val dependencies = ConcurrentHashMap<String, MutableSet<String>>()

    private val _stream = MutableSharedFlow<ServiceMetric>(
        replay = replay,
        extraBufferCapacity = extraBufferCapacity,
        // A slow visualiser must never stall ingest.
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
    )

    /** Live feed of everything ingested, fanned out to the Ktor WebSocket. */
    val stream: SharedFlow<ServiceMetric> = _stream.asSharedFlow()

    /** Live subscriber count on [stream] - lets callers skip work when nobody is watching. */
    val subscriberCount: StateFlow<Int> get() = _stream.subscriptionCount

    // ---------------------------------------------------------------- RPCs

    /**
     * An agent reports on behalf of one service, so the reply describes the last
     * service seen on the stream.
     */
    override suspend fun streamMetrics(requests: Flow<ServiceMetric>): HealthStatus {
        var reportingService = ""
        requests.collect { metric ->
            record(metric)
            if (metric.serviceId.isNotEmpty()) reportingService = metric.serviceId
        }
        return healthOf(reportingService)
    }

    override suspend fun getGraphTopology(request: Empty): GraphTopologyResponse = topology()

    // ------------------------------------------------------------- Ingest

    suspend fun record(metric: ServiceMetric) {
        val source = metric.serviceId
        val target = metric.targetService

        if (source.isNotEmpty()) {
            metricsByService.computeIfAbsent(source) { MetricWindow() }.add(metric)
            if (target.isNotEmpty()) {
                dependencies.computeIfAbsent(source) { ConcurrentHashMap.newKeySet() }.add(target)
            }
        }

        _stream.emit(metric)
    }

    // ------------------------------------------------------------ Queries

    fun rollingStats(serviceId: String): RollingStats =
        metricsByService[serviceId]?.rolling() ?: RollingStats.EMPTY

    fun healthOf(serviceId: String): HealthStatus = healthStatus {
        this.serviceId = serviceId
        this.status = statusOf(serviceId)
        this.blastRadiusScore = blastRadiusOf(serviceId)
    }

    fun topology(): GraphTopologyResponse = graphTopologyResponse {
        nodes += knownServices().sorted().map { id ->
            serviceNode {
                serviceId = id
                status = statusOf(id)
                blastRadiusScore = blastRadiusOf(id)
            }
        }
        edges += dependencyPairs().map { (source, target) ->
            // Averages cover the calls still inside the source's retained window.
            val calls = metricsByService[source]?.snapshot().orEmpty()
                .filter { it.targetService == target }
            dependencyEdge {
                sourceServiceId = source
                targetServiceId = target
                callCount = calls.size.toLong()
                avgLatencyMs = if (calls.isEmpty()) 0.0 else calls.sumOf { it.latencyMs } / calls.size
                avgErrorRate = if (calls.isEmpty()) 0.0 else calls.sumOf { it.errorRate } / calls.size
                // The window is oldest-first, so the last retained call is the current state.
                // A breaker that trips mid-window must not read as closed for another 25s.
                circuitOpen = calls.lastOrNull()?.circuitOpen ?: false
            }
        }
    }

    /**
     * Thresholds are applied to the rolling averages, not to individual samples, so a
     * single slow call cannot flip a service to CRITICAL on its own.
     */
    private fun statusOf(serviceId: String): String {
        val stats = rollingStats(serviceId)
        if (stats.sampleCount == 0) return STATUS_HEALTHY

        return when {
            stats.avgErrorRate > CRITICAL_ERROR_RATE || stats.avgLatencyMs > CRITICAL_LATENCY_MS ->
                STATUS_CRITICAL
            stats.avgErrorRate > WARNING_ERROR_RATE -> STATUS_WARNING
            else -> STATUS_HEALTHY
        }
    }

    /**
     * How many services would be affected if [serviceId] failed: the count of nodes that
     * depend on it, directly or transitively. Reverse reachability over the edge set.
     */
    private fun blastRadiusOf(serviceId: String): Double {
        val callers = mutableMapOf<String, MutableList<String>>()
        dependencies.forEach { (source, targets) ->
            targets.forEach { target -> callers.getOrPut(target) { mutableListOf() }.add(source) }
        }

        val dependents = mutableSetOf<String>()
        val queue = ArrayDeque<String>().apply { add(serviceId) }
        while (queue.isNotEmpty()) {
            for (caller in callers[queue.removeFirst()].orEmpty()) {
                // The guard also breaks dependency cycles.
                if (dependents.add(caller)) queue.add(caller)
            }
        }
        dependents.remove(serviceId)

        return dependents.size.toDouble()
    }

    private fun knownServices(): Set<String> = buildSet {
        addAll(metricsByService.keys)
        dependencies.forEach { (source, targets) ->
            add(source)
            addAll(targets)
        }
    }

    private fun dependencyPairs(): List<Pair<String, String>> =
        dependencies.entries
            .flatMap { (source, targets) -> targets.map { source to it } }
            .sortedWith(compareBy({ it.first }, { it.second }))

    /**
     * A bounded FIFO window of one service's most recent metrics. Averages are recomputed
     * from the retained samples rather than carried as running sums, which keeps them exact
     * as entries are evicted - cheap at [METRIC_WINDOW_SIZE] elements.
     */
    private class MetricWindow {
        private val window = ArrayDeque<ServiceMetric>()

        @Synchronized
        fun add(metric: ServiceMetric) {
            window.addLast(metric)
            while (window.size > METRIC_WINDOW_SIZE) window.removeFirst()
        }

        @Synchronized
        fun snapshot(): List<ServiceMetric> = window.toList()

        @Synchronized
        fun rolling(): RollingStats {
            if (window.isEmpty()) return RollingStats.EMPTY
            return RollingStats(
                sampleCount = window.size,
                avgLatencyMs = window.sumOf { it.latencyMs } / window.size,
                avgErrorRate = window.sumOf { it.errorRate } / window.size,
            )
        }
    }
}
