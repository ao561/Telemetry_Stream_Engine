package com.telemetry.stream

import com.telemetry.stream.proto.GraphTopologyResponse
import com.telemetry.stream.proto.HealthStatus
import com.telemetry.stream.proto.ServiceMetric
import com.telemetry.stream.proto.dependencyEdge
import com.telemetry.stream.proto.graphTopologyResponse
import com.telemetry.stream.proto.healthStatus
import com.telemetry.stream.proto.serviceNode
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import java.util.concurrent.ConcurrentHashMap

const val STATUS_HEALTHY = "HEALTHY"
const val STATUS_WARNING = "WARNING"
const val STATUS_CRITICAL = "CRITICAL"

// Thresholds applied to a service's outbound calls.
private const val CRITICAL_ERROR_RATE = 0.10
private const val WARNING_ERROR_RATE = 0.01
private const val WARNING_LATENCY_MS = 500.0

/**
 * In-memory rollup of the service dependency graph, plus a hot broadcast of the raw
 * feed that the Ktor WebSocket fans out to the visualiser.
 */
class TopologyRegistry(
    replay: Int = 0,
    extraBufferCapacity: Int = 512,
) {
    private val edges = ConcurrentHashMap<EdgeKey, EdgeState>()
    private val knownServices = ConcurrentHashMap.newKeySet<String>()

    private val _stream = MutableSharedFlow<ServiceMetric>(
        replay = replay,
        extraBufferCapacity = extraBufferCapacity,
        // A slow visualiser must never stall ingest.
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
    )

    val stream: SharedFlow<ServiceMetric> = _stream.asSharedFlow()

    /** Live subscriber count on [stream] - lets callers skip work when nobody is watching. */
    val subscriberCount: StateFlow<Int> get() = _stream.subscriptionCount

    suspend fun record(metric: ServiceMetric) {
        val source = metric.serviceId
        val target = metric.targetService

        if (source.isNotEmpty()) knownServices.add(source)
        if (target.isNotEmpty()) knownServices.add(target)
        if (source.isNotEmpty() && target.isNotEmpty()) {
            edges.compute(EdgeKey(source, target)) { _, previous ->
                (previous ?: EdgeState.EMPTY).plus(metric)
            }
        }

        _stream.emit(metric)
    }

    fun health(serviceId: String): HealthStatus = healthStatus {
        this.serviceId = serviceId
        this.status = statusOf(serviceId)
        this.blastRadiusScore = blastRadiusOf(serviceId)
    }

    fun topology(): GraphTopologyResponse = graphTopologyResponse {
        nodes += knownServices.sorted().map { id ->
            serviceNode {
                serviceId = id
                status = statusOf(id)
                blastRadiusScore = blastRadiusOf(id)
            }
        }
        edges += this@TopologyRegistry.edges.entries
            .sortedWith(compareBy({ it.key.source }, { it.key.target }))
            .map { (key, state) ->
                dependencyEdge {
                    sourceServiceId = key.source
                    targetServiceId = key.target
                    avgLatencyMs = state.avgLatencyMs
                    avgErrorRate = state.avgErrorRate
                    callCount = state.callCount
                }
            }
    }

    /** Health of a service, judged by the calls it makes to its dependencies. */
    private fun statusOf(serviceId: String): String {
        val outbound = edges.entries.filter { it.key.source == serviceId }
        val calls = outbound.sumOf { it.value.callCount }
        if (calls == 0L) return STATUS_HEALTHY

        val errorRate = outbound.sumOf { it.value.errorRateSum } / calls
        val latencyMs = outbound.sumOf { it.value.latencySum } / calls

        return when {
            errorRate >= CRITICAL_ERROR_RATE -> STATUS_CRITICAL
            errorRate >= WARNING_ERROR_RATE || latencyMs > WARNING_LATENCY_MS -> STATUS_WARNING
            else -> STATUS_HEALTHY
        }
    }

    /**
     * Fraction of the other known services that transitively depend on [serviceId] -
     * i.e. how much of the estate is exposed if it fails. 0.0 means nothing calls it,
     * 1.0 means everything does, directly or indirectly.
     */
    private fun blastRadiusOf(serviceId: String): Double {
        val others = knownServices.size - 1
        if (others <= 0) return 0.0

        val callers = edges.keys.groupBy({ it.target }, { it.source })
        val dependents = mutableSetOf<String>()
        val queue = ArrayDeque<String>().apply { add(serviceId) }
        while (queue.isNotEmpty()) {
            for (caller in callers[queue.removeFirst()].orEmpty()) {
                // The guard also breaks dependency cycles.
                if (dependents.add(caller)) queue.add(caller)
            }
        }
        dependents.remove(serviceId)

        return dependents.size.toDouble() / others
    }

    private data class EdgeKey(val source: String, val target: String)

    private data class EdgeState(
        val callCount: Long,
        val latencySum: Double,
        val errorRateSum: Double,
    ) {
        val avgLatencyMs: Double get() = if (callCount == 0L) 0.0 else latencySum / callCount
        val avgErrorRate: Double get() = if (callCount == 0L) 0.0 else errorRateSum / callCount

        fun plus(metric: ServiceMetric): EdgeState = EdgeState(
            callCount = callCount + 1,
            latencySum = latencySum + metric.latencyMs,
            errorRateSum = errorRateSum + metric.errorRate,
        )

        companion object {
            val EMPTY = EdgeState(callCount = 0, latencySum = 0.0, errorRateSum = 0.0)
        }
    }
}
