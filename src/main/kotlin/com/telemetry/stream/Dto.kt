package com.telemetry.stream

import com.telemetry.stream.proto.DependencyEdge
import com.telemetry.stream.proto.GraphTopologyResponse
import com.telemetry.stream.proto.ServiceMetric
import com.telemetry.stream.proto.ServiceNode
import kotlinx.serialization.Serializable

/** JSON projections of the proto types, for the Ktor HTTP/WebSocket surface. */

@Serializable
data class ServiceMetricDto(
    val serviceId: String,
    val targetService: String,
    val timestamp: Long,
    val latencyMs: Double,
    val errorRate: Double,
)

@Serializable
data class ServiceNodeDto(
    val serviceId: String,
    val status: String,
    val blastRadiusScore: Double,
)

@Serializable
data class DependencyEdgeDto(
    val source: String,
    val target: String,
    val avgLatencyMs: Double,
    val avgErrorRate: Double,
    val callCount: Long,
    val circuitOpen: Boolean,
)

@Serializable
data class GraphTopologyDto(
    val nodes: List<ServiceNodeDto>,
    val edges: List<DependencyEdgeDto>,
)

fun ServiceMetric.toDto(): ServiceMetricDto = ServiceMetricDto(
    serviceId = serviceId,
    targetService = targetService,
    timestamp = timestamp,
    latencyMs = latencyMs,
    errorRate = errorRate,
)

fun ServiceNode.toDto(): ServiceNodeDto = ServiceNodeDto(
    serviceId = serviceId,
    status = status,
    blastRadiusScore = blastRadiusScore,
)

fun DependencyEdge.toDto(): DependencyEdgeDto = DependencyEdgeDto(
    source = sourceServiceId,
    target = targetServiceId,
    avgLatencyMs = avgLatencyMs,
    avgErrorRate = avgErrorRate,
    callCount = callCount,
    circuitOpen = circuitOpen,
)

fun GraphTopologyResponse.toDto(): GraphTopologyDto = GraphTopologyDto(
    nodes = nodesList.map { it.toDto() },
    edges = edgesList.map { it.toDto() },
)

@Serializable
data class ControlStateDto(
    val outageActive: Boolean,
    val outageRemainingMs: Long,
    val circuitBreakersEnabled: Boolean,
)
