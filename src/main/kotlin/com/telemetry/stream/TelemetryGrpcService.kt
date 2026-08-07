package com.telemetry.stream

import com.google.protobuf.Empty
import com.telemetry.stream.proto.GraphTopologyResponse
import com.telemetry.stream.proto.HealthStatus
import com.telemetry.stream.proto.ServiceMetric
import com.telemetry.stream.proto.TelemetryServiceGrpcKt
import kotlinx.coroutines.flow.Flow

class TelemetryGrpcService(
    private val registry: TopologyRegistry,
) : TelemetryServiceGrpcKt.TelemetryServiceCoroutineImplBase() {

    /**
     * An agent reports on behalf of one service, so the reply describes the last
     * service seen on the stream.
     */
    override suspend fun streamMetrics(requests: Flow<ServiceMetric>): HealthStatus {
        var reportingService = ""
        requests.collect { metric ->
            if (metric.serviceId.isNotEmpty()) reportingService = metric.serviceId
            registry.record(metric)
        }
        return registry.health(reportingService)
    }

    override suspend fun getGraphTopology(request: Empty): GraphTopologyResponse = registry.topology()
}
