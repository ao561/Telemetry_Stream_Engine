package com.telemetry.stream.tools

import com.google.protobuf.Empty
import com.telemetry.stream.proto.ServiceMetric
import com.telemetry.stream.proto.TelemetryServiceGrpcKt
import com.telemetry.stream.proto.serviceMetric
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import kotlin.random.Random

/*
 * Load generator for a small microservice estate:
 *
 *     frontend-gateway -> payment-api -> auth-service -> db-primary
 *
 * Each service streams its own outbound calls over a long-lived client-streaming RPC, the way a
 * real sidecar agent would. Every SPIKE_INTERVAL_MS a fault is injected at db-primary; because the
 * server derives health from a service's *outbound* calls, the fault shows up as auth-service (its
 * caller) going CRITICAL, then propagates up the chain one hop at a time.
 *
 * Run against a server started by `./gradlew run`:
 *     ./gradlew mockCluster
 *     ./gradlew mockCluster --args="localhost:50051"
 */

private const val DEFAULT_TARGET = "localhost:50051"
private const val SAMPLE_INTERVAL_MS = 500L
private const val RECONNECT_DELAY_MS = 2_000L

private val SPIKE_INTERVAL_MS = envLong("SPIKE_INTERVAL_MS", 15_000L)
private val SPIKE_DURATION_MS = envLong("SPIKE_MIN_MS", 3_000L)..envLong("SPIKE_MAX_MS", 5_000L)

private val TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss")

private fun envLong(name: String, fallback: Long): Long =
    System.getenv(name)?.toLongOrNull() ?: fallback

/** One directed dependency, with the traffic it emits when healthy and when degraded. */
private class Dependency(
    val source: String,
    val target: String,
    /** How far this hop sits above the fault, so the cascade travels rather than teleports. */
    val hopDelayMs: Long,
    private val healthyLatencyMs: ClosedFloatingPointRange<Double>,
    private val healthyErrorRate: ClosedFloatingPointRange<Double>,
    private val faultLatencyMs: ClosedFloatingPointRange<Double>,
    private val faultErrorRate: ClosedFloatingPointRange<Double>,
) {
    fun sample(spikes: SpikeSchedule): ServiceMetric {
        val now = System.currentTimeMillis()
        val degraded = spikes.isDegraded(now, hopDelayMs)
        return serviceMetric {
            serviceId = source
            targetService = target
            timestamp = now
            latencyMs = (if (degraded) faultLatencyMs else healthyLatencyMs).sample()
            errorRate = (if (degraded) faultErrorRate else healthyErrorRate).sample()
        }
    }
}

// Ordered outward from the fault at db-primary.
private val CLUSTER = listOf(
    Dependency(
        source = "auth-service",
        target = "db-primary",
        hopDelayMs = 0,
        healthyLatencyMs = 5.0..20.0,
        healthyErrorRate = 0.0..0.01,
        faultLatencyMs = 400.0..900.0,
        faultErrorRate = 0.45..0.85,
    ),
    Dependency(
        source = "payment-api",
        target = "auth-service",
        hopDelayMs = 500,
        healthyLatencyMs = 20.0..60.0,
        healthyErrorRate = 0.0..0.01,
        faultLatencyMs = 250.0..600.0,
        faultErrorRate = 0.40..0.70,
    ),
    Dependency(
        source = "frontend-gateway",
        target = "payment-api",
        hopDelayMs = 1_000,
        healthyLatencyMs = 40.0..120.0,
        healthyErrorRate = 0.0..0.02,
        faultLatencyMs = 220.0..480.0,
        faultErrorRate = 0.20..0.45,
    ),
)

/** Shared fault window. Written by one coroutine, read by all the streams. */
private class SpikeSchedule {
    @Volatile
    private var startedAt = Long.MIN_VALUE

    @Volatile
    private var endsAt = Long.MIN_VALUE

    fun begin(now: Long, durationMs: Long) {
        startedAt = now
        endsAt = now + durationMs
    }

    /** A hop [hopDelayMs] above the fault both sees it later and recovers later. */
    fun isDegraded(now: Long, hopDelayMs: Long): Boolean =
        now >= startedAt + hopDelayMs && now < endsAt + hopDelayMs

    fun isActive(now: Long): Boolean = now in startedAt until endsAt
}

private fun ClosedFloatingPointRange<Double>.sample(): Double =
    Random.nextDouble(start, endInclusive)

fun main(args: Array<String>) = runBlocking<Unit> {
    val target = args.firstOrNull() ?: DEFAULT_TARGET
    val channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build()
    val stub = TelemetryServiceGrpcKt.TelemetryServiceCoroutineStub(channel)
    val spikes = SpikeSchedule()

    println("streaming synthetic cluster telemetry to $target")
    CLUSTER.forEach { println("  ${it.source} -> ${it.target}") }
    println("sample every ${SAMPLE_INTERVAL_MS}ms, db-primary fault every ${SPIKE_INTERVAL_MS}ms")
    println()

    Runtime.getRuntime().addShutdownHook(Thread { channel.shutdownNow() })

    try {
        coroutineScope {
            CLUSTER.forEach { dependency -> launch { streamFor(stub, dependency, spikes) } }
            launch { driveSpikes(spikes) }
            launch { reportCluster(stub, spikes) }
        }
    } finally {
        channel.shutdownNow()
    }
}

/** Holds one long-lived client stream open per service, reconnecting if the server goes away. */
private suspend fun streamFor(
    stub: TelemetryServiceGrpcKt.TelemetryServiceCoroutineStub,
    dependency: Dependency,
    spikes: SpikeSchedule,
) {
    while (currentCoroutineContext().isActive) {
        try {
            // The flow never completes, so the RPC stays open and the server keeps ingesting.
            stub.streamMetrics(
                flow {
                    while (currentCoroutineContext().isActive) {
                        emit(dependency.sample(spikes))
                        delay(SAMPLE_INTERVAL_MS)
                    }
                }
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            println("  stream ${dependency.source} dropped (${e.message}); retrying")
            delay(RECONNECT_DELAY_MS)
        }
    }
}

private suspend fun driveSpikes(spikes: SpikeSchedule) {
    while (currentCoroutineContext().isActive) {
        delay(SPIKE_INTERVAL_MS)
        val duration = Random.nextLong(SPIKE_DURATION_MS.first, SPIKE_DURATION_MS.last)
        spikes.begin(System.currentTimeMillis(), duration)
        println("  !! db-primary fault injected for ${duration}ms")
    }
}

/** Polls the server's own view of the graph so the cascade is visible on the console. */
private suspend fun reportCluster(
    stub: TelemetryServiceGrpcKt.TelemetryServiceCoroutineStub,
    spikes: SpikeSchedule,
) {
    while (currentCoroutineContext().isActive) {
        delay(1_000)
        val topology = try {
            stub.getGraphTopology(Empty.getDefaultInstance())
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            continue
        }

        val marker = if (spikes.isActive(System.currentTimeMillis())) "FAULT" else "  -  "
        val nodes = topology.nodesList.joinToString("  ") { node ->
            "${node.serviceId}=${node.status}(br=${node.blastRadiusScore.toInt()})"
        }
        println("${LocalTime.now().format(TIME_FORMAT)} [$marker] $nodes")
    }
}
