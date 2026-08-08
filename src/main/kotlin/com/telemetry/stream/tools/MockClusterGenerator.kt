package com.telemetry.stream.tools

import com.google.protobuf.Empty
import com.telemetry.stream.OutageStateDto
import com.telemetry.stream.proto.ServiceMetric
import com.telemetry.stream.proto.TelemetryServiceGrpcKt
import com.telemetry.stream.proto.serviceMetric
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
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

/** Where the dashboard records operator-triggered outages. */
private val CONTROL_URL = System.getenv("CONTROL_URL") ?: "http://localhost:8080/api/outage"
private const val CONTROL_POLL_MS = 500L

/** How many ticks of the cascade breakdown to print when an outage starts. */
private const val BREAKDOWN_TICKS = 3

/** Wire time on top of whatever the dependency itself takes. */
private val NETWORK_JITTER_MS = 0.4..2.5
private val JSON = Json { ignoreUnknownKeys = true }

private val SPIKE_INTERVAL_MS = envLong("SPIKE_INTERVAL_MS", 15_000L)
private val SPIKE_DURATION_MS = envLong("SPIKE_MIN_MS", 3_000L)..envLong("SPIKE_MAX_MS", 5_000L)

private val TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss")

private fun envLong(name: String, fallback: Long): Long =
    System.getenv(name)?.toLongOrNull() ?: fallback

/**
 * A service's own execution time, excluding anything it waits on downstream.
 *
 * Only a leaf carries a faulted profile: the fault is injected at db-primary and everything
 * above it inherits the delay through [tickServiceTimes] rather than being scripted.
 */
private class Service(
    val id: String,
    val localMs: ClosedFloatingPointRange<Double>,
    val faultedLocalMs: ClosedFloatingPointRange<Double>? = null,
)

/** One directed dependency. Latency is derived; only the error rate is scripted per hop. */
private class Dependency(
    val source: String,
    val target: String,
    /** Staggers the *error* cascade. Latency propagation gets its delay for free, one hop per tick. */
    val hopDelayMs: Long,
    private val healthyErrorRate: ClosedFloatingPointRange<Double>,
    private val faultedErrorRate: ClosedFloatingPointRange<Double>? = null,
) {
    fun sample(cluster: ClusterState, spikes: SpikeSchedule): ServiceMetric {
        val now = System.currentTimeMillis()
        val errors = (if (spikes.isDegraded(now, hopDelayMs)) faultedErrorRate else null)
            ?: healthyErrorRate

        return serviceMetric {
            serviceId = source
            targetService = target
            timestamp = now
            // What the caller waits for is however long the dependency currently takes,
            // which already includes that dependency's own downstream wait.
            latencyMs = cluster.serviceTimeOf(target) + NETWORK_JITTER_MS.sample()
            errorRate = errors.sample()
        }
    }
}

/*
 * frontend-gateway ----\
 *                       >--> payment-api --> auth-service --+--> db-primary   <- fault injected here
 * notification-worker -/                                    \--> redis-cache  <- healthy sibling
 */
private val SERVICES = listOf(
    Service(id = "db-primary", localMs = 4.0..14.0, faultedLocalMs = 400.0..900.0),
    Service(id = "redis-cache", localMs = 0.8..4.0),
    Service(id = "auth-service", localMs = 3.0..10.0),
    Service(id = "payment-api", localMs = 8.0..22.0),
    Service(id = "frontend-gateway", localMs = 12.0..35.0),
    Service(id = "notification-worker", localMs = 15.0..40.0),
)

// Ordered outward from the fault at db-primary.
private val CLUSTER = listOf(
    Dependency(
        source = "auth-service",
        target = "db-primary",
        hopDelayMs = 0,
        healthyErrorRate = 0.0..0.01,
        faultedErrorRate = 0.45..0.85,
    ),
    // Second dependency of auth-service, so the graph forks. No faulted error profile, and
    // redis-cache has no faulted local time either, so this branch stays green throughout.
    Dependency(
        source = "auth-service",
        target = "redis-cache",
        hopDelayMs = 0,
        healthyErrorRate = 0.0..0.004,
    ),
    Dependency(
        source = "payment-api",
        target = "auth-service",
        hopDelayMs = 500,
        healthyErrorRate = 0.0..0.01,
        faultedErrorRate = 0.40..0.70,
    ),
    Dependency(
        source = "frontend-gateway",
        target = "payment-api",
        hopDelayMs = 1_000,
        healthyErrorRate = 0.0..0.02,
        faultedErrorRate = 0.20..0.45,
    ),
    // Second caller of payment-api, so the graph also fans in.
    Dependency(
        source = "notification-worker",
        target = "payment-api",
        hopDelayMs = 1_000,
        healthyErrorRate = 0.0..0.02,
        faultedErrorRate = 0.18..0.42,
    ),
)

/** Derived from [CLUSTER] so the graph has a single source of truth. */
private val DEPENDENCIES_OF: Map<String, List<String>> =
    CLUSTER.groupBy({ it.source }, { it.target })

/**
 * Services in bottom-up topological order: every service appears after all of its dependencies.
 * A single pass in this order therefore lets a parent read the values its children were just
 * assigned, so a leaf spike reaches the top of the graph within one tick.
 */
private val EVALUATION_ORDER: List<Service> = buildEvaluationOrder()

private fun buildEvaluationOrder(): List<Service> {
    val byId = SERVICES.associateBy { it.id }
    val ordered = mutableListOf<Service>()
    val settled = mutableSetOf<String>()
    val onStack = mutableSetOf<String>()

    fun visit(id: String) {
        // The onStack guard breaks dependency cycles; the back edge is simply left unordered
        // and falls back to the previous tick's value in tickServiceTimes.
        if (id in settled || id in onStack) return
        onStack += id
        DEPENDENCIES_OF[id].orEmpty().forEach(::visit)
        onStack -= id
        if (settled.add(id)) byId[id]?.let(ordered::add)
    }

    SERVICES.forEach { visit(it.id) }
    return ordered
}

/** One service's contribution to the cascade, for the audit log. */
private class LatencyBreakdown(
    val serviceId: String,
    val internalMs: Double,
    val maxChildMs: Double,
    val totalMs: Double,
)

private fun ClosedFloatingPointRange<Double>.sample(): Double =
    Random.nextDouble(start, endInclusive)

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

    /**
     * Extends an in-flight window without moving its start. Re-calling [begin] each poll would
     * keep pushing startedAt forward, and `now >= startedAt + hopDelayMs` would then only ever
     * hold for hop 0 - the error cascade would stop dead at the first hop.
     */
    fun extendTo(end: Long) {
        if (end > endsAt) endsAt = end
    }

    /** A hop [hopDelayMs] above the fault both sees it later and recovers later. */
    fun isDegraded(now: Long, hopDelayMs: Long): Boolean =
        now >= startedAt + hopDelayMs && now < endsAt + hopDelayMs

    fun isActive(now: Long): Boolean = now in startedAt until endsAt
}

/**
 * How long each service currently takes to serve a request, inclusive of downstream wait.
 * This is the only coupling between hops, and what turns a leaf fault into a real cascade.
 */
private class ClusterState {
    private val serviceTimeMs = ConcurrentHashMap<String, Double>()

    fun snapshot(): Map<String, Double> = HashMap(serviceTimeMs)

    fun publishAll(values: Map<String, Double>) {
        serviceTimeMs.putAll(values)
    }

    fun serviceTimeOf(service: String): Double = serviceTimeMs[service] ?: 0.0
}

/**
 * Recomputes every service's total time: its own local execution plus the slowest of its
 * dependencies. A service with no dependencies is just its local time, which is where the
 * db-primary fault enters.
 *
 * Each pass reads the *previous* snapshot rather than values written during this pass, so a
 * change takes one sample interval to travel one hop instead of teleporting across the DAG.
 */
private fun tickServiceTimes(cluster: ClusterState, faulted: Boolean): List<LatencyBreakdown> {
    val previous = cluster.snapshot()
    val updated = HashMap<String, Double>(SERVICES.size)
    val breakdown = ArrayList<LatencyBreakdown>(SERVICES.size)

    for (service in EVALUATION_ORDER) {
        // Bottom-up: every dependency has already been assigned in this same pass, so a parent
        // sees its children's post-spike values immediately. `previous` only backstops an edge
        // that a dependency cycle forced buildEvaluationOrder to leave unordered.
        val maxChildMs = DEPENDENCIES_OF[service.id]
            .orEmpty()
            .maxOfOrNull { updated[it] ?: previous[it] ?: 0.0 }
            ?: 0.0

        val profile = service.faultedLocalMs?.takeIf { faulted } ?: service.localMs
        val internalMs = profile.sample()
        val totalMs = internalMs + maxChildMs

        updated[service.id] = totalMs
        breakdown += LatencyBreakdown(service.id, internalMs, maxChildMs, totalMs)
    }

    cluster.publishAll(updated)
    return breakdown
}

private fun printBreakdown(label: String, rows: List<LatencyBreakdown>) {
    println("  ${LocalTime.now().format(TIME_FORMAT)}  latency breakdown - $label")
    println("    %-20s %12s %14s %12s".format("service_id", "internal_ms", "max_child_ms", "total_ms"))
    for (row in rows) {
        println(
            "    %-20s %12.1f %14.1f %12.1f".format(
                row.serviceId, row.internalMs, row.maxChildMs, row.totalMs,
            )
        )
    }
}

private suspend fun driveServiceTimes(cluster: ClusterState, spikes: SpikeSchedule) {
    var wasFaulted = false
    var ticksLeftToLog = 0

    while (currentCoroutineContext().isActive) {
        val faulted = spikes.isActive(System.currentTimeMillis())
        val breakdown = tickServiceTimes(cluster, faulted)

        if (faulted && !wasFaulted) ticksLeftToLog = BREAKDOWN_TICKS
        if (!faulted && wasFaulted) printBreakdown("recovered", breakdown)
        if (ticksLeftToLog > 0) {
            printBreakdown("outage tick ${BREAKDOWN_TICKS - ticksLeftToLog + 1}", breakdown)
            ticksLeftToLog--
        }

        wasFaulted = faulted
        delay(SAMPLE_INTERVAL_MS)
    }
}

fun main(args: Array<String>) = runBlocking<Unit> {
    val target = args.firstOrNull() ?: DEFAULT_TARGET
    val channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build()
    val stub = TelemetryServiceGrpcKt.TelemetryServiceCoroutineStub(channel)
    val spikes = SpikeSchedule()
    val cluster = ClusterState()

    // One bottom-up pass seeds the whole graph, since each service is evaluated after its
    // dependencies - so the first metrics carry steady-state values rather than zeros.
    tickServiceTimes(cluster, faulted = false)

    println("streaming synthetic cluster telemetry to $target")
    CLUSTER.forEach { println("  ${it.source} -> ${it.target}") }
    println("sample every ${SAMPLE_INTERVAL_MS}ms, db-primary fault every ${SPIKE_INTERVAL_MS}ms")
    println()

    Runtime.getRuntime().addShutdownHook(Thread { channel.shutdownNow() })

    try {
        coroutineScope {
            launch { driveServiceTimes(cluster, spikes) }
            CLUSTER.forEach { dependency ->
                launch { streamFor(stub, dependency, cluster, spikes) }
            }
            launch { driveSpikes(spikes) }
            launch { followOperatorOutages(spikes) }
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
    cluster: ClusterState,
    spikes: SpikeSchedule,
) {
    while (currentCoroutineContext().isActive) {
        try {
            // The flow never completes, so the RPC stays open and the server keeps ingesting.
            stub.streamMetrics(
                flow {
                    while (currentCoroutineContext().isActive) {
                        emit(dependency.sample(cluster, spikes))
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

/**
 * Honours outages requested from the dashboard. The server only records the intent - this agent
 * is what actually degrades db-primary, so the resulting metrics arrive by the normal ingest path.
 */
private suspend fun followOperatorOutages(spikes: SpikeSchedule) {
    val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()
    val request = HttpRequest.newBuilder(URI.create(CONTROL_URL))
        .timeout(Duration.ofSeconds(2))
        .GET()
        .build()

    var announced = false
    while (currentCoroutineContext().isActive) {
        delay(CONTROL_POLL_MS)

        val state = try {
            withContext(Dispatchers.IO) {
                val response = http.send(request, HttpResponse.BodyHandlers.ofString())
                if (response.statusCode() == 200) {
                    JSON.decodeFromString<OutageStateDto>(response.body())
                } else {
                    null
                }
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            null
        } ?: continue

        val now = System.currentTimeMillis()
        if (state.active) {
            if (announced) {
                // Track whatever the server still has left, without moving the window's start.
                spikes.extendTo(now + state.remainingMs)
            } else {
                spikes.begin(now, state.remainingMs)
                announced = true
                println("  !! operator-triggered outage on db-primary (${state.remainingMs}ms)")
            }
        } else {
            announced = false
        }
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
