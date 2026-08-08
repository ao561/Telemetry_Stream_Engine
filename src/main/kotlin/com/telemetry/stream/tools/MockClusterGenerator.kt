package com.telemetry.stream.tools

import com.google.protobuf.Empty
import com.telemetry.stream.ControlStateDto
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

/** Where the dashboard records operator intent: outages, and whether breakers are armed. */
private val CONTROL_URL = System.getenv("CONTROL_URL") ?: "http://localhost:8080/api/controls"
private const val CONTROL_POLL_MS = 500L

/** How many ticks of the cascade breakdown to print when an outage starts. */
private const val BREAKDOWN_TICKS = 3

/*
 * Circuit breaker. A caller trips the breaker on a dependency after the calls it makes to that
 * dependency have been over threshold for BREAKER_TRIP_AFTER consecutive ticks.
 *
 * It deliberately does *not* key off the child's own status: the server judges a service by its
 * outbound calls, so a leaf like db-primary is never CRITICAL and a breaker waiting on that would
 * never trip. Real breakers watch the calls they make, which is what this does.
 */
private const val BREAKER_TRIP_AFTER = 3
private const val BREAKER_LATENCY_MS = 300.0
private const val BREAKER_RESET_MS = 5_000L
private val BREAKER_FALLBACK_MS = 1.0..4.0
private val BREAKER_FALLBACK_ERROR_RATE = 0.0..0.002

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
    val localErrorRate: ClosedFloatingPointRange<Double>,
    val faultedLocalMs: ClosedFloatingPointRange<Double>? = null,
    val faultedErrorRate: ClosedFloatingPointRange<Double>? = null,
    /** Share of a dependency's failures this service swallows via retries and fallbacks. */
    val errorAbsorption: Double = 0.0,
)

/** One directed dependency. Latency is derived; only the error rate is scripted per hop. */
private class Dependency(
    val source: String,
    val target: String,
) {
    fun sample(cluster: ClusterState, board: CircuitBoard): ServiceMetric {
        val shortCircuited = board.isOpen(source, target)

        return serviceMetric {
            serviceId = source
            targetService = target
            timestamp = System.currentTimeMillis()
            circuitOpen = shortCircuited
            // An open breaker means the caller served a fallback: fast, and *successful*. Both
            // signals have to reflect that, otherwise upstream keeps seeing failures from a call
            // that never actually happened.
            latencyMs = if (shortCircuited) {
                BREAKER_FALLBACK_MS.sample()
            } else {
                cluster.serviceTimeOf(target) + NETWORK_JITTER_MS.sample()
            }
            errorRate = if (shortCircuited) {
                BREAKER_FALLBACK_ERROR_RATE.sample()
            } else {
                cluster.failureRateOf(target)
            }
        }
    }
}

/*
 * frontend-gateway ----\
 *                       >--> payment-api --> auth-service --+--> db-primary   <- fault injected here
 * notification-worker -/                                    \--> redis-cache  <- healthy sibling
 */
private val SERVICES = listOf(
    Service(
        id = "db-primary",
        localMs = 4.0..14.0,
        localErrorRate = 0.0..0.004,
        faultedLocalMs = 400.0..900.0,
        faultedErrorRate = 0.45..0.85,
    ),
    Service(id = "redis-cache", localMs = 0.8..4.0, localErrorRate = 0.0..0.004),
    Service(
        id = "auth-service",
        localMs = 3.0..10.0,
        localErrorRate = 0.0..0.003,
        errorAbsorption = 0.30,
    ),
    Service(
        id = "payment-api",
        localMs = 8.0..22.0,
        localErrorRate = 0.0..0.004,
        errorAbsorption = 0.35,
    ),
    Service(
        id = "frontend-gateway",
        localMs = 12.0..35.0,
        localErrorRate = 0.0..0.006,
        errorAbsorption = 0.35,
    ),
    Service(
        id = "notification-worker",
        localMs = 15.0..40.0,
        localErrorRate = 0.0..0.006,
        errorAbsorption = 0.35,
    ),
)

// Ordered outward from the fault at db-primary.
private val CLUSTER = listOf(
    Dependency(source = "auth-service", target = "db-primary"),
    // Second dependency of auth-service, so the graph forks. redis-cache carries no faulted
    // profile, so this branch stays green throughout a db-primary outage.
    Dependency(source = "auth-service", target = "redis-cache"),
    Dependency(source = "payment-api", target = "auth-service"),
    Dependency(source = "frontend-gateway", target = "payment-api"),
    // Second caller of payment-api, so the graph also fans in.
    Dependency(source = "notification-worker", target = "payment-api"),
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
    val failureRate: Double,
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

    fun isActive(now: Long): Boolean = now in startedAt until endsAt
}

private enum class CircuitState { CLOSED, OPEN, HALF_OPEN }

/**
 * One caller's breaker on one dependency.
 *
 * While OPEN the caller stops observing the dependency entirely, so it cannot notice a recovery.
 * That is what HALF_OPEN is for: after [BREAKER_RESET_MS] one real call is let through, and its
 * result either closes the breaker or re-opens it.
 */
private class CircuitBreaker {
    @Volatile
    var state: CircuitState = CircuitState.CLOSED
        private set

    private var consecutiveBad = 0
    private var openedAt = 0L

    /** True when this tick's call should be short-circuited rather than really made. */
    @Synchronized
    fun shouldShortCircuit(now: Long): Boolean {
        if (state == CircuitState.OPEN && now - openedAt >= BREAKER_RESET_MS) {
            state = CircuitState.HALF_OPEN
        }
        return state == CircuitState.OPEN
    }

    /** Feeds back what a real call looked like. Only called when the call actually happened. */
    @Synchronized
    fun record(now: Long, latencyMs: Double) {
        val bad = latencyMs > BREAKER_LATENCY_MS
        when {
            // A failed probe re-opens immediately; no need to count to three again.
            bad && state == CircuitState.HALF_OPEN -> trip(now)
            bad -> {
                consecutiveBad++
                if (consecutiveBad >= BREAKER_TRIP_AFTER) trip(now)
            }
            state == CircuitState.HALF_OPEN -> {
                state = CircuitState.CLOSED
                consecutiveBad = 0
            }
            else -> consecutiveBad = 0
        }
    }

    private fun trip(now: Long) {
        state = CircuitState.OPEN
        openedAt = now
        consecutiveBad = 0
    }

    @Synchronized
    fun reset() {
        state = CircuitState.CLOSED
        consecutiveBad = 0
        openedAt = 0
    }
}

/** Every caller-to-dependency breaker in the estate. */
private class CircuitBoard {
    private val breakers = ConcurrentHashMap<String, CircuitBreaker>()

    /** Toggled from the dashboard. Disarmed breakers never short-circuit. */
    @Volatile
    var enabled: Boolean = true
        private set

    fun setEnabled(value: Boolean) {
        if (value == enabled) return
        enabled = value
        // Start from a clean slate either way, so re-arming does not resume a stale OPEN.
        breakers.values.forEach { it.reset() }
    }

    fun of(source: String, target: String): CircuitBreaker =
        breakers.computeIfAbsent("$source->$target") { CircuitBreaker() }

    /**
     * True whenever the caller is shielding upstream - OPEN, and also HALF_OPEN, because a probe
     * is one trial request rather than the whole tick's traffic. Reporting a failed probe as if
     * every call failed would leak the fault upstream once per reset window.
     */
    fun isOpen(source: String, target: String): Boolean =
        enabled && of(source, target).state != CircuitState.CLOSED

    fun openEdges(): List<String> =
        if (!enabled) emptyList()
        else breakers.entries
            .filter { it.value.state != CircuitState.CLOSED }
            .map { it.key }
            .sorted()
}

/**
 * How long each service currently takes to serve a request, inclusive of downstream wait.
 * This is the only coupling between hops, and what turns a leaf fault into a real cascade.
 */
private class ClusterState {
    /** Total time a caller waits for, inclusive of the service's own downstream calls. */
    private val serviceTimeMs = ConcurrentHashMap<String, Double>()

    /** The service's own execution time, exclusive of anything it waits on. */
    private val internalMs = ConcurrentHashMap<String, Double>()

    /** How often a call to this service fails, inclusive of what it inherits downstream. */
    private val failureRate = ConcurrentHashMap<String, Double>()

    fun snapshot(): Map<String, Double> = HashMap(serviceTimeMs)

    fun internalSnapshot(): Map<String, Double> = HashMap(internalMs)

    fun failureSnapshot(): Map<String, Double> = HashMap(failureRate)

    fun publishAll(
        totals: Map<String, Double>,
        internals: Map<String, Double>,
        failures: Map<String, Double>,
    ) {
        serviceTimeMs.putAll(totals)
        internalMs.putAll(internals)
        failureRate.putAll(failures)
    }

    fun serviceTimeOf(service: String): Double = serviceTimeMs[service] ?: 0.0

    fun failureRateOf(service: String): Double = failureRate[service] ?: 0.0
}

/**
 * Recomputes every service's total time: its own local execution plus the slowest of its
 * dependencies. A service with no dependencies is just its local time, which is where the
 * db-primary fault enters.
 *
 * Each pass reads the *previous* snapshot rather than values written during this pass, so a
 * change takes one sample interval to travel one hop instead of teleporting across the DAG.
 */
private fun tickServiceTimes(
    cluster: ClusterState,
    board: CircuitBoard,
    faulted: Boolean,
): List<LatencyBreakdown> {
    val now = System.currentTimeMillis()
    val previousTotal = cluster.snapshot()
    val previousInternal = cluster.internalSnapshot()
    val previousFailure = cluster.failureSnapshot()
    val updatedTotal = HashMap<String, Double>(SERVICES.size)
    val updatedInternal = HashMap<String, Double>(SERVICES.size)
    val updatedFailure = HashMap<String, Double>(SERVICES.size)
    val breakdown = ArrayList<LatencyBreakdown>(SERVICES.size)

    for (service in EVALUATION_ORDER) {
        // Sampled before the children are walked, so a breaker can read this service's own
        // execution time without it being contaminated by what it is waiting on.
        val profile = service.faultedLocalMs?.takeIf { faulted } ?: service.localMs
        val internalMs = profile.sample()
        updatedInternal[service.id] = internalMs

        val errorProfile = service.faultedErrorRate?.takeIf { faulted } ?: service.localErrorRate
        val internalErrorRate = errorProfile.sample()

        // Bottom-up: every dependency has already been assigned in this same pass, so a parent
        // sees its children's post-spike values immediately. `previous*` only backstops an edge
        // that a dependency cycle forced buildEvaluationOrder to leave unordered.
        var maxChildMs = 0.0
        var worstChildErrorRate = 0.0
        for (target in DEPENDENCIES_OF[service.id].orEmpty()) {
            val childTotalMs = updatedTotal[target] ?: previousTotal[target] ?: 0.0
            val childFailureRate = updatedFailure[target] ?: previousFailure[target] ?: 0.0
            if (!board.enabled) {
                // Disarmed: pay the child's cost in full, which is what lets the cascade build.
                if (childTotalMs > maxChildMs) maxChildMs = childTotalMs
                if (childFailureRate > worstChildErrorRate) worstChildErrorRate = childFailureRate
                continue
            }

            // A breaker judges its *direct* dependency, so it watches that service's own
            // execution time - not its inclusive total. Judging the total would make every
            // caller up the chain observe the same leaf fault and trip together, when only the
            // caller of the broken service should.
            val childInternalMs = updatedInternal[target] ?: previousInternal[target] ?: 0.0

            val breaker = board.of(service.id, target)
            val shortCircuited = breaker.shouldShortCircuit(now)
            // shouldShortCircuit may have just moved OPEN -> HALF_OPEN, so read the state after it.
            val probing = !shortCircuited && breaker.state == CircuitState.HALF_OPEN

            // Short-circuited: the caller never waits on the child and never sees it fail - the
            // fallback answers, quickly and successfully. Both signals must reflect that, or
            // upstream keeps inheriting failures from a call that never happened.
            val contributionMs: Double
            val contributionErrorRate: Double
            if (shortCircuited) {
                contributionMs = BREAKER_FALLBACK_MS.sample()
                contributionErrorRate = BREAKER_FALLBACK_ERROR_RATE.sample()
            } else {
                breaker.record(now, childInternalMs)
                if (probing) {
                    // The probe tells the breaker whether the dependency is back, but the caller
                    // still answers its own callers from the fallback while it finds out.
                    contributionMs = BREAKER_FALLBACK_MS.sample()
                    contributionErrorRate = BREAKER_FALLBACK_ERROR_RATE.sample()
                } else {
                    contributionMs = childTotalMs
                    contributionErrorRate = childFailureRate
                }
            }
            if (contributionMs > maxChildMs) maxChildMs = contributionMs
            if (contributionErrorRate > worstChildErrorRate) worstChildErrorRate = contributionErrorRate
        }

        val totalMs = internalMs + maxChildMs
        // Failures attenuate on the way up: each hop retries or falls back over a share of them.
        val failureRate =
            (internalErrorRate + worstChildErrorRate * (1 - service.errorAbsorption))
                .coerceIn(0.0, 1.0)

        updatedTotal[service.id] = totalMs
        updatedFailure[service.id] = failureRate
        breakdown += LatencyBreakdown(service.id, internalMs, maxChildMs, totalMs, failureRate)
    }

    cluster.publishAll(updatedTotal, updatedInternal, updatedFailure)
    return breakdown
}

private fun printBreakdown(label: String, rows: List<LatencyBreakdown>) {
    println("  ${LocalTime.now().format(TIME_FORMAT)}  latency breakdown - $label")
    println(
        "    %-20s %12s %14s %12s %10s".format(
            "service_id", "internal_ms", "max_child_ms", "total_ms", "errors",
        )
    )
    for (row in rows) {
        println(
            "    %-20s %12.1f %14.1f %12.1f %9.2f%%".format(
                row.serviceId, row.internalMs, row.maxChildMs, row.totalMs, row.failureRate * 100,
            )
        )
    }
}

private suspend fun driveServiceTimes(
    cluster: ClusterState,
    board: CircuitBoard,
    spikes: SpikeSchedule,
) {
    var wasFaulted = false
    var lastOpen = emptyList<String>()
    var ticksLeftToLog = 0

    while (currentCoroutineContext().isActive) {
        val faulted = spikes.isActive(System.currentTimeMillis())
        val breakdown = tickServiceTimes(cluster, board, faulted)

        val open = board.openEdges()
        if (open != lastOpen) {
            val opened = open - lastOpen.toSet()
            val closed = lastOpen - open.toSet()
            opened.forEach { println("  ~~ circuit OPEN  $it (short-circuiting to fallback)") }
            closed.forEach { println("  ~~ circuit CLOSED $it (dependency recovered)") }
            lastOpen = open
        }

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
    val board = CircuitBoard()

    // One bottom-up pass seeds the whole graph, since each service is evaluated after its
    // dependencies - so the first metrics carry steady-state values rather than zeros.
    tickServiceTimes(cluster, board, faulted = false)

    println("streaming synthetic cluster telemetry to $target")
    CLUSTER.forEach { println("  ${it.source} -> ${it.target}") }
    println("sample every ${SAMPLE_INTERVAL_MS}ms, db-primary fault every ${SPIKE_INTERVAL_MS}ms")
    println()

    Runtime.getRuntime().addShutdownHook(Thread { channel.shutdownNow() })

    try {
        coroutineScope {
            launch { driveServiceTimes(cluster, board, spikes) }
            CLUSTER.forEach { dependency ->
                launch { streamFor(stub, dependency, cluster, board) }
            }
            launch { driveSpikes(spikes) }
            launch { followOperatorControls(board, spikes) }
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
    board: CircuitBoard,
) {
    while (currentCoroutineContext().isActive) {
        try {
            // The flow never completes, so the RPC stays open and the server keeps ingesting.
            stub.streamMetrics(
                flow {
                    while (currentCoroutineContext().isActive) {
                        emit(dependency.sample(cluster, board))
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
private suspend fun followOperatorControls(board: CircuitBoard, spikes: SpikeSchedule) {
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
                    JSON.decodeFromString<ControlStateDto>(response.body())
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

        if (state.circuitBreakersEnabled != board.enabled) {
            board.setEnabled(state.circuitBreakersEnabled)
            println("  ~~ circuit breakers ${if (state.circuitBreakersEnabled) "ARMED" else "DISARMED"}")
        }

        if (state.outageActive) {
            if (announced) {
                // Track whatever the server still has left, without moving the window's start.
                spikes.extendTo(now + state.outageRemainingMs)
            } else {
                spikes.begin(now, state.outageRemainingMs)
                announced = true
                println("  !! operator-triggered outage on db-primary (${state.outageRemainingMs}ms)")
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
