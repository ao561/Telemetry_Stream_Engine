package com.telemetry.stream

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * Operator control state: what the dashboard has asked for.
 *
 * The server only records intent; it never fabricates telemetry. The load generator polls this
 * and does the actual work - injecting the fault, and honouring whether breakers are armed - so
 * the resulting metrics still arrive by the normal ingest path.
 */
class ControlPlane {
    // Epoch 0, not Long.MIN_VALUE: `activeUntil - now` must not underflow into a
    // huge positive, which would report an outage that never lapses.
    private val outageUntil = AtomicLong(0)
    // Off by default: the cascade is the interesting thing to see first, and breakers
    // suppress it almost entirely. Arm them from the dashboard to watch them work.
    private val breakersEnabled = AtomicBoolean(false)
    private val outageService = AtomicReference(DEFAULT_OUTAGE_SERVICE)

    /** Starts (or extends) an outage on [service], returning the epoch millis at which it lapses. */
    fun requestOutage(service: String, durationMs: Long): Long {
        val until = System.currentTimeMillis() + durationMs.coerceIn(MIN_DURATION_MS, MAX_DURATION_MS)
        outageService.set(service.ifBlank { DEFAULT_OUTAGE_SERVICE })
        outageUntil.set(until)
        return until
    }

    fun outageService(): String = outageService.get()

    fun clearOutage() {
        outageUntil.set(0)
    }

    fun outageRemainingMs(now: Long = System.currentTimeMillis()): Long =
        (outageUntil.get() - now).coerceAtLeast(0)

    fun isOutageActive(now: Long = System.currentTimeMillis()): Boolean = outageRemainingMs(now) > 0

    fun setCircuitBreakersEnabled(enabled: Boolean) {
        breakersEnabled.set(enabled)
    }

    fun circuitBreakersEnabled(): Boolean = breakersEnabled.get()

    fun state(): ControlStateDto = ControlStateDto(
        outageActive = isOutageActive(),
        outageRemainingMs = outageRemainingMs(),
        outageService = outageService(),
        circuitBreakersEnabled = circuitBreakersEnabled(),
    )

    companion object {
        const val DEFAULT_OUTAGE_MS = 12_000L
        const val DEFAULT_OUTAGE_SERVICE = "db-primary"
        private const val MIN_DURATION_MS = 1_000L

        /** Bounded so a stray request cannot wedge the estate red indefinitely. */
        private const val MAX_DURATION_MS = 120_000L
    }
}
