package com.telemetry.stream

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

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
    private val breakersEnabled = AtomicBoolean(true)

    /** Starts (or extends) an outage, returning the epoch millis at which it lapses. */
    fun requestOutage(durationMs: Long): Long {
        val until = System.currentTimeMillis() + durationMs.coerceIn(MIN_DURATION_MS, MAX_DURATION_MS)
        outageUntil.set(until)
        return until
    }

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
        circuitBreakersEnabled = circuitBreakersEnabled(),
    )

    companion object {
        const val DEFAULT_OUTAGE_MS = 12_000L
        private const val MIN_DURATION_MS = 1_000L

        /** Bounded so a stray request cannot wedge the estate red indefinitely. */
        private const val MAX_DURATION_MS = 120_000L
    }
}
