package com.telemetry.stream

import java.util.concurrent.atomic.AtomicLong

/**
 * Control-plane state for operator-triggered outages.
 *
 * The server only records the *intent*; it never fabricates telemetry. The load generator polls
 * this and injects the actual fault, which is how a real chaos agent works - the control plane
 * asks, the agent in the estate does the damage, and the resulting metrics arrive by the normal
 * ingest path.
 */
class OutageController {
    // Epoch 0, not Long.MIN_VALUE: `activeUntil - now` must not underflow into a
    // huge positive, which would report an outage that never lapses.
    private val activeUntil = AtomicLong(0)

    /** Starts (or extends) an outage, returning the epoch millis at which it lapses. */
    fun request(durationMs: Long): Long {
        val until = System.currentTimeMillis() + durationMs.coerceIn(MIN_DURATION_MS, MAX_DURATION_MS)
        activeUntil.set(until)
        return until
    }

    fun clear() {
        activeUntil.set(0)
    }

    fun remainingMs(now: Long = System.currentTimeMillis()): Long =
        (activeUntil.get() - now).coerceAtLeast(0)

    fun isActive(now: Long = System.currentTimeMillis()): Boolean = remainingMs(now) > 0

    fun state(): OutageStateDto = OutageStateDto(active = isActive(), remainingMs = remainingMs())

    companion object {
        const val DEFAULT_DURATION_MS = 12_000L
        private const val MIN_DURATION_MS = 1_000L

        /** Bounded so a stray request cannot wedge the estate red indefinitely. */
        private const val MAX_DURATION_MS = 120_000L
    }
}
