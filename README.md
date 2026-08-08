# Telemetry Stream Engine

Distributed metric aggregator & visualiser. Kotlin/JVM, gRPC for ingest and fan-out,
Ktor for the HTTP/WebSocket front end.

## Quick start

Needs **JDK 21+** and **Node 20.19+ / 22.12+** (Vite 8). Gradle comes via the wrapper.

Three processes, in this order — the generator needs the server, the dashboard needs both:

```sh
# terminal 1 — server: gRPC on :50051, HTTP/WebSocket on :8080
./gradlew run

# terminal 2 — the simulated microservice estate
SPIKE_INTERVAL_MS=3600000 ./gradlew mockCluster

# terminal 3 — dashboard on http://localhost:5173
cd frontend && npm install && npm run dev
```

Wait for terminal 1 to print `gRPC listening on 50051` before starting terminal 2, otherwise it
retries every 2 s until the server appears.

Open <http://localhost:5173> and you should see six services wired into a dependency graph, all
green, with latency figures on every edge. Then: pick a service in the header dropdown, set a
duration, and hit **Simulate Outage** to watch the failure cascade upstream.

`SPIKE_INTERVAL_MS=3600000` parks the generator's own fault timer (default: a fault every 15 s) so
that anything you see going red is attributable to your click. Drop it for background chaos.

## How it works

Three processes. Telemetry flows one way; operator control flows back the other.

```
  MockClusterGenerator  ──gRPC StreamMetrics──▶  TelemetryServiceImpl  ──WebSocket──▶  dashboard
   one stream per                                 50-sample window per     1 Hz         React Flow
   dependency edge,                               service -> status,                    canvas
   a sample per 500 ms                            edges, blast radius
          ▲                                                 ▲
          └──── GET /api/controls, every 500 ms ──── ControlPlane ◀──── POST from the dashboard
                (outage window, breakers armed?)     (records intent only)
```

**Ingest.** The generator runs 9 coroutines in one JVM: a long-lived `StreamMetrics` RPC for each
of the 5 dependency edges, plus the service-time ticker, the fault scheduler, the control poller
and a console reporter. Each stream emits one `ServiceMetric` per 500 ms describing a call to one
dependency, so a service with two dependencies emits two samples per tick — which is why its
rolling window covers less wall-clock time than a single-dependency service's.

Nothing is ever sent *between* the simulated services. The graph is reconstructed server-side by
joining `service_id → target_service` pairs, which is how real agent-based telemetry works: each
agent reports only its own outbound calls.

**Aggregation.** The server retains the last 50 metrics per service and derives everything from
them: rolling latency and error rate, the HEALTHY/WARNING/CRITICAL status, the edge set, and blast
radius. It never fabricates telemetry.

**Fan-out.** One ticker serialises the graph per second and publishes it to every connected socket,
so N browsers cost one serialisation per tick rather than N.

**Control runs backwards.** The *Simulate Outage* button, its service dropdown, and the
*Circuit Breakers* switch POST to the server,
which records the request in `ControlPlane` and does nothing else. The generator polls
`/api/controls` and performs the actual work — injecting the fault, arming or disarming breakers —
so the consequences still arrive through the normal ingest path rather than being painted on.

### Two clocks, and most of the surprises

Nearly every non-obvious behaviour here comes from one gap:

| | Period |
| --- | --- |
| generator tick, breaker decisions, control poll | **500 ms** |
| server's 50-sample rolling window | **25 s** |

The simulated physics moves fifty times faster than the measurement of it. A breaker trips ~1.5 s
into a fault while the status that would have revealed that fault needs ~25 s to move — so with
breakers armed the dashboard can show a perfectly healthy cluster *during* a live outage. That is
why the incident banner treats an open circuit as an incident in its own right instead of trusting
node colour, and why the fault-cadence table below only makes sense with breakers disarmed.

## Stack

| Component | Version |
| --- | --- |
| Kotlin (JVM toolchain 21) | 2.4.10 |
| Gradle | 9.7.0 (wrapper) |
| `com.google.protobuf` plugin | 0.10.0 |
| protobuf | 4.35.1 |
| grpc-java | 1.83.1 |
| grpc-kotlin | 1.5.0 |
| kotlinx-coroutines | 1.11.0 |
| Ktor | 3.5.2 |

## Layout

```
src/main/proto/                        .proto sources - compiled automatically, no per-file wiring
src/main/kotlin/com/telemetry/stream/  gRPC service, Ktor app, control plane
                                .../tools/  MockClusterGenerator - the simulated estate
src/test/kotlin/                       in-process gRPC tests
frontend/src/                          React + React Flow dashboard
```

## Protobuf codegen

Anything dropped into `src/main/proto` is compiled on the next build. Each `.proto`
produces four outputs under `build/generated/sources/proto/main/`:

| Dir | Generator | Contents |
| --- | --- | --- |
| `java/` | protoc builtin | message classes |
| `kotlin/` | protoc builtin `kotlin` | Kotlin DSL builders (`serviceMetric { ... }`) |
| `grpc/` | `protoc-gen-grpc-java` | service base classes |
| `grpckt/` | `protoc-gen-grpc-kotlin` | coroutine/`Flow` stubs |

The plugin registers these directories with the Kotlin and Java compile tasks itself,
so generated types are importable with no extra `sourceSets` configuration.

Regenerate explicitly with:

```sh
./gradlew generateProto
```

## Build and run

See [Quick start](#quick-start) to get the whole thing running. Individual tasks:

```sh
./gradlew build        # compile + test
./gradlew test         # tests only
./gradlew run          # server only: gRPC :50051 + HTTP/WebSocket :8080
./gradlew mockCluster  # generator only, against an already-running server
```

- gRPC on `:50051` (server reflection enabled, so `grpcurl` works without `-proto`)
- HTTP/WebSocket on `:8080`
- dashboard on `:5173` via `cd frontend && npm run dev`

```sh
curl localhost:8080/health         # {"status":"ok"}
curl localhost:8080/api/topology   # {"nodes":[...],"edges":[...]}
# ws://localhost:8080/ws/metrics   - pushes the whole graph once per second
```

## API

`TelemetryService` (see [telemetry.proto](src/main/proto/telemetry.proto)):

- `StreamMetrics(stream ServiceMetric) -> HealthStatus` — agents push observed calls over a
  long-lived client stream; the reply carries the health of the reporting service
- `GetGraphTopology(google.protobuf.Empty) -> GraphTopologyResponse` — the dependency graph as
  service nodes plus directed edges, for the visualiser

Each `ServiceMetric` is one observed call from `service_id` to `target_service`, so the edge set
is derived directly from the ingest stream.

### HTTP surface

| Route | Purpose |
| --- | --- |
| `GET /health` | liveness |
| `GET /api/topology` | current graph, same shape as the WebSocket frame |
| `GET /api/controls` | operator state; the generator polls this |
| `POST /api/outage?service=&durationMs=` | break one service; duration clamped to 1-120 s |
| `DELETE /api/outage` | clear it early |
| `POST /api/circuit-breakers?enabled=` | arm or disarm breakers |
| `ws://…/ws/metrics` | pushes the whole graph once per second |

Control state lives in [ControlPlane.kt](src/main/kotlin/com/telemetry/stream/ControlPlane.kt).
The server records intent only - the generator polls `/api/controls` and does the actual work, so
resulting metrics still arrive by the normal ingest path. CORS is scoped to the Vite dev origins.

## Visualiser

[frontend/src/App.tsx](frontend/src/App.tsx) renders the dependency graph with
[@xyflow/react](https://reactflow.dev), fed by the WebSocket.

```sh
cd frontend && npm install && npm run dev   # http://localhost:5173
```

It connects to `ws://localhost:8080/ws/metrics` and rebuilds the graph on every snapshot,
reconnecting every 2 s if the server goes away. Nodes are laid out left-to-right by dependency
depth; positions are only assigned to nodes seen for the first time, so the once-per-second
snapshot never undoes a drag.

Spacing is `COLUMN_WIDTH = 380` px and `ROW_HEIGHT = 200` px, both set by the label badges rather
than the cards. A column has to clear the widest card (~215 px for `notification-worker`) and still
leave the badge clear of the source handle; the row gap keeps siblings such as
`frontend-gateway` / `notification-worker` out of each other's badge band.

Edges use a custom `LatencyEdge` (smoothstep geometry) that draws its badge as HTML through
`EdgeLabelRenderer`, positioned a measured fraction along the route instead of React Flow's 50%
midpoint. Edges leaving the same node are staggered by `LABEL_POSITION_STEP`, because a fan-out's
routes have not diverged near their shared source - the two `auth-service` badges land 3 px apart
at a flat 0.25.

Measured against the live layout (badge 60x18 px):

| Config | Badge collisions | Min clearance to source handle |
| --- | --- | --- |
| 260 px cols, 50% label | 0 | **-3 px (overlap)** |
| 380 px cols, 50% label | 0 | 54 px |
| 380 px cols, flat 0.25 | **1** | 9 px |
| 380 px cols, staggered 0.35 / 0.50 | 0 | 27 px |

| Status | Card |
| --- | --- |
| `HEALTHY` | soft green |
| `WARNING` | amber |
| `CRITICAL` | red, flashing (falls back to a static red under `prefers-reduced-motion`) |

### Incident log

A collapsible terminal-style panel docks below the graph. The stage is a flex column rather than
an overlay, so the log never covers React Flow's controls or minimap, and it insets when the
detail drawer opens.

Entries are derived by **diffing consecutive snapshots**, not by sampling the stream - a
once-per-second feed would otherwise produce a once-per-second log. Replayed over 79 s of captured
frames spanning an outage (breakers disarmed, so the full cascade develops), the derivation
emitted **16 lines where a per-frame logger would have emitted 474**:

```
[12:38:10] WARN:  auth-service degraded — 6.0% error rate, 52ms avg latency
[12:38:10] ALERT: db-primary latency spiked to 99ms — 2 upstream services impacted
[12:38:13] ALERT: auth-service critical — 15.5% error rate
[12:38:14] ALERT: frontend-gateway critical — 201ms avg latency via cascading dependency
[12:39:16] WARN:  auth-service degraded — 14.4% error rate, 147ms avg latency
```

Events cover status transitions, services joining or leaving the graph, root-cause identification,
return to normal, outage requests, and stream connect/loss. Messages name whichever signal actually
tripped the threshold - latency or error rate - rather than assuming.

Auto-scroll only follows the tail while the reader is already at the bottom, so scrolling back
through an incident is not yanked away by the next event.

**Root-cause seeding.** `analyseRootCause` walks the frontier from *every* degraded service, not
just the CRITICAL ones. Seeding from CRITICAL alone is wrong during an uneven recovery: a mid-chain
service can sit at WARNING while its callers are still CRITICAL, which made a caller look like the
frontier and get blamed instead of the leaf below it. On the captured stream that bug named
`auth-service` as root cause during recovery; with the fix `db-primary` is the only service ever
named.

### Node inspector sparkline

Clicking a node opens a drawer with a live latency chart over a rolling 30-second buffer.

The chart is **hand-rolled SVG rather than a chart library**. recharts is 7.1 MB unpacked across 11
transitive dependencies (react-redux, immer, victory-vendor/d3) - disproportionate next to a
119 kB bundle for one sparkline. The inline version costs ~2.3 kB gzip and gives direct control
over the glow. Swap it for recharts if you later want axes, tooltips or legends.

- **Buffer.** History is kept for *every* service, not just the selected one, so the chart is
  already populated when the drawer opens. Samples older than 30 s are pruned on each frame, and
  the map is rebuilt from the incoming snapshot so a departed service drops out.
- **Leaves hold nothing.** A leaf reports no outbound calls, so it contributes no samples and the
  chart section is hidden rather than drawn flat at zero.
- **Curve.** Cubic through each point with control points at the segment midpoint - smooth without
  the overshoot a Catmull-Rom spline gives on a latency spike.
- **Colour.** The line, its glow and the area fill all inherit `currentColor` from a status class,
  so green/amber/red follows the node's current state. A dashed line marks the server's 200 ms
  CRITICAL threshold, giving the colour change something to read against.

Replayed against captured frames spanning an outage: buffer capped at 31 points / 30 s exactly,
leaves held 0, and every plotted point stayed inside the viewbox while `auth-service` latency ran
6 ms → 361 ms → 101 ms and its status walked HEALTHY → WARNING → CRITICAL.

### Circuit breakers

**Off by default** - the cascade is the more interesting thing to see first, and breakers suppress
it almost entirely. Arm them from the **Circuit Breakers** switch in the header. The server records
the intent at `POST /api/circuit-breakers?enabled=`, the generator picks it up from its
`/api/controls` poll, and disarming resets every breaker so re-arming never resumes a stale OPEN.

Each caller holds a breaker per dependency, in the generator. It trips on the direct dependency's
**own internal execution time** - `BREAKER_TRIP_AFTER` consecutive ticks over
`BREAKER_LATENCY_MS` (300 ms) - not on the child's reported status, and not on the child's
inclusive total.

Both of those distinctions matter:

- Keying off the child's *status* would never fire. The server judges a service by its outbound
  calls, so a leaf like `db-primary` is never CRITICAL.
- Keying off the child's *total* makes every caller up the chain trip together. A total is
  inclusive of downstream wait, so `payment-api` would observe `auth-service` at
  `local + 650 ms` and trip on a fault two hops below it. Watching internal time keeps each
  breaker judging only what its direct dependency actually controls.

Measured across one `db-primary` outage, breakers armed:

| Edge | Peak latency | Breaker |
| --- | --- | --- |
| `auth-service -> db-primary` | 50 ms (then 2 ms on fallback) | **OPEN** |
| `payment-api -> auth-service` | 88 ms | closed |
| `frontend-gateway -> payment-api` | 103 ms | closed |
| `notification-worker -> payment-api` | 103 ms | closed |
| `auth-service -> redis-cache` | ~4 ms | closed |

One circuit opens, not four. Upstream sees a brief rise while the pre-trip samples age out of the
25 s window, stays well under both the 300 ms trip line and the 200 ms CRITICAL line, and keeps its
own breakers closed.

**A HALF_OPEN probe shields upstream too.** The probe informs the breaker, but the caller still
answers its own callers from the fallback while it finds out. Letting the probe through as ordinary
traffic reports the dependency's full failure rate for that tick, and with a probe every 5 s inside
a 25 s window that alone sustained ~6% upstream error - enough to hold `payment-api` at WARNING for
the whole outage.

Measured over a 55 s outage with breakers armed, once the pre-trip samples have aged out:

| Edge | Error rate |
| --- | --- |
| `auth-service -> db-primary` | 0.1% (fallback) |
| `payment-api -> auth-service` | 0.3% |
| `frontend-gateway -> payment-api` | 0.4% |

Worst upstream error rate 0.42% against a 5% WARNING threshold, and **every service stayed HEALTHY
for the entire outage**. Keying off the child would never work: the server judges a service by its
outbound calls, so a leaf like `db-primary` is never CRITICAL and a breaker waiting on that would
never fire.

```
CLOSED  --3 consecutive slow calls-->  OPEN  --after 5s-->  HALF_OPEN  --good probe-->  CLOSED
                                        ^                                  |
                                        +-------------- bad probe ---------+
```

HALF_OPEN is not optional. While OPEN the caller stops observing the dependency entirely, so it can
never notice a recovery; one real probe call after the reset window is the only way back.

While OPEN, `tickServiceTimes` substitutes a small fallback cost for that child instead of its real
service time, so upstream latency genuinely recovers. Measured across an outage: `auth-service`'s
`max_child_ms` fell from **552 ms to 3.7 ms** while `db-primary` was still broken.

`circuit_open` rides on `ServiceMetric` and is aggregated onto `DependencyEdge` from the newest
retained sample - not an average, since a breaker that trips mid-window must not read as closed for
another 25 s. The dashboard draws open circuits as **amber broken edges** with an `OPEN` badge, tags
the dependency in the drawer, and logs each transition.

The switch makes the contrast directly demonstrable. Same outage, measured on `auth-service ->
db-primary`:

| Breakers | Edge latency | Open circuits | Statuses |
| --- | --- | --- | --- |
| disarmed | 533 ms | 0 | all four upstream services CRITICAL |
| armed | 50 ms, then 2 ms on fallback | 1 | everything stays HEALTHY |

**The breaker outruns the dashboard.** It trips in ~1.5 s while the server's 50-sample window needs
~25 s to move a status. In a measured run no service ever left HEALTHY during a `db-primary`
outage - the fallback path is fast and clean, so the rolling averages never crossed a threshold.
Statuses alone would have shown a perfectly healthy cluster. That is why `analyseRootCause` treats
an open circuit as an incident in its own right:

```
f 0  OK       All systems operational
f 9  WARNING  root cause: db-primary — 4 circuits open — traffic on fallback
f24  OK       All systems operational
```

### Incident banner

A banner sits at the top of the canvas: a compact green *All systems operational* pill when
everything is healthy, and a red root-cause card during an incident.

The culprit cannot be read straight off the statuses. A leaf is **never** marked CRITICAL - the
server judges a service by its *outbound* calls and a leaf makes none - so `db-primary` reports
HEALTHY throughout its own outage. `analyseRootCause()` infers it instead:

1. Take the CRITICAL services (falling back to WARNING, so the banner can name a culprit while the
   rolling averages are still climbing).
2. Keep those that depend on nothing else degraded - the frontier, closest to the fault.
3. Blame the slowest dependency that frontier service is waiting on.

For a `db-primary` outage the frontier is `auth-service`, whose slowest dependency is
`db-primary` at ~716 ms against `redis-cache` at ~4 ms - so the cache branch is correctly not
blamed. Impact is counted as the degraded subset of that service's transitive dependents:

| State | Banner |
| --- | --- |
| healthy | `All systems operational` |
| 6 s into an outage | `Root cause: db-primary — 4 of 4 dependent services impacted · 407 ms · 46.8% errors on auth-service → db-primary` |
| 28 s in | same, evidence now `716 ms · 65.0% errors` |

While an operator outage is in flight but no service has crossed threshold yet, the banner shows
*Outage injected — impact propagating*, because the server's 50-sample average lags the fault by
several seconds.

**Node latency is derived, not served.** `GraphTopologyResponse` carries `avgLatencyMs` on
*edges* only - `ServiceNode` has just `serviceId`, `status` and `blastRadiusScore`. The card shows
the call-count-weighted mean of a node's outbound edges, which reconstructs the same rolling
average the server used to pick the node's status. A leaf such as `db-primary` makes no outbound
calls, so its latency renders as `--`. Adding `avg_latency_ms` to `ServiceNode` in the proto would
let the card read it directly.

## Simulated cluster

[MockClusterGenerator.kt](src/main/kotlin/com/telemetry/stream/tools/MockClusterGenerator.kt) drives a
synthetic estate at a running server:

```
frontend-gateway ----\
                      >--> payment-api --> auth-service --+--> db-primary   <- fault injected here
notification-worker -/                                    \--> redis-cache  <- healthy sibling
```

The graph branches in both directions: `auth-service` fans **out** to two dependencies, and
`payment-api` fans **in** from two callers. `redis-cache` carries no fault profile, so during a
`db-primary` outage its branch stays green while the database branch degrades - which is what
makes the blast radius reading meaningful rather than decorative. With breakers armed the database
branch shows as an open circuit rather than going red.

Started by [Quick start](#quick-start) above. The gRPC target is its one positional argument:

```sh
./gradlew mockCluster --args="localhost:50051"
```

Each service holds its own long-lived client-streaming RPC open and samples every 500 ms, the way a
sidecar agent would. The generator also polls `GetGraphTopology` once a second and prints the
server's own view.

### Latency propagation

Latency is **derived, not scripted**. Every service has only a local execution time; the fault is
injected in one place, `db-primary`'s local time. Each tick recomputes

```
serviceTime(X) = localExecution(X) + max( serviceTime(D) for D in dependencies(X) )
```

and the metric on edge `S -> T` reports `serviceTime(T)` - what the caller actually waited for,
already inclusive of everything beneath it. So latency **accumulates outward** and a leaf fault is
inherited by every service above it without any per-hop fault values.

`max` is what makes the fork behave: `auth-service` waits on the slowest of `db-primary` and
`redis-cache`, so it inherits the database delay while the `auth-service -> redis-cache` edge keeps
reporting its own few milliseconds.

Propagation is emergent rather than timed. Each pass reads the *previous* snapshot of service
times, so a change travels exactly one hop per 500 ms sample - no hard-coded per-hop delays.

Measured mid-outage with every rolling window fully saturated, **breakers disarmed** so the
cascade is allowed to develop:

| Edge | Baseline | During outage |
| --- | --- | --- |
| `auth-service -> db-primary` | 10.1 ms | 682 ms |
| `auth-service -> redis-cache` | 4.0 ms | **4 ms** |
| `payment-api -> auth-service` | 16.3 ms | 696 ms |
| `frontend-gateway -> payment-api` | 29.9 ms | 711 ms |
| `notification-worker -> payment-api` | 29.9 ms | 711 ms |

Latency grows strictly outward from the fault. All four upstream services reach CRITICAL; both
leaves stay HEALTHY, since the server judges a service by its outbound calls and a leaf makes none.
With breakers **armed** none of this happens - see [Circuit breakers](#circuit-breakers).

Because health is derived from a service's **outbound** calls, `db-primary` itself stays HEALTHY -
it makes no calls. What marks it as the culprit is its blast radius of 4.

**Error rates propagate the same way.** Each service has its own internal error rate, and a
caller observes its dependency's *effective* failure rate:

```
failureRate(X) = internalError(X) + max(failureRate of X's dependencies) * (1 - absorption(X))
```

Absorption is the share of a dependency's failures a service swallows through retries and
fallbacks, so failures attenuate on the way up while latency accumulates. Only the leaf carries a
faulted profile; nothing is scripted per hop, and `hopDelayMs` is gone.

This is what makes the circuit breaker coherent: an open breaker contributes the fallback's error
rate (~0.1%) rather than the dependency's, so a caller that is successfully falling back reports
success to *its* callers. Before this, `auth-service` would short-circuit and go fast while
`payment-api` still read a hardcoded 24% failure band and stayed WARNING.

Note the server has no latency *warning* band - `> 200 ms` is CRITICAL and the only WARNING rule is
on error rate - so a latency-only degradation jumps straight from HEALTHY to CRITICAL.

### A wrinkle worth knowing

A service's rolling window is keyed by *service*, not by edge, so `auth-service`'s two outbound
streams share one 50-entry window. Two consequences:

- Its window covers ~12.5 s of history rather than 25 s, because it fills at twice the rate.
- Its node-level latency is a blend of slow `db-primary` calls and fast `redis-cache` ones, so the
  aggregate understates the sick dependency. Mid-outage with breakers disarmed, the `auth-service`
  card reads ~343 ms while the `db-primary` edge behind it is at ~682 ms.

That is a real observability pathology, not a bug - an average across dependencies hides the
outlier. The drawer's per-edge breakdown is what exposes it.

### Choosing what breaks

The header carries three controls: a **duration** field in seconds (default 15, clamped 1-120 to
match the server), a **service** dropdown, and the *Simulate Outage* button itself.

One shared fault definition (`FAULT_LATENCY_MS`, `FAULT_ERROR_RATE`) is applied to whichever
service is targeted, so every service is breakable without a per-service profile.

The list is **restricted to services that have callers** - the targets of at least one edge.
Breaking a root such as `frontend-gateway` would be invisible: health is derived from a service's
*outbound* calls, and nothing calls a root, so its own latency spike is never observed by anyone.
The same reasoning explains why a leaf never shows CRITICAL for its own fault.

Measured with breakers disarmed, 27 s into a 30 s outage:

| Broken service | Services degraded | Blast radius |
| --- | --- | --- |
| `db-primary` | auth-service, payment-api, frontend-gateway, notification-worker | 4 |
| `redis-cache` | auth-service, payment-api, frontend-gateway, notification-worker | 4 |
| `auth-service` | payment-api, frontend-gateway, notification-worker | 3 |

Note the third row: breaking `auth-service` leaves `auth-service` itself HEALTHY. Its own calls to
`db-primary` and `redis-cache` are still fast and clean - it is only slow *to its callers*. A
mid-tier service is as invisible to its own fault as a leaf is, and the incident banner has to
infer it the same way.

Retargeting mid-outage works: the generator re-begins the window on the new service rather than
extending the old one. An unknown or blank service falls back to `db-primary`.

### Tuning the fault cadence

> These figures were measured with **circuit breakers disarmed**, which is the default. Arm them
> from the header switch and the breaker trips ~1.5 s into a fault, so the estate stays HEALTHY
> throughout and there is no cascade left to tune.

The 50-entry window holds **25 s** of history at a 500 ms sample rate, which interacts with the
fault period:

| `SPIKE_INTERVAL_MS` | Observed behaviour |
| --- | --- |
| 15000 (default) | Cascade reaches CRITICAL, but the estate never returns to HEALTHY - a fault always remains in the window |
| 20000 | Falls back to WARNING between faults, still never green |
| 40000 | Full cycle: HEALTHY -> cascade -> WARNING -> HEALTHY about 25 s after the fault clears |

Short faults only ever reach WARNING: a 3.5 s fault is ~7 samples in a 50-sample window, which
dilutes below the CRITICAL threshold. Override with environment variables:

```sh
SPIKE_INTERVAL_MS=40000 SPIKE_MIN_MS=4000 SPIKE_MAX_MS=4500 ./gradlew mockCluster
```

## Health and blast radius

Computed in [TelemetryServiceImpl.kt](src/main/kotlin/com/telemetry/stream/TelemetryServiceImpl.kt),
which retains the **last 50 metrics per service** in a `ConcurrentHashMap` of bounded windows.

**Status** is judged from the rolling averages over that window, not from single samples, so one
slow call cannot flip a service on its own:

| Condition (rolling average) | Status |
| --- | --- |
| error rate > 0.15, or latency > 200 ms | `CRITICAL` |
| error rate > 0.05 | `WARNING` |
| otherwise | `HEALTHY` |

Comparisons are strict, so a value sitting exactly on a threshold falls to the tier below.

**Blast radius** is the *count* of services that transitively depend on a service - how many nodes
are affected if it fails. Reverse reachability over the edge set; the visited-set guard also makes
it terminate on dependency cycles. For `checkout -> payments -> ledger`, `ledger` scores 2.

Dependency links are retained even after their metrics age out of the 50-entry window, so the graph
structure does not decay; such an edge simply reports `callCount: 0`.

## WebSocket broadcast

[Main.kt](src/main/kotlin/com/telemetry/stream/Main.kt) starts both servers in one process: gRPC on
50051, and Ktor on 8080 serving `/ws/metrics`.

A single coroutine ticks once per second, serialises the topology **once**, and publishes it to a
`MutableSharedFlow` that every connected session collects - so N clients cost one serialisation per
tick, not N, and all of them see the same snapshot.

- `replay = 1` hands a newly connected client the current graph immediately, rather than making it
  wait up to a second for the next tick.
- `BufferOverflow.DROP_OLDEST` means a slow client falls behind on its own and can never stall the
  ticker or the other clients. For always-send-latest state this is the right trade; note that a
  `StateFlow` would *not* work here, since it conflates equal values and would go silent whenever
  the graph stopped changing.

The raw per-metric feed is still available on the service as `TelemetryServiceImpl.stream`, but no
endpoint currently exposes it.
