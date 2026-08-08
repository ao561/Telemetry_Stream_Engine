import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  Background,
  BaseEdge,
  Controls,
  EdgeLabelRenderer,
  getSmoothStepPath,
  Handle,
  MarkerType,
  MiniMap,
  Position,
  ReactFlow,
  useEdgesState,
  useNodesState,
  type Edge,
  type EdgeProps,
  type EdgeTypes,
  type Node,
  type NodeProps,
  type NodeTypes,
  type XYPosition,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import './App.css'

const API_BASE = 'http://localhost:8080'
const WS_URL = 'ws://localhost:8080/ws/metrics'
const RECONNECT_DELAY_MS = 2000
const OUTAGE_DURATION_MS = 15000

/** Rolling latency history kept per service for the drawer sparkline. */
const HISTORY_WINDOW_MS = 30_000
const SPARK_WIDTH = 296
const SPARK_HEIGHT = 68
const SPARK_PAD = 7

/** Mirrors CRITICAL_LATENCY_MS on the server, drawn as the sparkline's threshold line. */
const CRITICAL_LATENCY_MS = 200
const CIRCUIT_COLOR = '#f59e0b'

/** Keeps the panel bounded during a long session. */
const LOG_LIMIT = 300

// Columns must clear the widest card (~215px for "notification-worker") with enough run
// left over that a mid-path label badge cannot reach either handle.
const COLUMN_WIDTH = 380
// Siblings in a column (frontend-gateway / notification-worker, db-primary / redis-cache) need
// enough vertical separation that their edge badges never land in the same band.
const ROW_HEIGHT = 200

const EDGE_STROKE_WIDTH = 2
const EDGE_MARKER = { type: MarkerType.ArrowClosed, width: 20, height: 20 } as const

/**
 * Badges sit part-way along the path, toward the source, rather than at the 50% midpoint where
 * several routes converge. Edges leaving the same node are staggered by LABEL_POSITION_STEP,
 * because a fan-out's routes have not diverged yet near their shared source - at a flat 0.25 the
 * two auth-service badges land 3px apart.
 */
const LABEL_POSITION_BASE = 0.35
const LABEL_POSITION_STEP = 0.15
const LABEL_POSITION_MAX = 0.8

/* ------------------------------------------------------------------ payload */

type Status = 'HEALTHY' | 'WARNING' | 'CRITICAL'

/** Mirrors GraphTopologyDto on the server. */
type ServiceNodeDto = {
  serviceId: string
  status: Status
  blastRadiusScore: number
}

type DependencyEdgeDto = {
  source: string
  target: string
  avgLatencyMs: number
  avgErrorRate: number
  callCount: number
  /** The caller's breaker on this dependency is open; calls are served from a fallback. */
  circuitOpen: boolean
}

type GraphTopologyDto = {
  nodes: ServiceNodeDto[]
  edges: DependencyEdgeDto[]
}

type ControlStateDto = {
  outageActive: boolean
  outageRemainingMs: number
  circuitBreakersEnabled: boolean
}

type LatencySample = { t: number; latencyMs: number }

type LogLevel = 'INFO' | 'WARN' | 'ALERT'

type LogEntry = {
  id: number
  at: Date
  level: LogLevel
  message: string
}

let logSequence = 0
const logEntry = (level: LogLevel, message: string): LogEntry => ({
  id: (logSequence += 1),
  at: new Date(),
  level,
  message,
})

const clockOf = (at: Date) => at.toLocaleTimeString('en-GB', { hour12: false })

/* -------------------------------------------------------------- node model */

// A type alias (not an interface) so it satisfies React Flow's Record<string, unknown> bound.
type ServiceCardData = {
  label: string
  status: Status
  blastRadiusScore: number
  latencyMs: number
  errorRate: number
  callCount: number
}

type ServiceCardNode = Node<ServiceCardData, 'serviceCard'>

/**
 * The server reports latency per dependency edge, not per node. A node's latency is therefore
 * the call-count-weighted mean of its outbound edges, which reproduces the same rolling average
 * the server itself used to decide the node's status.
 */
function outboundStats(serviceId: string, edges: DependencyEdgeDto[]) {
  const outbound = edges.filter((edge) => edge.source === serviceId)
  const calls = outbound.reduce((sum, edge) => sum + edge.callCount, 0)
  if (calls === 0) return { latencyMs: 0, errorRate: 0, callCount: 0 }

  return {
    latencyMs: outbound.reduce((sum, e) => sum + e.avgLatencyMs * e.callCount, 0) / calls,
    errorRate: outbound.reduce((sum, e) => sum + e.avgErrorRate * e.callCount, 0) / calls,
    callCount: calls,
  }
}

/**
 * Layered left-to-right layout: a node sits one column right of its callers. Edges are relaxed
 * at most `nodes.length` times so a dependency cycle terminates instead of looping forever.
 */
function layout(topology: GraphTopologyDto): Map<string, XYPosition> {
  const depth = new Map<string, number>()
  topology.nodes.forEach((node) => depth.set(node.serviceId, 0))

  for (let pass = 0; pass < topology.nodes.length; pass++) {
    let changed = false
    for (const edge of topology.edges) {
      const candidate = (depth.get(edge.source) ?? 0) + 1
      if (candidate > (depth.get(edge.target) ?? 0)) {
        depth.set(edge.target, candidate)
        changed = true
      }
    }
    if (!changed) break
  }

  const rowsPerColumn = new Map<number, number>()
  const positions = new Map<string, XYPosition>()
  for (const node of [...topology.nodes].sort((a, b) => a.serviceId.localeCompare(b.serviceId))) {
    const column = depth.get(node.serviceId) ?? 0
    const row = rowsPerColumn.get(column) ?? 0
    rowsPerColumn.set(column, row + 1)
    positions.set(node.serviceId, { x: column * COLUMN_WIDTH, y: row * ROW_HEIGHT })
  }
  return positions
}

const STATUS_COLOR: Record<Status, string> = {
  HEALTHY: '#10b981',
  WARNING: '#f59e0b',
  CRITICAL: '#ef4444',
}

/** Shared by every edge; per-edge options below only add the error-rate colouring. */
const defaultEdgeOptions = {
  type: 'latency' as const,
  markerEnd: EDGE_MARKER,
  style: { strokeWidth: EDGE_STROKE_WIDTH },
}

/**
 * Geometric point a given fraction along an SVG path. Uses a detached path element, which is
 * exact for the rounded corners a smoothstep route produces; falls back to the caller's
 * midpoint if the browser refuses to measure a detached node.
 */
function pointAlongPath(d: string, ratio: number): { x: number; y: number } | null {
  if (typeof document === 'undefined') return null
  try {
    const probe = document.createElementNS('http://www.w3.org/2000/svg', 'path')
    probe.setAttribute('d', d)
    const total = probe.getTotalLength()
    if (!Number.isFinite(total) || total === 0) return null
    const point = probe.getPointAtLength(total * ratio)
    return { x: point.x, y: point.y }
  } catch {
    return null
  }
}

/**
 * Smoothstep edge whose latency badge is rendered as HTML at [LABEL_POSITION] along the route,
 * instead of React Flow's default 50% midpoint - which is exactly where several edges converge.
 */
function LatencyEdge({
  id,
  sourceX,
  sourceY,
  sourcePosition,
  targetX,
  targetY,
  targetPosition,
  markerEnd,
  style,
  label,
  data,
}: EdgeProps) {
  const [path, midX, midY] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  })

  const meta = data as { labelPosition?: number; circuitOpen?: boolean } | undefined
  const ratio = meta?.labelPosition ?? LABEL_POSITION_BASE

  const badge = useMemo(
    () => pointAlongPath(path, ratio) ?? { x: midX, y: midY },
    [path, ratio, midX, midY],
  )

  return (
    <>
      <BaseEdge id={id} path={path} markerEnd={markerEnd} style={style} />
      {label != null && (
        <EdgeLabelRenderer>
          <div
            className={`edge-badge${meta?.circuitOpen ? ' edge-badge-open' : ''}`}
            style={{ transform: `translate(-50%, -50%) translate(${badge.x}px, ${badge.y}px)` }}
          >
            {label}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  )
}

const edgeTypes: EdgeTypes = { latency: LatencyEdge }

function edgeStroke(errorRate: number): string {
  if (errorRate > 0.15) return '#ef4444'
  if (errorRate > 0.05) return '#f59e0b'
  return '#475569'
}

const ms = (value: number) => `${value.toFixed(1)} ms`
const pct = (value: number) => `${(value * 100).toFixed(2)}%`

/* --------------------------------------------------------------- sparkline */

/** Cubic through the points, with control points at each segment's horizontal midpoint. */
function smoothPath(points: { x: number; y: number }[]): string {
  if (points.length < 2) return ''
  let d = `M ${points[0].x.toFixed(2)} ${points[0].y.toFixed(2)}`
  for (let i = 1; i < points.length; i++) {
    const from = points[i - 1]
    const to = points[i]
    const mid = ((from.x + to.x) / 2).toFixed(2)
    d += ` C ${mid} ${from.y.toFixed(2)}, ${mid} ${to.y.toFixed(2)}, ${to.x.toFixed(2)} ${to.y.toFixed(2)}`
  }
  return d
}

function LatencySparkline({
  serviceId,
  samples,
  status,
}: {
  serviceId: string
  samples: LatencySample[]
  status: Status
}) {
  if (samples.length < 2) {
    return <p className="drawer-note">Collecting latency history…</p>
  }

  const spanMs = Math.max(samples[samples.length - 1].t - samples[0].t, 1)
  const peak = Math.max(...samples.map((s) => s.latencyMs))
  // Keep the threshold on screen even when traffic is fast, so the headroom stays legible.
  const ceiling = Math.max(peak * 1.15, CRITICAL_LATENCY_MS * 1.15)

  const plotW = SPARK_WIDTH - SPARK_PAD * 2
  const plotH = SPARK_HEIGHT - SPARK_PAD * 2
  const points = samples.map((sample) => ({
    x: SPARK_PAD + ((sample.t - samples[0].t) / spanMs) * plotW,
    y: SPARK_HEIGHT - SPARK_PAD - (sample.latencyMs / ceiling) * plotH,
  }))

  const line = smoothPath(points)
  const last = points[points.length - 1]
  const base = SPARK_HEIGHT - SPARK_PAD
  const area = `${line} L ${last.x.toFixed(2)} ${base} L ${points[0].x.toFixed(2)} ${base} Z`
  const thresholdY = SPARK_HEIGHT - SPARK_PAD - (CRITICAL_LATENCY_MS / ceiling) * plotH
  const fillId = `spark-fill-${serviceId}`

  return (
    <div className={`spark spark-${status.toLowerCase()}`}>
      <div className="spark-head">
        <span>peak {peak.toFixed(0)} ms</span>
        <span>last {Math.round(spanMs / 1000)}s</span>
      </div>
      <svg
        width={SPARK_WIDTH}
        height={SPARK_HEIGHT}
        role="img"
        aria-label={`${serviceId} latency over the last ${Math.round(spanMs / 1000)} seconds, currently ${samples[samples.length - 1].latencyMs.toFixed(0)} milliseconds`}
      >
        <defs>
          <linearGradient id={fillId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="currentColor" stopOpacity="0.30" />
            <stop offset="100%" stopColor="currentColor" stopOpacity="0" />
          </linearGradient>
        </defs>

        {thresholdY > SPARK_PAD && (
          <>
            <line
              className="spark-threshold"
              x1={SPARK_PAD}
              y1={thresholdY}
              x2={SPARK_WIDTH - SPARK_PAD}
              y2={thresholdY}
            />
            <text className="spark-threshold-label" x={SPARK_WIDTH - SPARK_PAD} y={thresholdY - 4}>
              {CRITICAL_LATENCY_MS} ms
            </text>
          </>
        )}

        <path d={area} fill={`url(#${fillId})`} />
        <path className="spark-line" d={line} fill="none" />
        <circle className="spark-tip" cx={last.x} cy={last.y} r="3" />
      </svg>
    </div>
  )
}

/* -------------------------------------------------------------------- rca */

type RootCause = {
  service: string
  impacted: string[]
  dependents: number
  evidence: string | null
  severity: Status
}

/** Every service that transitively depends on [serviceId] - reverse reachability, cycle-safe. */
function dependentsOf(serviceId: string, edges: DependencyEdgeDto[]): Set<string> {
  const callers = new Map<string, string[]>()
  for (const edge of edges) {
    callers.set(edge.target, [...(callers.get(edge.target) ?? []), edge.source])
  }

  const seen = new Set<string>()
  const queue = [serviceId]
  while (queue.length > 0) {
    for (const caller of callers.get(queue.shift() as string) ?? []) {
      if (!seen.has(caller)) {
        seen.add(caller)
        queue.push(caller)
      }
    }
  }
  seen.delete(serviceId)
  return seen
}

/**
 * Infers the culprit behind a spreading incident.
 *
 * A leaf is never itself marked CRITICAL - the server judges a service by its *outbound* calls,
 * and a leaf makes none - so the root cause cannot simply be read off the statuses. Instead:
 * take the degraded services, keep those that depend on nothing else degraded (the frontier,
 * closest to the fault), and blame the slowest dependency that frontier service is waiting on.
 */
function analyseRootCause(topology: GraphTopologyDto): RootCause | null {
  const statusOf = new Map(topology.nodes.map((node) => [node.serviceId, node.status]))
  const outgoing = (id: string) => topology.edges.filter((edge) => edge.source === id)

  // Seed from *every* degraded service, not just the CRITICAL ones. During an uneven recovery a
  // mid-chain service can sit at WARNING while its callers are still CRITICAL; ignoring it would
  // make a caller look like the frontier and get the blame instead of the real leaf below it.
  const degraded = topology.nodes.filter((node) => node.status !== 'HEALTHY')

  // A tripped breaker hides the fault: the fallback path is fast and clean, so every service can
  // read HEALTHY while the dependency underneath is still broken. Report it rather than claiming
  // all is well.
  if (degraded.length === 0) {
    const open = topology.edges.filter((edge) => edge.circuitOpen)
    if (open.length === 0) return null
    const service = open[0].target
    const dependents = dependentsOf(service, topology.edges)
    return {
      service,
      impacted: [],
      dependents: dependents.size,
      severity: 'WARNING',
      evidence: `${open.length} circuit${open.length > 1 ? 's' : ''} open — traffic on fallback`,
    }
  }

  const degradedIds = new Set(degraded.map((node) => node.serviceId))
  const frontier = degraded.filter(
    (node) => !outgoing(node.serviceId).some((e) => degradedIds.has(e.target)),
  )
  const anchor = frontier[0] ?? degraded[0]

  // Slowest dependency of the frontier service - typically a healthy-looking leaf.
  const worst = [...outgoing(anchor.serviceId)].sort((a, b) => b.avgLatencyMs - a.avgLatencyMs)[0]
  const service = worst?.target ?? anchor.serviceId

  const dependents = dependentsOf(service, topology.edges)
  const impacted = [...dependents].filter((id) => statusOf.get(id) !== 'HEALTHY').sort()

  return {
    service,
    impacted,
    dependents: dependents.size,
    severity: degraded.some((node) => node.status === 'CRITICAL') ? 'CRITICAL' : 'WARNING',
    evidence: worst
      ? `${worst.avgLatencyMs.toFixed(0)} ms · ${(worst.avgErrorRate * 100).toFixed(1)}% errors on ${worst.source} → ${worst.target}`
      : null,
  }
}

/* --------------------------------------------------------------- node card */

function ServiceCard({ data, selected }: NodeProps<ServiceCardNode>) {
  return (
    <div className={`svc-card svc-${data.status.toLowerCase()}${selected ? ' svc-selected' : ''}`}>
      <Handle type="target" position={Position.Left} />

      <div className="svc-header">
        <span className="svc-name">{data.label}</span>
        <span className="svc-status">{data.status}</span>
      </div>

      <dl className="svc-metrics">
        <div>
          <dt>latency</dt>
          <dd>{data.callCount === 0 ? '--' : ms(data.latencyMs)}</dd>
        </div>
        <div>
          <dt>blast radius</dt>
          <dd>{data.blastRadiusScore}</dd>
        </div>
      </dl>

      <Handle type="source" position={Position.Right} />
    </div>
  )
}

const nodeTypes: NodeTypes = { serviceCard: ServiceCard }

/* ------------------------------------------------------------------ drawer */

type DrawerProps = {
  topology: GraphTopologyDto
  serviceId: string
  history: LatencySample[]
  onClose: () => void
}

function ServiceDrawer({ topology, serviceId, history, onClose }: DrawerProps) {
  const node = topology.nodes.find((n) => n.serviceId === serviceId)
  if (!node) return null

  const outbound = topology.edges.filter((e) => e.source === serviceId)
  const inbound = topology.edges.filter((e) => e.target === serviceId)
  const stats = outboundStats(serviceId, topology.edges)

  return (
    <aside className="drawer" aria-label={`Details for ${serviceId}`}>
      <header className="drawer-head">
        <div>
          <h2>{node.serviceId}</h2>
          <span className={`badge badge-${node.status.toLowerCase()}`}>{node.status}</span>
        </div>
        <button className="drawer-close" onClick={onClose} aria-label="Close details">
          ×
        </button>
      </header>

      <section className="drawer-section">
        <h3>Rolling health</h3>
        <dl className="drawer-stats">
          <div>
            <dt>Latency</dt>
            <dd>{stats.callCount === 0 ? '--' : ms(stats.latencyMs)}</dd>
          </div>
          <div>
            <dt>Error rate</dt>
            <dd>{stats.callCount === 0 ? '--' : pct(stats.errorRate)}</dd>
          </div>
          <div>
            <dt>Samples in window</dt>
            <dd>{stats.callCount}</dd>
          </div>
          <div>
            <dt>Blast radius</dt>
            <dd>{node.blastRadiusScore}</dd>
          </div>
        </dl>
        {stats.callCount === 0 && (
          <p className="drawer-note">
            Leaf service — it makes no outbound calls, so the server reports no latency for it.
            Its health is inferred by its callers.
          </p>
        )}
      </section>

      {stats.callCount > 0 && (
        <section className="drawer-section">
          <h3>Latency, last 30s</h3>
          <LatencySparkline serviceId={serviceId} samples={history} status={node.status} />
        </section>
      )}

      <section className="drawer-section">
        <h3>
          Depends on <span className="count">{outbound.length}</span>
        </h3>
        {outbound.length === 0 ? (
          <p className="drawer-note">Nothing — this is a leaf.</p>
        ) : (
          <ul className="drawer-list">
            {outbound.map((edge) => (
              <li key={edge.target} className={edge.circuitOpen ? 'peer-open' : undefined}>
                <span className="peer">
                  {edge.target}
                  {edge.circuitOpen && <span className="peer-badge">circuit open</span>}
                </span>
                <span className="peer-stats">
                  {ms(edge.avgLatencyMs)} · {pct(edge.avgErrorRate)} · {edge.callCount} calls
                </span>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="drawer-section">
        <h3>
          Called by <span className="count">{inbound.length}</span>
        </h3>
        {inbound.length === 0 ? (
          <p className="drawer-note">Nothing — this is an entry point.</p>
        ) : (
          <ul className="drawer-list">
            {inbound.map((edge) => (
              <li key={edge.source}>
                <span className="peer">{edge.source}</span>
                <span className="peer-stats">
                  {ms(edge.avgLatencyMs)} · {pct(edge.avgErrorRate)} · {edge.callCount} calls
                </span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </aside>
  )
}

/* ------------------------------------------------------------- log events */

type OutboundStats = { latencyMs: number; errorRate: number; callCount: number }

/** Why a service crossed a line, phrased from whichever signal actually tripped it. */
function describeStatus(status: Status, stats: OutboundStats | undefined): string {
  const latency = stats ? `${stats.latencyMs.toFixed(0)}ms` : 'n/a'
  const errors = stats ? `${(stats.errorRate * 100).toFixed(1)}%` : 'n/a'
  const latencyTripped = (stats?.latencyMs ?? 0) > CRITICAL_LATENCY_MS

  if (status === 'CRITICAL') {
    return latencyTripped
      ? `critical — ${latency} avg latency via cascading dependency`
      : `critical — ${errors} error rate`
  }
  if (status === 'WARNING') return `degraded — ${errors} error rate, ${latency} avg latency`
  return `recovered — ${latency} avg latency`
}

/**
 * Diffs the incoming snapshot against the last one and emits only genuine transitions, so a
 * once-per-second stream does not become a once-per-second log.
 */
function deriveEvents(
  incoming: GraphTopologyDto,
  statsById: Map<string, OutboundStats>,
  lastStatus: { current: Map<string, Status> },
  lastRootCause: { current: string | null },
  lastCircuits: { current: Set<string> },
): LogEntry[] {
  const events: LogEntry[] = []
  const previous = lastStatus.current
  const current = new Map<string, Status>()

  for (const node of incoming.nodes) {
    current.set(node.serviceId, node.status)
    const before = previous.get(node.serviceId)

    if (before === undefined) {
      events.push(logEntry('INFO', `${node.serviceId} joined the graph (${node.status})`))
      continue
    }
    if (before === node.status) continue

    const level: LogLevel =
      node.status === 'CRITICAL' ? 'ALERT' : node.status === 'WARNING' ? 'WARN' : 'INFO'
    events.push(
      logEntry(level, `${node.serviceId} ${describeStatus(node.status, statsById.get(node.serviceId))}`),
    )
  }

  for (const serviceId of previous.keys()) {
    if (!current.has(serviceId)) events.push(logEntry('INFO', `${serviceId} left the graph`))
  }

  // Root cause is inferred, not reported: a leaf is never CRITICAL itself, so this is the only
  // place the actual culprit gets named.
  const rca = analyseRootCause(incoming)
  if (rca && rca.service !== lastRootCause.current) {
    const worst = incoming.edges
      .filter((edge) => edge.target === rca.service)
      .sort((a, b) => b.avgLatencyMs - a.avgLatencyMs)[0]
    events.push(
      logEntry(
        'ALERT',
        worst
          ? `${rca.service} latency spiked to ${worst.avgLatencyMs.toFixed(0)}ms — ${rca.impacted.length} upstream services impacted`
          : `${rca.service} identified as root cause — ${rca.impacted.length} upstream services impacted`,
      ),
    )
  }
  if (!rca && lastRootCause.current !== null) {
    events.push(logEntry('INFO', 'Cluster status normal — all services healthy'))
  }

  const open = new Set(
    incoming.edges.filter((edge) => edge.circuitOpen).map((edge) => `${edge.source}->${edge.target}`),
  )
  for (const edge of open) {
    if (!lastCircuits.current.has(edge)) {
      events.push(logEntry('WARN', `Circuit OPEN on ${edge} — calls short-circuited to fallback`))
    }
  }
  for (const edge of lastCircuits.current) {
    if (!open.has(edge)) {
      events.push(logEntry('INFO', `Circuit CLOSED on ${edge} — dependency recovered`))
    }
  }

  lastStatus.current = current
  lastRootCause.current = rca?.service ?? null
  lastCircuits.current = open
  return events
}

/* ---------------------------------------------------------------- log panel */

function IncidentLog({
  entries,
  collapsed,
  onToggle,
  onClear,
  inset,
}: {
  entries: LogEntry[]
  collapsed: boolean
  onToggle: () => void
  onClear: () => void
  inset: boolean
}) {
  const bodyRef = useRef<HTMLDivElement>(null)
  // Only chase the tail while the reader is already at the bottom, so scrolling back
  // through an incident is not yanked away by the next event.
  const pinnedRef = useRef(true)

  useEffect(() => {
    const body = bodyRef.current
    if (body && pinnedRef.current) body.scrollTop = body.scrollHeight
  }, [entries, collapsed])

  return (
    <section className={`log${collapsed ? ' log-collapsed' : ''}${inset ? ' log-inset' : ''}`}>
      <header className="log-bar">
        <button className="log-toggle" onClick={onToggle} aria-expanded={!collapsed}>
          <span className={`log-caret${collapsed ? ' log-caret-closed' : ''}`}>▾</span>
          Incident log
        </button>
        <span className="log-count">{entries.length}</span>
        <button className="log-clear" onClick={onClear} disabled={entries.length === 0}>
          Clear
        </button>
      </header>

      {!collapsed && (
        <div
          className="log-body"
          ref={bodyRef}
          onScroll={(event) => {
            const el = event.currentTarget
            pinnedRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 24
          }}
        >
          {entries.length === 0 ? (
            <p className="log-empty">No events yet — waiting on cluster state changes.</p>
          ) : (
            entries.map((entry) => (
              <p key={entry.id} className={`log-line log-${entry.level.toLowerCase()}`}>
                <span className="log-time">[{clockOf(entry.at)}]</span>
                <span className="log-level">{entry.level}:</span>
                <span className="log-msg">{entry.message}</span>
              </p>
            ))
          )}
        </div>
      )}
    </section>
  )
}

/* --------------------------------------------------------------------- app */

export default function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState<ServiceCardNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [topology, setTopology] = useState<GraphTopologyDto | null>(null)
  const [history, setHistory] = useState<Map<string, LatencySample[]>>(() => new Map())
  const [log, setLog] = useState<LogEntry[]>([])
  const [logCollapsed, setLogCollapsed] = useState(false)

  // Previous cluster state, so events fire on transitions rather than once per frame.
  const lastStatusRef = useRef<Map<string, Status>>(new Map())
  const lastRootCauseRef = useRef<string | null>(null)
  const lastCircuitsRef = useRef<Set<string>>(new Set())

  const appendLog = useCallback((entries: LogEntry[]) => {
    if (entries.length === 0) return
    setLog((previous) => [...previous, ...entries].slice(-LOG_LIMIT))
  }, [])
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [connected, setConnected] = useState(false)
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null)

  const [outageEndsAt, setOutageEndsAt] = useState(0)
  const [outageError, setOutageError] = useState<string | null>(null)
  const [breakersEnabled, setBreakersEnabled] = useState(true)
  const [clock, setClock] = useState(() => Date.now())

  // Drives the outage countdown without re-rendering on every websocket frame.
  useEffect(() => {
    const id = setInterval(() => setClock(Date.now()), 250)
    return () => clearInterval(id)
  }, [])

  const outageRemainingMs = Math.max(0, outageEndsAt - clock)
  const outageActive = outageRemainingMs > 0

  const triggerOutage = useCallback(async () => {
    setOutageError(null)
    try {
      const response = await fetch(
        `${API_BASE}/api/outage?durationMs=${OUTAGE_DURATION_MS}`,
        { method: 'POST' },
      )
      if (!response.ok) throw new Error(`server returned ${response.status}`)
      const state = (await response.json()) as ControlStateDto
      setOutageEndsAt(Date.now() + state.outageRemainingMs)
      setBreakersEnabled(state.circuitBreakersEnabled)
      appendLog([
        logEntry('ALERT', `Outage injected for ${(state.outageRemainingMs / 1000).toFixed(0)}s`),
      ])
    } catch (error) {
      const reason = error instanceof Error ? error.message : 'request failed'
      setOutageError(reason)
      appendLog([logEntry('WARN', `Outage request failed — ${reason}`)])
    }
  }, [appendLog])

  const toggleBreakers = useCallback(
    async (enabled: boolean) => {
      // Optimistic, so the switch feels immediate; the response is authoritative.
      setBreakersEnabled(enabled)
      try {
        const response = await fetch(
          `${API_BASE}/api/circuit-breakers?enabled=${enabled}`,
          { method: 'POST' },
        )
        if (!response.ok) throw new Error(`server returned ${response.status}`)
        const state = (await response.json()) as ControlStateDto
        setBreakersEnabled(state.circuitBreakersEnabled)
        appendLog([
          logEntry(
            enabled ? 'INFO' : 'WARN',
            `Circuit breakers ${enabled ? 'armed' : 'disarmed'} — dependency failures will ${enabled ? 'be short-circuited' : 'cascade upstream'}`,
          ),
        ])
      } catch (error) {
        const reason = error instanceof Error ? error.message : 'request failed'
        setBreakersEnabled(!enabled)
        appendLog([logEntry('WARN', `Circuit breaker toggle failed — ${reason}`)])
      }
    },
    [appendLog],
  )

  // The switch reflects server state, so a reload does not show a stale position.
  useEffect(() => {
    let cancelled = false
    fetch(`${API_BASE}/api/controls`)
      .then((response) => (response.ok ? response.json() : null))
      .then((state: ControlStateDto | null) => {
        if (!cancelled && state) setBreakersEnabled(state.circuitBreakersEnabled)
      })
      .catch(() => undefined)
    return () => {
      cancelled = true
    }
  }, [])

  const applyTopology = useCallback(
    (incoming: GraphTopologyDto) => {
      setTopology(incoming)
      const positions = layout(incoming)
      const statsById = new Map(
        incoming.nodes.map((node) => [node.serviceId, outboundStats(node.serviceId, incoming.edges)]),
      )

      setHistory((previous) => {
        const now = Date.now()
        const next = new Map<string, LatencySample[]>()
        for (const node of incoming.nodes) {
          const kept = (previous.get(node.serviceId) ?? [])
            .filter((sample) => now - sample.t <= HISTORY_WINDOW_MS)
          const stats = statsById.get(node.serviceId)
          // A leaf makes no outbound calls, so it contributes no latency history.
          if (stats && stats.callCount > 0) kept.push({ t: now, latencyMs: stats.latencyMs })
          next.set(node.serviceId, kept)
        }
        // Rebuilt from `incoming`, so a service that disappears drops out of the buffer.
        return next
      })

      setNodes((current) => {
        // Carry over what React Flow already knows about each node, so a snapshot every
        // second never undoes a drag or clears the selection ring. Only nodes seen for the
        // first time get laid out.
        const existing = new Map(current.map((node) => [node.id, node]))

        return incoming.nodes.map((node) => ({
          id: node.serviceId,
          type: 'serviceCard' as const,
          selected: existing.get(node.serviceId)?.selected ?? false,
          position: existing.get(node.serviceId)?.position ??
            positions.get(node.serviceId) ?? { x: 0, y: 0 },
          data: {
            label: node.serviceId,
            status: node.status,
            blastRadiusScore: node.blastRadiusScore,
            ...(statsById.get(node.serviceId) ?? { latencyMs: 0, errorRate: 0, callCount: 0 }),
          },
        }))
      })

      // Stagger badges across the edges leaving each node, so a fan-out never stacks them.
      const outboundSeen = new Map<string, number>()

      setEdges(
        incoming.edges.map((edge) => {
          // An open breaker is its own signal: amber and broken, regardless of error rate,
          // because the fallback path is fast and clean and would otherwise render healthy.
          const stroke = edge.circuitOpen ? CIRCUIT_COLOR : edgeStroke(edge.avgErrorRate)
          const index = outboundSeen.get(edge.source) ?? 0
          outboundSeen.set(edge.source, index + 1)
          return {
            id: `${edge.source}->${edge.target}`,
            source: edge.source,
            target: edge.target,
            label: edge.circuitOpen
              ? `OPEN · ${edge.avgLatencyMs.toFixed(0)} ms`
              : `${edge.avgLatencyMs.toFixed(0)} ms`,
            animated: !edge.circuitOpen && edge.avgErrorRate > 0.05,
            markerEnd: { ...EDGE_MARKER, color: stroke },
            style: {
              stroke,
              strokeWidth: EDGE_STROKE_WIDTH,
              ...(edge.circuitOpen ? { strokeDasharray: '7 5' } : {}),
            },
            data: {
              circuitOpen: edge.circuitOpen,
              labelPosition: Math.min(
                LABEL_POSITION_BASE + index * LABEL_POSITION_STEP,
                LABEL_POSITION_MAX,
              ),
            },
          }
        }),
      )

      appendLog(deriveEvents(incoming, statsById, lastStatusRef, lastRootCauseRef, lastCircuitsRef))
      setLastUpdate(new Date())
    },
    [setNodes, setEdges, appendLog],
  )

  // Latest-ref: lets the socket effect run once while still calling the current
  // handler, so a changed callback identity never tears the connection down.
  const applyTopologyRef = useRef(applyTopology)
  useEffect(() => {
    applyTopologyRef.current = applyTopology
  }, [applyTopology])

  useEffect(() => {
    let socket: WebSocket | null = null
    let retry: ReturnType<typeof setTimeout> | undefined
    let disposed = false

    const connect = () => {
      socket = new WebSocket(WS_URL)

      socket.onopen = () => {
        setConnected(true)
        appendLog([logEntry('INFO', `Connected to ${WS_URL}`)])
      }

      socket.onmessage = (event) => {
        try {
          applyTopologyRef.current(JSON.parse(event.data) as GraphTopologyDto)
        } catch (error) {
          console.error('discarding malformed topology frame', error)
        }
      }

      socket.onclose = () => {
        setConnected((wasConnected) => {
          if (wasConnected) appendLog([logEntry('WARN', 'Telemetry stream lost — reconnecting')])
          return false
        })
        if (!disposed) retry = setTimeout(connect, RECONNECT_DELAY_MS)
      }

      // onerror is always followed by onclose, which owns the retry.
      socket.onerror = () => socket?.close()
    }

    connect()

    return () => {
      disposed = true
      if (retry) clearTimeout(retry)
      socket?.close()
    }
  }, [appendLog])

  const rootCause = useMemo(
    () => (topology ? analyseRootCause(topology) : null),
    [topology],
  )

  const critical = useMemo(
    () => (topology?.nodes ?? []).filter((n) => n.status === 'CRITICAL').length,
    [topology],
  )
  const warning = useMemo(
    () => (topology?.nodes ?? []).filter((n) => n.status === 'WARNING').length,
    [topology],
  )

  return (
    <div className="app">
      <header className="bar">
        <div className="bar-brand">
          <span className="bar-mark" />
          <h1>Telemetry Stream Engine</h1>
        </div>

        <div className="bar-metrics">
          <span className="chip">
            <em>{topology?.nodes.length ?? 0}</em> services
          </span>
          <span className={`chip ${warning ? 'chip-warn' : ''}`}>
            <em>{warning}</em> warning
          </span>
          <span className={`chip ${critical ? 'chip-crit' : ''}`}>
            <em>{critical}</em> critical
          </span>
        </div>

        <div className="bar-actions">
          {outageError && <span className="bar-error">outage failed: {outageError}</span>}
          <label className={`switch${breakersEnabled ? ' switch-on' : ''}`}>
            <input
              type="checkbox"
              checked={breakersEnabled}
              onChange={(event) => toggleBreakers(event.currentTarget.checked)}
            />
            <span className="switch-track" aria-hidden="true">
              <span className="switch-thumb" />
            </span>
            Circuit Breakers
          </label>
          <button
            className={`btn-outage${outageActive ? ' btn-outage-live' : ''}`}
            onClick={triggerOutage}
            disabled={outageActive}
          >
            {outageActive
              ? `Outage active · ${(outageRemainingMs / 1000).toFixed(0)}s`
              : 'Simulate Outage'}
          </button>
          <span className="bar-conn">
            <span className={`dot ${connected ? 'dot-live' : 'dot-down'}`} />
            {connected ? 'live' : 'reconnecting'}
            {lastUpdate && <span className="bar-stamp">{lastUpdate.toLocaleTimeString()}</span>}
          </span>
        </div>
      </header>

      <div className="stage">
        <div className="canvas">
        {/* Incident banner. Shown while an outage is in flight even before the rolling
            windows have caught up, since the server's 50-sample average lags the fault. */}
        <div className={`rca${selectedId ? ' rca-shifted' : ''}`}>
          {rootCause ? (
            <div
              className={`rca-card ${rootCause.severity === 'CRITICAL' ? 'rca-incident' : 'rca-pending'}`}
            >
              <span className="rca-dot" />
              <div className="rca-text">
                <strong>Root cause: {rootCause.service}</strong>
                <span>
                  {rootCause.impacted.length} of {rootCause.dependents} dependent services impacted
                  {rootCause.impacted.length > 0 && ` — ${rootCause.impacted.join(', ')}`}
                  {rootCause.evidence && ` · ${rootCause.evidence}`}
                </span>
              </div>
              {outageActive && <span className="rca-tag">outage injected</span>}
            </div>
          ) : outageActive ? (
            <div className="rca-card rca-pending">
              <span className="rca-dot" />
              <div className="rca-text">
                <strong>Outage injected</strong>
                <span>impact propagating — rolling averages have not crossed threshold yet</span>
              </div>
            </div>
          ) : (
            <div className="rca-card rca-ok">
              <span className="rca-dot" />
              All systems operational
            </div>
          )}
        </div>

        <ReactFlow
          colorMode="dark"
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={(_, node) => setSelectedId(node.id)}
          onPaneClick={() => setSelectedId(null)}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          defaultEdgeOptions={defaultEdgeOptions}
          fitView
        >
          <Background gap={22} size={1} color="#1e293b" />
          <Controls />
          <MiniMap
            pannable
            zoomable
            nodeColor={(node) => STATUS_COLOR[(node.data as ServiceCardData).status]}
            maskColor="rgba(8, 12, 20, 0.75)"
          />
        </ReactFlow>

        {topology && selectedId && (
          <ServiceDrawer
            topology={topology}
            serviceId={selectedId}
            history={history.get(selectedId) ?? []}
            onClose={() => setSelectedId(null)}
          />
        )}

        {nodes.length === 0 && (
          <p className="stage-empty">
            Waiting for topology on {WS_URL} — start the server, then{' '}
            <code>./gradlew mockCluster</code>.
          </p>
        )}
        </div>

        <IncidentLog
          entries={log}
          collapsed={logCollapsed}
          onToggle={() => setLogCollapsed((open) => !open)}
          onClear={() => setLog([])}
          inset={selectedId !== null}
        />
      </div>
    </div>
  )
}
