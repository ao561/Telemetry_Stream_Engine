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
}

type GraphTopologyDto = {
  nodes: ServiceNodeDto[]
  edges: DependencyEdgeDto[]
}

type OutageStateDto = {
  active: boolean
  remainingMs: number
}

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

  const ratio = (data as { labelPosition?: number } | undefined)?.labelPosition
    ?? LABEL_POSITION_BASE

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
            className="edge-badge"
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
  onClose: () => void
}

function ServiceDrawer({ topology, serviceId, onClose }: DrawerProps) {
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

      <section className="drawer-section">
        <h3>
          Depends on <span className="count">{outbound.length}</span>
        </h3>
        {outbound.length === 0 ? (
          <p className="drawer-note">Nothing — this is a leaf.</p>
        ) : (
          <ul className="drawer-list">
            {outbound.map((edge) => (
              <li key={edge.target}>
                <span className="peer">{edge.target}</span>
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

/* --------------------------------------------------------------------- app */

export default function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState<ServiceCardNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [topology, setTopology] = useState<GraphTopologyDto | null>(null)
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [connected, setConnected] = useState(false)
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null)

  const [outageEndsAt, setOutageEndsAt] = useState(0)
  const [outageError, setOutageError] = useState<string | null>(null)
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
      const state = (await response.json()) as OutageStateDto
      setOutageEndsAt(Date.now() + state.remainingMs)
    } catch (error) {
      setOutageError(error instanceof Error ? error.message : 'request failed')
    }
  }, [])

  const applyTopology = useCallback(
    (incoming: GraphTopologyDto) => {
      setTopology(incoming)
      const positions = layout(incoming)

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
            ...outboundStats(node.serviceId, incoming.edges),
          },
        }))
      })

      // Stagger badges across the edges leaving each node, so a fan-out never stacks them.
      const outboundSeen = new Map<string, number>()

      setEdges(
        incoming.edges.map((edge) => {
          const stroke = edgeStroke(edge.avgErrorRate)
          const index = outboundSeen.get(edge.source) ?? 0
          outboundSeen.set(edge.source, index + 1)
          return {
            id: `${edge.source}->${edge.target}`,
            source: edge.source,
            target: edge.target,
            label: `${edge.avgLatencyMs.toFixed(0)} ms`,
            animated: edge.avgErrorRate > 0.05,
            markerEnd: { ...EDGE_MARKER, color: stroke },
            style: { stroke, strokeWidth: EDGE_STROKE_WIDTH },
            data: {
              labelPosition: Math.min(
                LABEL_POSITION_BASE + index * LABEL_POSITION_STEP,
                LABEL_POSITION_MAX,
              ),
            },
          }
        }),
      )

      setLastUpdate(new Date())
    },
    [setNodes, setEdges],
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

      socket.onopen = () => setConnected(true)

      socket.onmessage = (event) => {
        try {
          applyTopologyRef.current(JSON.parse(event.data) as GraphTopologyDto)
        } catch (error) {
          console.error('discarding malformed topology frame', error)
        }
      }

      socket.onclose = () => {
        setConnected(false)
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
  }, [])

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
    </div>
  )
}
