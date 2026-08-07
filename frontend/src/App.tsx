import { useCallback, useEffect, useRef, useState } from 'react'
import {
  Background,
  Controls,
  Handle,
  MarkerType,
  MiniMap,
  Position,
  ReactFlow,
  useEdgesState,
  useNodesState,
  type Edge,
  type Node,
  type NodeProps,
  type NodeTypes,
  type XYPosition,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import './App.css'

const WS_URL = 'ws://localhost:8080/ws/metrics'
const RECONNECT_DELAY_MS = 2000

const COLUMN_WIDTH = 260
const ROW_HEIGHT = 130

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

function edgeStroke(errorRate: number): string {
  if (errorRate > 0.15) return '#dc2626'
  if (errorRate > 0.05) return '#d97706'
  return '#94a3b8'
}

/* --------------------------------------------------------------- node card */

function ServiceCard({ data }: NodeProps<ServiceCardNode>) {
  return (
    <div className={`svc-card svc-${data.status.toLowerCase()}`}>
      <Handle type="target" position={Position.Left} />

      <div className="svc-header">
        <span className="svc-name">{data.label}</span>
        <span className="svc-status">{data.status}</span>
      </div>

      <dl className="svc-metrics">
        <div>
          <dt>latency</dt>
          <dd>{data.callCount === 0 ? '--' : `${data.latencyMs.toFixed(1)} ms`}</dd>
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

/* --------------------------------------------------------------------- app */

export default function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState<ServiceCardNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [connected, setConnected] = useState(false)
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null)

  const applyTopology = useCallback(
    (topology: GraphTopologyDto) => {
      const positions = layout(topology)

      setNodes((current) => {
        // Reuse the position a node already has, so a snapshot every second never
        // undoes a drag; only nodes seen for the first time get laid out.
        const placed = new Map(current.map((node) => [node.id, node.position]))

        return topology.nodes.map((node) => ({
          id: node.serviceId,
          type: 'serviceCard' as const,
          position: placed.get(node.serviceId) ??
            positions.get(node.serviceId) ?? { x: 0, y: 0 },
          data: {
            label: node.serviceId,
            status: node.status,
            blastRadiusScore: node.blastRadiusScore,
            ...outboundStats(node.serviceId, topology.edges),
          },
        }))
      })

      setEdges(
        topology.edges.map((edge) => ({
          id: `${edge.source}->${edge.target}`,
          source: edge.source,
          target: edge.target,
          label: `${edge.avgLatencyMs.toFixed(0)} ms`,
          animated: edge.avgErrorRate > 0.05,
          markerEnd: { type: MarkerType.ArrowClosed, color: edgeStroke(edge.avgErrorRate) },
          style: {
            stroke: edgeStroke(edge.avgErrorRate),
            strokeWidth: edge.avgErrorRate > 0.05 ? 2.5 : 1.5,
          },
        })),
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

  return (
    <div className="app">
      <header className="app-bar">
        <h1>Telemetry Stream Engine</h1>
        <div className="app-status">
          <span className={`dot ${connected ? 'dot-live' : 'dot-down'}`} />
          {connected ? 'live' : 'reconnecting'}
          {lastUpdate && <span className="app-stamp">{lastUpdate.toLocaleTimeString()}</span>}
        </div>
      </header>

      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView
        proOptions={{ hideAttribution: false }}
      >
        <Background />
        <Controls />
        <MiniMap pannable zoomable />
      </ReactFlow>

      {nodes.length === 0 && (
        <p className="app-empty">
          Waiting for topology on {WS_URL} — start the server, then <code>./gradlew mockCluster</code>.
        </p>
      )}
    </div>
  )
}
