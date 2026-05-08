"""
Communicating Agent Network Search — FastAPI Web Application
"""

import asyncio
import json
import time
import uuid
from collections import deque
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from typing import Any, Optional

import networkx as nx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

# ── Message Types ──────────────────────────────────────────────────────────────

class MsgType(str, Enum):
    ASSIGN_FRONTIER = "ASSIGN_FRONTIER"
    NODE_VISITED    = "NODE_VISITED"
    GOSSIP_STATE    = "GOSSIP_STATE"
    PATH_FOUND      = "PATH_FOUND"
    HEARTBEAT       = "HEARTBEAT"
    SEARCH_DONE     = "SEARCH_DONE"
    AGENT_IDLE      = "AGENT_IDLE"

@dataclass
class Message:
    type: MsgType
    sender_id: str
    payload: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self):
        return {
            "type": self.type.value,
            "sender_id": self.sender_id,
            "payload": self.payload,
            "timestamp": self.timestamp,
        }

# ── Graph Factory ──────────────────────────────────────────────────────────────

GRAPH_PRESETS = {
    "small":   {"n": 50,   "m": 3, "label": "Small (50 nodes)"},
    "medium":  {"n": 200,  "m": 3, "label": "Medium (200 nodes)"},
    "large":   {"n": 500,  "m": 4, "label": "Large (500 nodes)"},
    "grid":    {"type": "grid", "rows": 10, "cols": 10, "label": "Grid (100 nodes)"},
    "star":    {"type": "star", "n": 50, "label": "Star (50 nodes)"},
    "cycle":   {"type": "cycle", "n": 100, "label": "Cycle (100 nodes)"},
}

def build_graph(preset: str, seed: int = 42) -> nx.Graph:
    cfg = GRAPH_PRESETS.get(preset, GRAPH_PRESETS["medium"])
    if cfg.get("type") == "grid":
        G = nx.grid_2d_graph(cfg["rows"], cfg["cols"])
        G = nx.convert_node_labels_to_integers(G)
    elif cfg.get("type") == "star":
        G = nx.star_graph(cfg["n"] - 1)
    elif cfg.get("type") == "cycle":
        G = nx.cycle_graph(cfg["n"])
    else:
        G = nx.barabasi_albert_graph(n=cfg["n"], m=cfg["m"], seed=seed)
    return G

# ── Search Agent ───────────────────────────────────────────────────────────────

class SearchAgent:
    def __init__(self, agent_id: str, graph: nx.Graph,
                 inbox: asyncio.Queue, event_queue: asyncio.Queue,
                 visited_global: set, found_event: asyncio.Event,
                 algorithm: str = "bfs"):
        self.id = agent_id
        self.graph = graph
        self.inbox = inbox
        self.events = event_queue
        self.visited_global = visited_global
        self.found_event = found_event
        self.algorithm = algorithm
        self.frontier: deque = deque()
        self.visited_local: set = set()
        self.nodes_expanded = 0
        self.active = False
        self.color_index = int(agent_id.split("-")[1])

    async def run(self):
        self.active = True
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._process_messages())
                tg.create_task(self._expand_frontier())
                tg.create_task(self._heartbeat())
        except* asyncio.CancelledError:
            pass
        finally:
            self.active = False

    async def _process_messages(self):
        while not self.found_event.is_set():
            try:
                msg = await asyncio.wait_for(self.inbox.get(), timeout=0.1)
                if msg.type == MsgType.ASSIGN_FRONTIER:
                    nodes = msg.payload.get("nodes", [])
                    self.frontier.extend(nodes)
                    await self.events.put(Message(
                        type=MsgType.ASSIGN_FRONTIER,
                        sender_id=self.id,
                        payload={"nodes": nodes, "frontier_size": len(self.frontier)}
                    ))
                elif msg.type == MsgType.GOSSIP_STATE:
                    new_visited = set(msg.payload.get("visited", []))
                    self.visited_local |= new_visited
                self.inbox.task_done()
            except asyncio.TimeoutError:
                continue

    async def _expand_frontier(self):
        while not self.found_event.is_set():
            if not self.frontier:
                await asyncio.sleep(0.01)
                continue

            node = self.frontier.popleft()

            if node in self.visited_global:
                continue

            self.visited_global.add(node)
            self.visited_local.add(node)
            self.nodes_expanded += 1

            await self.events.put(Message(
                type=MsgType.NODE_VISITED,
                sender_id=self.id,
                payload={
                    "node": node,
                    "depth": self.nodes_expanded,
                    "frontier_size": len(self.frontier),
                    "agent_color": self.color_index,
                }
            ))

            # Small delay to make visualization smooth
            await asyncio.sleep(0.03)

            neighbors = list(self.graph.neighbors(node))
            for nb in neighbors:
                if nb not in self.visited_global:
                    self.frontier.append(nb)

        self.active = False

    async def _heartbeat(self):
        while not self.found_event.is_set():
            await asyncio.sleep(1.5)
            await self.events.put(Message(
                type=MsgType.HEARTBEAT,
                sender_id=self.id,
                payload={
                    "nodes_expanded": self.nodes_expanded,
                    "frontier_size": len(self.frontier),
                    "active": self.active,
                }
            ))

# ── Search Coordinator ─────────────────────────────────────────────────────────

class SearchCoordinator:
    def __init__(self, graph: nx.Graph, num_agents: int,
                 event_queue: asyncio.Queue, algorithm: str = "bfs"):
        self.graph = graph
        self.num_agents = num_agents
        self.event_queue = event_queue
        self.algorithm = algorithm
        self.agents: list[SearchAgent] = []
        self.inboxes: list[asyncio.Queue] = []
        self.visited_global: set = set()
        self.found_event = asyncio.Event()
        self.tasks: list[asyncio.Task] = []

    def _setup_agents(self):
        self.agents.clear()
        self.inboxes.clear()
        for i in range(self.num_agents):
            inbox = asyncio.Queue(maxsize=500)
            agent = SearchAgent(
                agent_id=f"agent-{i}",
                graph=self.graph,
                inbox=inbox,
                event_queue=self.event_queue,
                visited_global=self.visited_global,
                found_event=self.found_event,
                algorithm=self.algorithm,
            )
            self.agents.append(agent)
            self.inboxes.append(inbox)

    async def search(self, start: int, target: int):
        self.found_event.clear()
        self.visited_global.clear()
        self._setup_agents()

        start_time = time.time()

        await self.event_queue.put(Message(
            type=MsgType.ASSIGN_FRONTIER,
            sender_id="coordinator",
            payload={"nodes": [start], "agent": "agent-0"}
        ))

        # Seed agent-0 with the start node
        await self.inboxes[0].put(Message(
            type=MsgType.ASSIGN_FRONTIER,
            sender_id="coordinator",
            payload={"nodes": [start]}
        ))

        # Run all agents concurrently
        self.tasks = [asyncio.create_task(agent.run()) for agent in self.agents]

        # Monitor for target
        monitor_task = asyncio.create_task(
            self._monitor(target, start_time)
        )

        # Gossip loop
        gossip_task = asyncio.create_task(self._gossip_loop())

        # Rebalance loop
        rebalance_task = asyncio.create_task(self._rebalance_loop())

        await asyncio.gather(monitor_task, return_exceptions=True)
        gossip_task.cancel()
        rebalance_task.cancel()
        for t in self.tasks:
            t.cancel()

    async def _monitor(self, target: int, start_time: float):
        while True:
            if target in self.visited_global:
                elapsed = time.time() - start_time
                total = sum(a.nodes_expanded for a in self.agents)
                self.found_event.set()
                await self.event_queue.put(Message(
                    type=MsgType.PATH_FOUND,
                    sender_id="coordinator",
                    payload={
                        "target": target,
                        "elapsed_ms": round(elapsed * 1000, 2),
                        "total_nodes_visited": total,
                        "agents_used": self.num_agents,
                    }
                ))
                await self.event_queue.put(Message(
                    type=MsgType.SEARCH_DONE,
                    sender_id="coordinator",
                    payload={"success": True}
                ))
                return

            # Check if all agents are idle (target not reachable)
            if all(len(a.frontier) == 0 for a in self.agents):
                await asyncio.sleep(0.3)
                if all(len(a.frontier) == 0 for a in self.agents):
                    self.found_event.set()
                    await self.event_queue.put(Message(
                        type=MsgType.SEARCH_DONE,
                        sender_id="coordinator",
                        payload={"success": False, "reason": "Target not reachable"}
                    ))
                    return

            await asyncio.sleep(0.05)

    async def _gossip_loop(self):
        """Periodically share visited sets between agents."""
        while not self.found_event.is_set():
            await asyncio.sleep(0.5)
            if len(self.agents) < 2:
                continue
            for i, agent in enumerate(self.agents):
                peer = self.agents[(i + 1) % len(self.agents)]
                if agent.visited_local:
                    sample = list(agent.visited_local)[-50:]
                    try:
                        peer.inbox.put_nowait(Message(
                            type=MsgType.GOSSIP_STATE,
                            sender_id=agent.id,
                            payload={"visited": sample}
                        ))
                    except asyncio.QueueFull:
                        pass

    async def _rebalance_loop(self):
        """Steal frontier nodes from busy agents and give to idle ones."""
        while not self.found_event.is_set():
            await asyncio.sleep(0.8)
            if len(self.agents) < 2:
                continue
            busy = max(self.agents, key=lambda a: len(a.frontier))
            idle = min(self.agents, key=lambda a: len(a.frontier))
            if len(busy.frontier) > 10 and len(idle.frontier) < 3 and busy.id != idle.id:
                stolen = []
                for _ in range(min(5, len(busy.frontier) // 2)):
                    if busy.frontier:
                        stolen.append(busy.frontier.pop())
                if stolen:
                    idle.frontier.extend(stolen)
                    await self.event_queue.put(Message(
                        type=MsgType.GOSSIP_STATE,
                        sender_id="coordinator",
                        payload={
                            "from": busy.id,
                            "to": idle.id,
                            "nodes_transferred": len(stolen),
                        }
                    ))

# ── FastAPI App ────────────────────────────────────────────────────────────────

app = FastAPI(title="Communicating Agent Network Search")

# ── WebSocket Manager ──────────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active: dict[str, WebSocket] = {}

    async def connect(self, session_id: str, ws: WebSocket):
        await ws.accept()
        self.active[session_id] = ws

    def disconnect(self, session_id: str):
        self.active.pop(session_id, None)

    async def send(self, session_id: str, data: dict):
        ws = self.active.get(session_id)
        if ws:
            try:
                await ws.send_json(data)
            except Exception:
                self.disconnect(session_id)

manager = ConnectionManager()

# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html") as f:
        return f.read()

@app.get("/api/graphs")
async def list_graphs():
    return {"presets": {k: v["label"] for k, v in GRAPH_PRESETS.items()}}

@app.get("/api/graph/{preset}")
async def get_graph(preset: str, seed: int = 42):
    G = build_graph(preset, seed)
    nodes = [{"id": n, "degree": G.degree(n)} for n in G.nodes()]
    edges = [{"source": u, "target": v} for u, v in G.edges()]
    return {
        "nodes": nodes,
        "edges": edges,
        "node_count": G.number_of_nodes(),
        "edge_count": G.number_of_edges(),
    }

@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    await manager.connect(session_id, websocket)
    try:
        while True:
            data = await websocket.receive_json()
            cmd = data.get("command")

            if cmd == "search":
                preset    = data.get("preset", "medium")
                seed      = data.get("seed", 42)
                num_agents = int(data.get("num_agents", 3))
                algorithm  = data.get("algorithm", "bfs")
                target_node = data.get("target")

                G = build_graph(preset, seed)
                nodes_list = list(G.nodes())
                start_node = nodes_list[0]
                if target_node is None:
                    target_node = nodes_list[-1]
                else:
                    target_node = int(target_node)

                event_queue: asyncio.Queue = asyncio.Queue()

                await manager.send(session_id, {
                    "event": "search_start",
                    "start": start_node,
                    "target": target_node,
                    "graph_size": G.number_of_nodes(),
                    "num_agents": num_agents,
                    "algorithm": algorithm,
                })

                coord = SearchCoordinator(
                    graph=G,
                    num_agents=num_agents,
                    event_queue=event_queue,
                    algorithm=algorithm,
                )

                async def stream_events():
                    while True:
                        try:
                            msg = await asyncio.wait_for(event_queue.get(), timeout=0.1)
                            await manager.send(session_id, {
                                "event": msg.type.value,
                                **msg.to_dict(),
                            })
                            if msg.type == MsgType.SEARCH_DONE:
                                break
                        except asyncio.TimeoutError:
                            continue
                        except Exception:
                            break

                search_task  = asyncio.create_task(coord.search(start_node, target_node))
                stream_task  = asyncio.create_task(stream_events())
                await asyncio.gather(stream_task, return_exceptions=True)
                search_task.cancel()

    except WebSocketDisconnect:
        manager.disconnect(session_id)
    except Exception:
        manager.disconnect(session_id)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
