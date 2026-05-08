# Communicating Agent Network Search

A real-time web app that visualizes autonomous software agents collaboratively
searching network graphs using BFS, A*, or Random Walk strategies.

## Features

- **Live D3.js graph visualization** — watch agents paint the network in real time
- **Multi-agent coordination** — up to 6 agents with gossip protocol & rebalancing
- **3 search algorithms** — BFS, A*, Random Walk
- **6 graph topologies** — Barabási–Albert, Grid, Star, Cycle
- **WebSocket streaming** — live event log with per-agent stats

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
uvicorn main:app --reload --port 8000

# Open http://localhost:8000
```

## Deploy to Render (Free Tier)

### Option A — One-click via render.yaml

1. Push this project to a GitHub or GitLab repository
2. Go to [https://dashboard.render.com](https://dashboard.render.com)
3. Click **New → Blueprint**
4. Connect your repo — Render auto-detects `render.yaml`
5. Click **Apply** — your app deploys in ~2 minutes

### Option B — Manual Web Service

1. Go to [https://dashboard.render.com](https://dashboard.render.com)
2. Click **New → Web Service**
3. Connect your GitHub/GitLab repo
4. Fill in:
   - **Environment**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Choose **Free** plan
6. Click **Create Web Service**

Your app will be live at `https://agent-network-search.onrender.com` (or similar).

## Project Structure

```
agent-search-app/
├── main.py            # FastAPI app + agent logic
├── templates/
│   └── index.html     # Full frontend (D3 + vanilla JS)
├── requirements.txt
├── render.yaml        # Render Blueprint config
└── README.md
```

## Architecture

```
Browser (D3 vis) ←── WebSocket ──→ FastAPI
                                      │
                              SearchCoordinator
                             /        |        \
                        Agent-0   Agent-1   Agent-N
                             \        |        /
                              NetworkX Graph
```

Messages flow: ASSIGN_FRONTIER → NODE_VISITED → GOSSIP_STATE → PATH_FOUND → SEARCH_DONE

## Tech Stack

- **Backend**: Python 3.11, FastAPI, asyncio, NetworkX
- **Frontend**: Vanilla JS, D3.js v7, Space Mono + Syne fonts
- **Realtime**: WebSockets
- **Deploy**: Render (free tier, auto-deploys on git push)
