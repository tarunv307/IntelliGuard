# CSV Graph Chat — README

## One-command startup

```bash
# 1. Clone the repo
git clone <repo-url> && cd csv-graph-chat

# 2. Copy env file (password already set for local dev)
cp .env.example .env

# 3. Build and start everything
docker compose up --build

# 4. Open the UI
open http://localhost:3000
```

**That's it.** Zero manual steps.

---

## Services

| Service | Port | Description |
|---------|------|-------------|
| **ui** | 3000 | Browser UI — drag-drop upload, progress, chat |
| **api** | 8000 | FastAPI — /ingest /status /health /chat |
| **kafka** | 9092 | Apache Kafka 3.7.0 (KRaft, no ZooKeeper) |
| **neo4j** | 7474 / 7687 | Neo4j 5.24 Community — Neo4j Browser at :7474 |
| **loader** | — | Kafka consumer → Neo4j MERGE (internal) |

---

## Quick test

```bash
# Health
curl -s localhost:8000/health | jq

# Upload CSV
curl -s -F "file=@data/small_clean.csv" localhost:8000/ingest | jq

# Poll status (replace JOB_ID)
curl -s "localhost:8000/status?job_id=JOB_ID" | jq

# Chat
curl -s -X POST localhost:8000/chat \
  -H 'Content-Type: application/json' \
  -d '{"question":"How many rows are there?"}' | jq

# Verify in Neo4j Browser → http://localhost:7474
# Login: neo4j / csvgraphdb
# Run: MATCH (r:Row) RETURN r LIMIT 25
```

---

## Architecture

```
UI → POST /ingest → API → Kafka topic:csv-rows → Loader → Neo4j MERGE
UI ← poll /status ← API ← POST /internal/progress ← Loader
UI → POST /chat   → API → Cypher → Neo4j → grounded answer
```

## Environment variables

See `.env.example`. Neo4j password is set via `NEO4J_PASSWORD` — never baked into images.

## Stopping / resetting

```bash
# Stop and remove volumes (full reset)
docker compose down -v

# Stop but keep data
docker compose down
```
