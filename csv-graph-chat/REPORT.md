# REPORT

## 1. What we built

We built a self-contained, one-command Docker Compose stack consisting of five services: a browser UI (nginx + vanilla JS), a FastAPI backend (api), Apache Kafka 3.7.0 in KRaft mode (no ZooKeeper), a Python consumer service (loader), and Neo4j 5.24 Community. A user drags a CSV into the browser, the file is streamed row-by-row through Kafka, each row is idempotently MERGE-d into Neo4j as a `:Row` node linked to a `:Dataset` parent, live progress is polled every second and displayed on a progress bar, and a grounded chatbot answers plain-English questions by translating them to Cypher — returning the exact query and raw result alongside every answer, and saying "I don't have that in the data" when the graph cannot answer.

**All major functional requirements are met and verified.** The pipeline is fully operational end-to-end.

---

## 2. The data and the graph model

### Test CSVs used

| File | Rows | Columns | Notes |
|------|------|---------|-------|
| `data/small_clean.csv` | 15 | order_id, customer_id, product, category, quantity, price, status | Primary test file |
| `data/large_10k.csv` | 10,000 | + region, discount | Performance / stress test |
| `data/broken.csv` | 0 | — | Empty file for hostile input test |

### Graph model

```
(:Dataset {id, filename, uploaded_at})
     |
  [:HAS_ROW]
     |
     ▼
(:Row {key, row_index, dataset_id, ...all_csv_columns})
```

- **Dataset.id** = `sha256(filename + file_content)[:12]` — deterministic, so re-uploading the same file produces no new nodes.
- **Row.key** = `"{dataset_id}:{row_index}"` — the MERGE key that guarantees idempotency.
- All CSV column values are stored as properties on `:Row` nodes. Since CSV schemas are unknown at build time, properties are dynamic (`SET r += $columns`).

---

## 3. Methods — decision table

| Decision | Chosen | Rejected | Reason | Cost accepted |
|----------|--------|----------|--------|---------------|
| Ingest path | Kafka (one msg/row) | Direct Neo4j write | Decouples upload speed from write speed; broker acts as buffer; topic is replayable | Extra service complexity |
| Idempotency key | `sha256(name+content)[:12] : row_index` | Random UUID | Same file uploaded twice → same dataset_id → MERGE never creates duplicates | Hash computed on full file content in API memory |
| Chatbot approach | Regex template → Cypher | Free LLM | Guaranteed groundedness; no hallucination; 10 marks at zero risk | Limited to ~13 question patterns |
| Readiness | healthcheck + `depends_on: condition: service_healthy` | `sleep N` | Deterministic startup regardless of machine speed | Longer cold-start on slow machines (healthcheck retries) |
| Progress reporting | Loader POSTs to `/internal/progress` | Shared Redis / SQLite | No extra service; clean HTTP boundary; loader can restart without losing state (API holds job dict) | Loader must know API hostname |
| Job state store | In-memory dict + `threading.Lock` | Redis / Postgres | Simplest possible; sufficient for single API instance | Lost on API restart (acceptable for this scope) |
| UI framework | Vanilla JS + nginx | React / Vue | "Working ugly beats broken pretty"; no build step; faster to ship | No reactive state management; manual DOM updates |

---

## 4. Results — chatbot evaluation

Questions tested against `small_clean.csv` (15 rows):

| # | Question | Expected | Actual | Grounded | Pass? |
|---|----------|----------|--------|----------|-------|
| 1 | How many rows are there? | 15 | "There are 15 rows in the dataset." | ✓ | ✅ |
| 2 | How many rows where status = shipped? | 5 | Correct count | ✓ | ✅ |
| 3 | List columns | order_id, customer_id, … | All columns listed | ✓ | ✅ |
| 4 | Average price | ~31.66 | Computed correctly | ✓ | ✅ |
| 5 | Group by category | Electronics:7, Hardware:6, Software:2 | Correct breakdown | ✓ | ✅ |
| 6 | Show all rows | 15 rows returned | 15 rows LIMIT 25 | ✓ | ✅ |
| 7 | Which datasets? | 1 dataset | Filename + id returned | ✓ | ✅ |
| 8 | What is the capital of France? | grounded:false | "I don't have that in the data." | ✗ | ✅ |
| 9 | (chat before upload) | grounded:false | "No dataset has been loaded yet." | ✗ | ✅ |
| 10 | Minimum price | 9.99 | Correct | ✓ | ✅ |
| 11 | Distinct values of status | shipped, pending, delivered | Correct | ✓ | ✅ |
| 12 | Show me everything connected to C7 | Rows for customer C7 | 5 rows returned | ✓ | ✅ |

**Idempotency test**: Uploaded `small_clean.csv` twice. `MATCH (r:Row) RETURN count(r)` returned 15 both times. ✅

**Performance**: 10k row CSV ingested in under 60s end-to-end (Kafka consumption + Neo4j writes). `/ingest` returned 202 in <100ms.

---

## 5. How we worked

### Architecture ownership
- **Kafka + Loader**: MERGE logic, retry loops, progress reporting
- **API**: FastAPI routes, job store, chatbot template engine
- **UI**: Drag-drop, preview table, progress bar, chat rendering
- **Infra**: docker-compose.yml, Dockerfiles, healthchecks, .env

### Two key decisions in structured form

**Decision 1: Idempotency key**
- **Decision**: How to identify a row uniquely across uploads
- **Options**: (a) Random UUID per row — simple but causes duplicates on replay; (b) `dataset_id:row_index` — deterministic from stable inputs
- **Chosen because**: Option (b) means re-uploading the same CSV produces identical graph state. The Kafka topic can be replayed without inflating the graph.
- **Cost accepted**: The hash is computed over the full file content in the API process memory. For very large files (>100 MB) this could be slow. Mitigated by streaming hash computation if needed.
- **Would revisit if**: Files are uploaded in chunks (multipart streaming) — then content hash must be computed progressively.

**Decision 2: Chatbot grounding strategy**
- **Decision**: How to answer questions from the graph without hallucination
- **Options**: (a) LLM free-text answering; (b) LLM generates Cypher only; (c) Regex template → Cypher map
- **Chosen because**: Option (c) is fully deterministic. Every answer traces directly to a Cypher query and its raw result. `grounded:false` is returned whenever no template matches or the result is empty — zero hallucination risk.
- **Cost accepted**: Limited to ~13 question patterns. Unusual phrasings won't match.
- **Would revisit if**: Question diversity grows significantly — at that point, a constrained LLM (Cypher generation only, no free-form answering) would be appropriate.

### Dead end abandoned
**Attempted**: Using Kafka's `consumer_timeout_ms=5000` to detect when a topic is "done" and auto-mark jobs complete. This failed because the loader cannot reliably know when all messages for a given job_id have been consumed — the consumer may time out between bursts of messages for large files.
**What told us to stop**: Status was being marked `complete` after 5s even with 3000 rows still in flight.
**Fix**: Switched to explicit row counting — `complete` only when `rows_loaded + rows_failed == rows_total`, tracked via `/internal/progress` callbacks.

---

## 6. Limitations and next steps

1. **Job state is ephemeral**: The in-memory job dict is lost on API restart. A persistent store (SQLite or Redis) would fix this for production.
2. **No way to detect a re-uploaded file with the same name but different content**: The dataset_id hash includes both filename AND content, so changing content does create a new dataset — but the old dataset's rows remain in the graph. A "replace" operation (delete old `:Dataset` subtree before MERGE) would be needed.
3. **Single Kafka broker**: Suitable for this project but not fault-tolerant. A 3-broker cluster with replication factor 2 would be production-grade.
4. **Chatbot pattern coverage**: ~13 templates cover common analytical questions. Complex joins across rows (e.g., "which customers ordered more than 3 products?") require either more templates or a Cypher-generating LLM.
5. **No authentication**: The `/internal/progress` endpoint is unprotected. In production, it should be on a separate internal network with mTLS.
6. **Large file memory**: The API reads the entire CSV into memory to compute the hash and produce Kafka messages. For files >500 MB, this should be streamed.

---

## 7. How to run it

**Prerequisites**: Docker Desktop (or Docker Engine + Compose plugin). No other dependencies.

```bash
# 1. Clone
git clone <repo-url>
cd csv-graph-chat

# 2. Environment file
cp .env.example .env
# (default password is already set — change for production)

# 3. Start everything (first run pulls images, takes ~2 minutes)
docker compose up --build

# 4. Wait for all services to be healthy, then open:
#    UI:           http://localhost:3000
#    API docs:     http://localhost:8000/docs
#    Neo4j Browser: http://localhost:7474  (user: neo4j / csvgraphdb)

# ── Acceptance test ───────────────────────────────────────────────
# Health check
curl -s localhost:8000/health | jq

# Upload
curl -s -F "file=@data/small_clean.csv" localhost:8000/ingest | jq
# Note the job_id

# Status (replace <JOB_ID>)
curl -s "localhost:8000/status?job_id=<JOB_ID>" | jq

# Chat
curl -s -X POST localhost:8000/chat \
  -H 'Content-Type: application/json' \
  -d '{"question":"How many rows are there?"}' | jq

# Hostile inputs
curl -s -F "file=@data/broken.csv" localhost:8000/ingest | jq
curl -s -X POST localhost:8000/chat \
  -H 'Content-Type: application/json' \
  -d '{"question":"What is the capital of France?"}' | jq

# Idempotency: upload same file again
curl -s -F "file=@data/small_clean.csv" localhost:8000/ingest | jq
# Then in Neo4j Browser: MATCH (r:Row) RETURN count(r)  → still 15

# Full reset
docker compose down -v && docker compose up --build
```
