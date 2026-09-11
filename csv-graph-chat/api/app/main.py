"""
main.py — FastAPI application.

Endpoints:
  POST /ingest                — upload CSV, publish rows to Kafka
  GET  /status?job_id=        — poll job progress
  GET  /health                — liveness + Kafka + Neo4j connectivity
  POST /chat                  — grounded chatbot
  POST /internal/progress     — called by loader to update job counters
"""
import csv
import hashlib
import io
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app import chatbot, jobs, kafka_producer, neo4j_client
from app.config import KAFKA_TOPIC

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ── Lifespan: warm up connections on startup ──────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting API — warming up Kafka + Neo4j connections…")
    try:
        kafka_producer.get_producer()
    except Exception as exc:
        logger.error("Kafka warm-up failed (will retry on first request): %s", exc)
    try:
        neo4j_client.get_driver()
    except Exception as exc:
        logger.error("Neo4j warm-up failed (will retry on first request): %s", exc)
    yield
    neo4j_client.close()


app = FastAPI(
    title="CSV Graph Chat API",
    description="Upload CSV → Kafka → Neo4j → Grounded Chatbot",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Helper ────────────────────────────────────────────────────────────────────

def _dataset_id(filename: str, content: bytes) -> str:
    """Stable 12-char ID = sha256(filename + content)[:12]."""
    h = hashlib.sha256((filename + content.decode("utf-8", errors="replace")).encode()).hexdigest()
    return h[:12]


# ── POST /ingest ──────────────────────────────────────────────────────────────

@app.post("/ingest", status_code=202)
async def ingest(file: UploadFile = File(...)):
    """
    Accept a multipart CSV upload.
    Validates the file, publishes one Kafka message per row, returns job info.
    """
    # Read raw bytes
    raw = await file.read()

    # Empty file
    if not raw:
        raise HTTPException(status_code=400, detail="CSV is empty — please upload a non-empty file.")

    # Try decoding
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File is not valid UTF-8 text — not a CSV.")

    # Must have at least one line
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        raise HTTPException(status_code=400, detail="CSV is empty — please upload a non-empty file.")

    # Parse with DictReader
    reader = csv.DictReader(io.StringIO(text))
    try:
        header = reader.fieldnames
    except Exception:
        raise HTTPException(status_code=400, detail="Could not parse CSV header.")

    if not header:
        raise HTTPException(status_code=400, detail="CSV has no header row.")

    # Validate it looks like a CSV (has at least one comma or 2+ columns)
    if len(header) < 1:
        raise HTTPException(status_code=400, detail="Not a valid CSV file.")

    # Collect rows
    rows = []
    for row in reader:
        # Pad/trim ragged rows gracefully
        clean = {k: (v or "") for k, v in row.items() if k is not None}
        rows.append(clean)

    # Generate IDs
    dataset_id = _dataset_id(file.filename or "upload.csv", raw)
    filename = file.filename or "upload.csv"
    rows_received = len(rows)

    # Create job
    job_id = jobs.create_job(dataset_id, filename, rows_received)

    # Publish rows to Kafka (header-only CSV: 0 rows, still valid)
    for idx, columns in enumerate(rows):
        msg = {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "filename": filename,
            "row_index": idx,
            "columns": columns,
        }
        kafka_producer.send_row(KAFKA_TOPIC, msg)

    kafka_producer.flush()

    logger.info(
        "Ingested job_id=%s dataset_id=%s filename=%s rows=%d",
        job_id, dataset_id, filename, rows_received,
    )

    # Header-only → immediately mark complete
    if rows_received == 0:
        jobs.record_progress(job_id, loaded=0, failed=0)

    return {
        "job_id": job_id,
        "dataset_id": dataset_id,
        "filename": filename,
        "rows_received": rows_received,
        "status": "queued",
    }


# ── GET /status ───────────────────────────────────────────────────────────────

@app.get("/status")
async def status(job_id: str = Query(..., description="Job ID returned by /ingest")):
    """Return current job status and row counts."""
    job = jobs.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return job


# ── GET /health ───────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """
    Return ok only when BOTH Kafka and Neo4j are genuinely reachable.
    Returns 200 always — the caller reads the status field.
    """
    kafka_ok = kafka_producer.is_connected()
    neo4j_ok = neo4j_client.is_connected()
    overall = "ok" if (kafka_ok and neo4j_ok) else "degraded"

    return {
        "status": overall,
        "kafka_connected": kafka_ok,
        "neo4j_connected": neo4j_ok,
    }


# ── POST /chat ────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    question: str


@app.post("/chat")
async def chat(req: ChatRequest):
    """Answer a plain-English question from the graph (grounded only)."""
    if not req.question or not req.question.strip():
        return {
            "answer": "Please enter a question.",
            "cypher": None,
            "result": [],
            "grounded": False,
        }
    return chatbot.answer(req.question)


# ── POST /internal/progress ───────────────────────────────────────────────────

class ProgressUpdate(BaseModel):
    job_id: str
    loaded: int = 0
    failed: int = 0


@app.post("/internal/progress", include_in_schema=False)
async def internal_progress(update: ProgressUpdate):
    """
    Called by the loader service to update job counters.
    Not exposed in public API docs.
    """
    result = jobs.record_progress(update.job_id, update.loaded, update.failed)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Job '{update.job_id}' not found.")
    return {"ok": True, "status": result["status"]}
