"""
main.py — FastAPI application with S3 Storage & Supabase Persistence.

Endpoints:
  POST /ingest                — upload CSV, publish rows to Kafka, store in S3 & Supabase
  GET  /status?job_id=        — poll job progress
  GET  /health                — liveness + Kafka + Neo4j connectivity
  POST /chat                  — grounded chatbot with Supabase audit logging
  POST /internal/progress     — called by loader to update job counters
"""
import csv
import hashlib
import io
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app import chatbot, jobs, kafka_producer, neo4j_client, s3_client, supabase_client
from app.config import KAFKA_TOPIC

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ── Lifespan: warm up connections on startup ──────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting API — warming up Kafka, Neo4j, S3, and Supabase connections…")
    try:
        kafka_producer.get_producer()
    except Exception as exc:
        logger.warning("Kafka warm-up skipped / retry on demand: %s", exc)
    try:
        neo4j_client.get_driver()
    except Exception as exc:
        logger.warning("Neo4j warm-up skipped / retry on demand: %s", exc)
    try:
        s3_client.get_s3_client()
    except Exception as exc:
        logger.warning("S3 warm-up skipped: %s", exc)
    try:
        supabase_client.get_supabase_client()
    except Exception as exc:
        logger.warning("Supabase warm-up skipped: %s", exc)
    yield
    neo4j_client.close()


app = FastAPI(
    title="IntelliGuard API",
    description="CSV → Kafka → Neo4j → Grounded Chatbot with S3 & Supabase",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Global error handlers — always return JSON, never HTML ─────────────────────

@app.exception_handler(Exception)
async def _global_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled exception on %s %s: %s", request.method, request.url.path, exc)
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {exc}"},
    )

@app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
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
    Validates file, saves to S3 & Supabase, publishes rows to Kafka, returns job info.
    """
    raw = await file.read()

    if not raw:
        raise HTTPException(status_code=400, detail="CSV is empty — please upload a non-empty file.")

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File is not valid UTF-8 text — not a CSV.")

    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        raise HTTPException(status_code=400, detail="CSV is empty — please upload a non-empty file.")

    reader = csv.DictReader(io.StringIO(text))
    try:
        header = reader.fieldnames
    except Exception:
        raise HTTPException(status_code=400, detail="Could not parse CSV header.")

    if not header:
        raise HTTPException(status_code=400, detail="CSV has no header row.")

    if len(header) < 1:
        raise HTTPException(status_code=400, detail="Not a valid CSV file.")

    # Collect rows
    rows = []
    for row in reader:
        clean = {k: (v or "") for k, v in row.items() if k is not None}
        rows.append(clean)

    dataset_id = _dataset_id(file.filename or "upload.csv", raw)
    filename = file.filename or "upload.csv"
    rows_received = len(rows)

    # 1. Upload raw CSV to AWS S3 (if configured)
    s3_url = s3_client.upload_csv_to_s3(dataset_id, filename, raw)

    # 2. Record dataset metadata in Supabase (if configured)
    supabase_client.record_dataset(dataset_id, filename, rows_received, s3_url)

    # 3. Create tracking job
    job_id = jobs.create_job(dataset_id, filename, rows_received)

    # 4. Publish rows to Kafka
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
        "Ingested job_id=%s dataset_id=%s filename=%s rows=%d s3_url=%s",
        job_id, dataset_id, filename, rows_received, s3_url,
    )

    if rows_received == 0:
        jobs.record_progress(job_id, loaded=0, failed=0)

    return {
        "job_id": job_id,
        "dataset_id": dataset_id,
        "filename": filename,
        "rows_received": rows_received,
        "s3_url": s3_url,
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
    Return liveness + Kafka + Neo4j connectivity status.
    """
    kafka_ok = kafka_producer.is_connected()
    neo4j_ok = neo4j_client.is_connected()
    s3_configured = s3_client.get_s3_client() is not None
    supabase_configured = supabase_client.get_supabase_client() is not None
    overall = "ok" if (kafka_ok and neo4j_ok) else "degraded"

    return {
        "status": overall,
        "kafka_connected": kafka_ok,
        "neo4j_connected": neo4j_ok,
        "s3_storage_active": s3_configured,
        "supabase_db_active": supabase_configured,
    }


# ── POST /chat ────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    question: str
    dataset_id: str = None


@app.post("/chat")
async def chat(req: ChatRequest):
    """Answer a plain-English question from the graph and log to Supabase."""
    if not req.question or not req.question.strip():
        return {
            "answer": "Please enter a question.",
            "cypher": None,
            "result": [],
            "grounded": False,
        }

    response = chatbot.answer(req.question)

    # Audit log chat query in Supabase
    try:
        supabase_client.log_chat_query(
            question=req.question,
            answer=response.get("answer", ""),
            cypher=response.get("cypher"),
            grounded=response.get("grounded", False),
            dataset_id=req.dataset_id,
        )
    except Exception:
        pass

    return response


# ── POST /internal/progress ───────────────────────────────────────────────────

class ProgressUpdate(BaseModel):
    job_id: str
    loaded: int = 0
    failed: int = 0


@app.post("/internal/progress", include_in_schema=False)
async def internal_progress(update: ProgressUpdate):
    """
    Called by loader service to update job counters.
    """
    result = jobs.record_progress(update.job_id, update.loaded, update.failed)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Job '{update.job_id}' not found.")
    return {"ok": True, "status": result["status"]}


# ── Static UI Mounting for Standalone Render Deployment ────────────────────────
static_dir = os.path.join(os.path.dirname(__file__), "..", "..", "ui", "public")
if os.path.exists(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
