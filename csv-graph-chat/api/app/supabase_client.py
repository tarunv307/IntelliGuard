"""
supabase_client.py — Supabase client for persistent metadata, jobs, and chat audit logging.

Provides:
  - record_dataset(dataset_id, filename, row_count, s3_url)
  - upsert_job(job_id, dataset_id, filename, rows_total, rows_loaded, rows_failed, status)
  - get_job(job_id) -> Optional[dict]
  - log_chat(question, answer, cypher, grounded, dataset_id)
  - Graceful fallback when Supabase credentials are not configured.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

from supabase import create_client, Client

from app.config import SUPABASE_URL, SUPABASE_KEY

logger = logging.getLogger(__name__)

_supabase_client: Optional[Client] = None


def get_supabase_client() -> Optional[Client]:
    global _supabase_client
    if _supabase_client is not None:
        return _supabase_client

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.info("Supabase credentials not set — using local in-memory/graph persistence.")
        return None

    try:
        _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Connected to Supabase at %s", SUPABASE_URL)
        return _supabase_client
    except Exception as exc:
        logger.error("Failed to connect to Supabase: %s", exc)
        return None


def record_dataset(dataset_id: str, filename: str, row_count: int, s3_url: Optional[str] = None) -> bool:
    """Save dataset metadata to Supabase 'datasets' table."""
    client = get_supabase_client()
    if client is None:
        return False

    try:
        data = {
            "id": dataset_id,
            "filename": filename,
            "row_count": row_count,
            "s3_url": s3_url,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        client.table("datasets").upsert(data).execute()
        return True
    except Exception as exc:
        logger.warning("Failed to record dataset in Supabase: %s", exc)
        return False


def upsert_job(
    job_id: str,
    dataset_id: str,
    filename: str,
    rows_total: int,
    rows_loaded: int = 0,
    rows_failed: int = 0,
    status: str = "queued",
) -> bool:
    """Save or update job state in Supabase 'jobs' table."""
    client = get_supabase_client()
    if client is None:
        return False

    try:
        data = {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "filename": filename,
            "rows_total": rows_total,
            "rows_loaded": rows_loaded,
            "rows_failed": rows_failed,
            "status": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        client.table("jobs").upsert(data).execute()
        return True
    except Exception as exc:
        logger.warning("Failed to update job in Supabase: %s", exc)
        return False


def get_job_from_db(job_id: str) -> Optional[dict]:
    """Fetch job state from Supabase 'jobs' table."""
    client = get_supabase_client()
    if client is None:
        return None

    try:
        res = client.table("jobs").select("*").eq("job_id", job_id).limit(1).execute()
        if res.data:
            return res.data[0]
    except Exception as exc:
        logger.warning("Failed to read job from Supabase: %s", exc)
    return None


def log_chat_query(
    question: str,
    answer: str,
    cypher: Optional[str] = None,
    grounded: bool = True,
    dataset_id: Optional[str] = None,
) -> bool:
    """Audit log chat questions and answers to Supabase 'chat_logs' table."""
    client = get_supabase_client()
    if client is None:
        return False

    try:
        data = {
            "question": question,
            "answer": answer,
            "cypher": cypher,
            "grounded": grounded,
            "dataset_id": dataset_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        client.table("chat_logs").insert(data).execute()
        return True
    except Exception as exc:
        logger.warning("Failed to log chat query to Supabase: %s", exc)
        return False
