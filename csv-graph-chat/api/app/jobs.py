"""
jobs.py — in-memory job store with thread-safe locking.

State machine:  queued → loading → complete | failed

complete is set ONLY when rows_loaded + rows_failed == rows_total,
ensuring the chatbot sees consistent totals.

Note: get_all_dataset_ids() falls back to Neo4j so it survives container
restarts (in-memory state is empty after restart but graph data persists).
"""
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional


_lock = threading.Lock()
_jobs: dict[str, dict] = {}


def create_job(dataset_id: str, filename: str, rows_total: int) -> str:
    """Create a new job and return its job_id."""
    job_id = str(uuid.uuid4())[:8]
    with _lock:
        _jobs[job_id] = {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "filename": filename,
            "status": "queued",
            "rows_total": rows_total,
            "rows_loaded": 0,
            "rows_failed": 0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    return job_id


def get_job(job_id: str) -> Optional[dict]:
    """Return job dict or None if not found."""
    with _lock:
        return dict(_jobs[job_id]) if job_id in _jobs else None


def record_progress(job_id: str, loaded: int = 0, failed: int = 0) -> Optional[dict]:
    """
    Increment rows_loaded / rows_failed counters.
    Transitions status:
      - queued → loading on first progress call
      - loading → complete when loaded + failed == total
      - loading → failed when failed > 0 and all rows accounted for
    Returns updated job dict, or None if job not found.
    """
    with _lock:
        if job_id not in _jobs:
            return None
        job = _jobs[job_id]
        job["rows_loaded"] += loaded
        job["rows_failed"] += failed
        job["updated_at"] = datetime.now(timezone.utc).isoformat()

        # Status transitions
        accounted = job["rows_loaded"] + job["rows_failed"]
        total = job["rows_total"]

        if job["status"] == "queued" and accounted > 0:
            job["status"] = "loading"

        if total == 0 or accounted >= total:
            # All rows accounted for
            if job["rows_failed"] > 0 and job["rows_loaded"] == 0:
                job["status"] = "failed"
            else:
                job["status"] = "complete"

        return dict(job)


def get_all_dataset_ids() -> list[str]:
    """
    Return all dataset IDs currently tracked in memory.
    Falls back to querying Neo4j directly so the chatbot works even
    after an API container restart (in-memory state is lost but graph persists).
    """
    with _lock:
        ids = [j["dataset_id"] for j in _jobs.values()]
    if ids:
        return ids
    # Fallback: check whether ANY Row nodes exist in the graph
    try:
        from app import neo4j_client  # local import to avoid circular
        result = neo4j_client.run_query(
            "MATCH (r:Row) RETURN r.dataset_id AS dataset_id LIMIT 1", {}
        )
        if result:
            ds_id = result[0].get("dataset_id")
            return [ds_id] if ds_id else ["__unknown__"]
    except Exception:
        pass
    return []
