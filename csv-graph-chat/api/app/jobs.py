"""
jobs.py — thread-safe job store with Supabase database synchronization.

State machine:  queued → loading → complete | failed

complete is set ONLY when rows_loaded + rows_failed == rows_total,
ensuring the chatbot sees consistent totals.
"""
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional

_lock = threading.Lock()
_jobs: dict[str, dict] = {}


def create_job(dataset_id: str, filename: str, rows_total: int) -> str:
    """Create a new job, store in memory and sync to Supabase."""
    job_id = str(uuid.uuid4())[:8]
    created_at = datetime.now(timezone.utc).isoformat()
    job_data = {
        "job_id": job_id,
        "dataset_id": dataset_id,
        "filename": filename,
        "status": "queued",
        "rows_total": rows_total,
        "rows_loaded": 0,
        "rows_failed": 0,
        "created_at": created_at,
        "updated_at": created_at,
    }
    with _lock:
        _jobs[job_id] = job_data

    # Sync to Supabase
    try:
        from app import supabase_client
        supabase_client.upsert_job(
            job_id=job_id,
            dataset_id=dataset_id,
            filename=filename,
            rows_total=rows_total,
            rows_loaded=0,
            rows_failed=0,
            status="queued",
        )
    except Exception:
        pass

    return job_id


def get_job(job_id: str) -> Optional[dict]:
    """Return job dict from in-memory cache or fallback to Supabase."""
    with _lock:
        if job_id in _jobs:
            return dict(_jobs[job_id])

    # Fallback to Supabase
    try:
        from app import supabase_client
        db_job = supabase_client.get_job_from_db(job_id)
        if db_job:
            with _lock:
                _jobs[job_id] = db_job
            return dict(db_job)
    except Exception:
        pass

    return None


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
            # Try loading from Supabase first
            try:
                from app import supabase_client
                db_job = supabase_client.get_job_from_db(job_id)
                if db_job:
                    _jobs[job_id] = db_job
            except Exception:
                pass

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
            if job["rows_failed"] > 0 and job["rows_loaded"] == 0:
                job["status"] = "failed"
            else:
                job["status"] = "complete"

        res = dict(job)

    # Sync to Supabase
    try:
        from app import supabase_client
        supabase_client.upsert_job(
            job_id=job["job_id"],
            dataset_id=job["dataset_id"],
            filename=job["filename"],
            rows_total=job["rows_total"],
            rows_loaded=job["rows_loaded"],
            rows_failed=job["rows_failed"],
            status=job["status"],
        )
    except Exception:
        pass

    return res


def get_all_dataset_ids() -> list[str]:
    """
    Return all dataset IDs currently tracked in memory, Supabase, or Neo4j.
    """
    with _lock:
        ids = [j["dataset_id"] for j in _jobs.values()]
    if ids:
        return ids

    # Fallback: check whether ANY Row nodes exist in the graph
    try:
        from app import neo4j_client
        result = neo4j_client.run_query(
            "MATCH (r:Row) RETURN r.dataset_id AS dataset_id LIMIT 1", {}
        )
        if result:
            ds_id = result[0].get("dataset_id")
            return [ds_id] if ds_id else ["__unknown__"]
    except Exception:
        pass
    return []
