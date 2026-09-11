"""
loader.py — Kafka consumer that MERGEs CSV rows into Neo4j.

Idempotency:
  - Dataset MERGE key: d.id = dataset_id
  - Row MERGE key:     r.key = "{dataset_id}:{row_index}"
  → Re-consuming the same Kafka topic produces identical graph (no duplicates).

Progress reporting:
  - On each successful MERGE   → POST /internal/progress {loaded: 1}
  - On each failed MERGE       → POST /internal/progress {failed: 1}
  - Never silently swallows exceptions.

Startup:
  - Retries on NoBrokersAvailable with exponential backoff.
  - Retries on Neo4j ServiceUnavailable similarly.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s loader: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config from env ───────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "csv-rows")
NEO4J_URI       = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER      = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD  = os.getenv("NEO4J_PASSWORD", "")
NEO4J_DATABASE  = os.getenv("NEO4J_DATABASE", "neo4j")
API_URL         = os.getenv("API_INTERNAL_URL", "http://api:8000")

if not NEO4J_PASSWORD:
    raise RuntimeError("NEO4J_PASSWORD must be set")

# ── Cypher ────────────────────────────────────────────────────────────────────

MERGE_CYPHER = """
MERGE (d:Dataset {id: $dataset_id})
  ON CREATE SET d.filename    = $filename,
                d.uploaded_at = datetime()
MERGE (r:Row {key: $row_key})
  SET r.row_index  = $row_index,
      r.dataset_id = $dataset_id
SET r += $columns
MERGE (d)-[:HAS_ROW]->(r)
"""


# ── Neo4j driver with retry ───────────────────────────────────────────────────

def connect_neo4j(max_retries: int = 15) -> "neo4j.Driver":
    delay = 2
    for attempt in range(1, max_retries + 1):
        try:
            driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=(NEO4J_USER, NEO4J_PASSWORD),
                connection_timeout=5,
            )
            driver.verify_connectivity()
            logger.info("Connected to Neo4j at %s", NEO4J_URI)
            return driver
        except (ServiceUnavailable, Exception) as exc:
            logger.warning("Neo4j not ready (attempt %d/%d): %s — retrying in %ds", attempt, max_retries, exc, delay)
            time.sleep(delay)
            delay = min(delay * 2, 60)
    raise RuntimeError(f"Cannot connect to Neo4j after {max_retries} attempts")


# ── Kafka consumer with retry ─────────────────────────────────────────────────

def connect_consumer(max_retries: int = 15) -> KafkaConsumer:
    delay = 2
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="csv-loader-group",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=-1,  # block forever, no timeout
                session_timeout_ms=30_000,
                heartbeat_interval_ms=10_000,
            )
            logger.info("Kafka consumer connected to %s, topic=%s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
            return consumer
        except NoBrokersAvailable:
            logger.warning("Kafka not available (attempt %d/%d) — retrying in %ds", attempt, max_retries, delay)
            time.sleep(delay)
            delay = min(delay * 2, 60)
    raise RuntimeError(f"Cannot connect to Kafka after {max_retries} attempts")


# ── Progress reporting ────────────────────────────────────────────────────────

def report_progress(job_id: str, loaded: int = 0, failed: int = 0) -> None:
    """
    POST progress update to the API's internal endpoint.
    Fire-and-forget with best-effort retry (3 attempts).
    """
    url = f"{API_URL}/internal/progress"
    payload = {"job_id": job_id, "loaded": loaded, "failed": failed}
    for attempt in range(3):
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.ok:
                return
            logger.warning("Progress report non-OK status %d for job %s", resp.status_code, job_id)
            return
        except requests.RequestException as exc:
            logger.warning("Progress report failed (attempt %d): %s", attempt + 1, exc)
            time.sleep(1)


# ── Row processing ────────────────────────────────────────────────────────────

def process_message(driver, msg: dict) -> None:
    """
    MERGE a single CSV row into Neo4j.
    Raises on failure so the caller can report it.
    """
    dataset_id = msg["dataset_id"]
    filename   = msg.get("filename", "unknown.csv")
    row_index  = msg["row_index"]
    columns    = msg.get("columns", {})
    job_id     = msg.get("job_id", "unknown")

    row_key = f"{dataset_id}:{row_index}"

    with driver.session(database=NEO4J_DATABASE) as session:
        session.run(
            MERGE_CYPHER,
            dataset_id=dataset_id,
            filename=filename,
            row_key=row_key,
            row_index=row_index,
            columns=columns,
        )

    logger.debug("Merged row key=%s job=%s", row_key, job_id)
    report_progress(job_id, loaded=1)


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("Loader starting up…")

    driver   = connect_neo4j()
    consumer = connect_consumer()

    logger.info("Loader ready — consuming topic '%s'", KAFKA_TOPIC)

    for message in consumer:
        msg = message.value
        job_id = msg.get("job_id", "unknown")

        try:
            process_message(driver, msg)
        except Exception as exc:
            logger.error(
                "Failed to merge row job=%s row_index=%s: %s",
                job_id, msg.get("row_index"), exc,
            )
            report_progress(job_id, failed=1)


if __name__ == "__main__":
    main()
