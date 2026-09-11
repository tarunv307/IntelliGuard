"""
kafka_producer.py — KafkaProducer with retry on NoBrokersAvailable.

The producer is initialised once at startup and reused for all /ingest calls.
Backoff: 2s, 4s, 8s … up to 60s per attempt (max 10 retries).
"""
import json
import logging
import time
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from app.config import KAFKA_BOOTSTRAP, KAFKA_TOPIC

logger = logging.getLogger(__name__)

_producer: Optional[KafkaProducer] = None


def _json_serializer(data: dict) -> bytes:
    return json.dumps(data).encode("utf-8")


def get_producer(max_retries: int = 10) -> KafkaProducer:
    """
    Return (or lazily create) a singleton KafkaProducer.
    Retries on NoBrokersAvailable with exponential backoff.
    """
    global _producer
    if _producer is not None:
        return _producer

    delay = 2
    for attempt in range(1, max_retries + 1):
        try:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=_json_serializer,
                acks="all",
                retries=3,
                request_timeout_ms=10_000,
            )
            logger.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP)
            return _producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not available (attempt %d/%d). Retrying in %ds…",
                attempt, max_retries, delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, 60)

    raise RuntimeError(f"Cannot connect to Kafka after {max_retries} attempts")


def send_row(topic: str, message: dict) -> None:
    """Send a single row message to the topic."""
    producer = get_producer()
    producer.send(topic, value=message)


def flush() -> None:
    """Flush all buffered messages synchronously."""
    if _producer:
        _producer.flush()


def is_connected() -> bool:
    """Quick liveness check — tries to fetch broker metadata."""
    try:
        p = get_producer()
        p.bootstrap_connected()
        return True
    except Exception:
        return False
