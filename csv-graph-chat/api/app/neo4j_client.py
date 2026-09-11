"""
neo4j_client.py — Neo4j driver wrapper with retry on ServiceUnavailable.

The driver is initialised once at startup. All Cypher queries go through
run_query() which returns a list of record dicts.
"""
import logging
import time
from typing import Any, Optional

from neo4j import GraphDatabase, Driver
from neo4j.exceptions import ServiceUnavailable, AuthError

from app.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE

logger = logging.getLogger(__name__)

_driver: Optional[Driver] = None


def get_driver(max_retries: int = 15) -> Driver:
    """
    Return (or lazily create) a singleton Neo4j driver.
    Retries verify_connectivity() on ServiceUnavailable.
    """
    global _driver
    if _driver is not None:
        return _driver

    delay = 2
    for attempt in range(1, max_retries + 1):
        try:
            driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=(NEO4J_USER, NEO4J_PASSWORD),
                connection_timeout=5,
                max_connection_lifetime=300,
            )
            driver.verify_connectivity()
            _driver = driver
            logger.info("Neo4j driver connected to %s / %s", NEO4J_URI, NEO4J_DATABASE)
            return _driver
        except (ServiceUnavailable, AuthError) as exc:
            logger.warning(
                "Neo4j not ready (attempt %d/%d): %s. Retrying in %ds…",
                attempt, max_retries, exc, delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, 60)

    raise RuntimeError(f"Cannot connect to Neo4j after {max_retries} attempts")


def run_query(cypher: str, params: Optional[dict] = None) -> list[dict[str, Any]]:
    """
    Execute a Cypher query and return results as a list of plain dicts.
    Raises on connection failure — callers handle errors.
    """
    driver = get_driver()
    params = params or {}
    with driver.session(database=NEO4J_DATABASE) as session:
        result = session.run(cypher, **params)
        return [dict(record) for record in result]


def is_connected() -> bool:
    """Quick liveness check — verify connectivity."""
    try:
        get_driver().verify_connectivity()
        return True
    except Exception:
        return False


def close() -> None:
    """Gracefully close the driver on shutdown."""
    global _driver
    if _driver:
        _driver.close()
        _driver = None
