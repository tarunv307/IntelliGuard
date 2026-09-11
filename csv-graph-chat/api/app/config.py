"""
config.py — all configuration from environment variables only.
Never hardcode secrets. Import this module everywhere config is needed.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Neo4j
NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "")
NEO4J_DATABASE: str = os.getenv("NEO4J_DATABASE", "neo4j")

# Kafka
KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "csv-rows")

# Validate critical secrets are present
if not NEO4J_PASSWORD:
    raise RuntimeError("NEO4J_PASSWORD environment variable must be set")
