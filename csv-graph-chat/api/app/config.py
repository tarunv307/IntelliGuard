"""
config.py — all configuration from environment variables.
Never hardcode secrets. Import this module everywhere config is needed.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Neo4j Graph Database
NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "")
NEO4J_DATABASE: str = os.getenv("NEO4J_DATABASE", "neo4j")

# Kafka Message Broker
KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "csv-rows")

# AWS S3 Storage
AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
AWS_S3_BUCKET: str = os.getenv("AWS_S3_BUCKET", "")
AWS_S3_ENDPOINT_URL: str = os.getenv("AWS_S3_ENDPOINT_URL", "")

# Supabase Database (PostgreSQL)
SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
SUPABASE_DB_URL: str = os.getenv("SUPABASE_DB_URL", "")

# Validate critical secrets are present in local development
if not NEO4J_PASSWORD and not os.getenv("RENDER"):
    raise RuntimeError("NEO4J_PASSWORD environment variable must be set")
