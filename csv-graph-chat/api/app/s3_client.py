"""
s3_client.py — AWS S3 client for storing uploaded raw CSV datasets.

Provides:
  - upload_csv_to_s3(dataset_id, filename, content) -> Optional[str] (S3 URI or URL)
  - generate_presigned_url(s3_key) -> Optional[str]
  - Graceful fallback when AWS credentials are not configured.
"""
import io
import logging
import os
from typing import Optional
import boto3
from botocore.exceptions import ClientError

from app.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    AWS_S3_BUCKET,
    AWS_S3_ENDPOINT_URL,
)

logger = logging.getLogger(__name__)

_s3_client = None


def get_s3_client():
    global _s3_client
    if _s3_client is not None:
        return _s3_client

    if not AWS_S3_BUCKET or not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        logger.info("S3 storage not fully configured — running in local mode.")
        return None

    try:
        kwargs = {
            "service_name": "s3",
            "region_name": AWS_REGION,
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        }
        if AWS_S3_ENDPOINT_URL:
            kwargs["endpoint_url"] = AWS_S3_ENDPOINT_URL

        _s3_client = boto3.client(**kwargs)
        logger.info("Connected to AWS S3 bucket: %s", AWS_S3_BUCKET)
        return _s3_client
    except Exception as exc:
        logger.error("Failed to initialize AWS S3 client: %s", exc)
        return None


def upload_csv_to_s3(dataset_id: str, filename: str, content: bytes) -> Optional[str]:
    """
    Upload CSV content to S3 bucket under `datasets/{dataset_id}/{filename}`.
    Returns s3:// URI or https URL if successful, or None if S3 is disabled.
    """
    client = get_s3_client()
    if client is None or not AWS_S3_BUCKET:
        return None

    s3_key = f"datasets/{dataset_id}/{filename}"
    try:
        client.upload_fileobj(
            io.BytesIO(content),
            AWS_S3_BUCKET,
            s3_key,
            ExtraArgs={"ContentType": "text/csv"},
        )
        s3_uri = f"s3://{AWS_S3_BUCKET}/{s3_key}"
        logger.info("Uploaded CSV to S3: %s", s3_uri)
        return s3_uri
    except ClientError as exc:
        logger.error("Failed to upload CSV to S3 (%s): %s", s3_key, exc)
        return None


def generate_presigned_url(s3_key: str, expiration_seconds: int = 3600) -> Optional[str]:
    """Generate a pre-signed download URL for a file in S3."""
    client = get_s3_client()
    if client is None or not AWS_S3_BUCKET:
        return None

    try:
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": AWS_S3_BUCKET, "Key": s3_key},
            ExpiresIn=expiration_seconds,
        )
        return url
    except ClientError as exc:
        logger.error("Failed to generate pre-signed URL for %s: %s", s3_key, exc)
        return None
