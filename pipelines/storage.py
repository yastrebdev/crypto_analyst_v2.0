import json
import logging

import boto3

from config import (
    MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD,
    MINIO_BUCKET
)

logger = logging.getLogger(__name__)

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id=MINIO_ROOT_USER,
    aws_secret_access_key=MINIO_ROOT_PASSWORD
)


def init_storage():
    try:
        s3.create_bucket(Bucket=MINIO_BUCKET)
        logger.info(f"Bucket '{MINIO_BUCKET}' created")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        logger.info(f"Bucket '{MINIO_BUCKET}' already exists")


def flush_to_s3(date: str, hour: int, prefix: str, body: list[dict]):
    key = f"raw/{prefix}/date={date}/hour={hour}/raw.json"

    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=json.dumps(body).encode("utf-8")
    )