from __future__ import annotations

import os
from typing import Tuple
import boto3


def _env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Env var obrigatória não configurada: {name}")
    return v


def s3_client():
    endpoint = _env("S3_ENDPOINT")
    access_key = _env("S3_ACCESS_KEY_ID")
    secret_key = _env("S3_SECRET_ACCESS_KEY")
    region = os.getenv("S3_REGION", "auto")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


def upload_bytes(*, bucket: str, key: str, data: bytes, content_type: str) -> None:
    s3 = s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType=content_type,
    )


def download_bytes(*, bucket: str, key: str) -> Tuple[bytes, str]:
    s3 = s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    content_type = obj.get("ContentType") or "application/octet-stream"
    return body, content_type
