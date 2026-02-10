# app/infra/storage_s3.py
from __future__ import annotations

import os
from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


class S3StorageError(RuntimeError):
    pass


def _getenv(name: str) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        raise S3StorageError(f"Missing env: {name}")
    return v


def _s3_client():
    endpoint = _getenv("RAILWAY_S3_ENDPOINT").rstrip("/")  # https://t3.storageapi.dev
    access = _getenv("RAILWAY_S3_ACCESS_KEY_ID")
    secret = _getenv("RAILWAY_S3_SECRET_ACCESS_KEY")
    bucket = _getenv("RAILWAY_BUCKET")  # só valida aqui
    _ = bucket

    region = (os.getenv("RAILWAY_S3_REGION") or "auto").strip()  # precisa bater com o "auto" do credential

    try:
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            region_name=region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
        )
    except Exception as e:
        raise S3StorageError(f"Falha criando client S3: {e}") from e


def _bucket() -> str:
    return _getenv("RAILWAY_BUCKET")


def normalize_image_content_type(ct: Optional[str]) -> str:
    ct = (ct or "").lower().strip()
    if ct == "image/jpg":
        return "image/jpeg"
    return ct


def upload_image_bytes(*, data: bytes, content_type: str, key_prefix: str) -> str:
    if not data:
        raise S3StorageError("data vazio")
    ct = normalize_image_content_type(content_type)
    if ct not in {"image/jpeg", "image/png", "image/webp"}:
        raise S3StorageError(f"ContentType não permitido: {ct}")

    key_prefix = key_prefix.strip("/")

    # você pode manter seu uuid; aqui só exemplo
    import uuid
    ext = ".jpg" if ct == "image/jpeg" else ".png" if ct == "image/png" else ".webp"
    key = f"{key_prefix}/{uuid.uuid4().hex}{ext}"

    s3 = _s3_client()
    try:
        s3.put_object(
            Bucket=_bucket(),
            Key=key,
            Body=data,
            ContentType=ct,
        )
    except (ClientError, BotoCoreError) as e:
        raise S3StorageError(f"Erro upload S3: {e}") from e

    return key


def delete_object_best_effort(key: str) -> None:
    if not key:
        return
    key = key.lstrip("/")
    s3 = _s3_client()
    try:
        s3.delete_object(Bucket=_bucket(), Key=key)
    except Exception:
        return


def presign_get_url(key: str, expires_seconds: int = 3600) -> str:
    if not key:
        raise S3StorageError("key vazia")
    key = key.lstrip("/")  # IMPORTANTÍSSIMO

    s3 = _s3_client()
    try:
        return s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": _bucket(), "Key": key},
            ExpiresIn=int(expires_seconds),
            HttpMethod="GET",
        )
    except (ClientError, BotoCoreError) as e:
        raise S3StorageError(f"Erro presign S3: {e}") from e


def head_object(key: str) -> None:
    key = (key or "").lstrip("/")
    s3 = _s3_client()
    s3.head_object(Bucket=_bucket(), Key=key)
