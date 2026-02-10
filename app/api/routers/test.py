from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Form, Depends
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.deps import DBSession
from app.api.auth_deps import get_current_user
from app.infra.models import ProductORM, ProductStatus, ProductImageORM
from app.schemas.products import ProductOut

from app.infra.storage_s3 import (
    upload_image_bytes,
    delete_object_best_effort,
    normalize_image_content_type,
)

import traceback

router = APIRouter()

@router.get("/_debug/s3")
def debug_s3():
    try:
        key = upload_image_bytes(data=b"test", content_type="image/png", key_prefix="debug")
        return {"ok": True, "key": key}
    except Exception as e:
        print("DEBUG S3 ERROR:", repr(e))
        traceback.print_exc()
        raise