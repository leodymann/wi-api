# app/api/routers/health.py
from __future__ import annotations

import os
import time
from typing import Any

import requests
from fastapi import APIRouter
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.infra.db import engine

router = APIRouter()

def _safe_err(e: Exception) -> str:
    s = str(e) or e.__class__.__name__
    # evita vazar url/credenciais (best effort)
    for k in ("postgres://", "postgresql://"):
        if k in s:
            s = "db_error"
    return s[:300]

@router.get("/health")
def health() -> dict[str, Any]:
    started = time.time()

    # 1) DB check
    db_ok = False
    db_error = None
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        db_ok = True
    except SQLAlchemyError as e:
        db_error = _safe_err(e)
    except Exception as e:
        db_error = _safe_err(e)

    # 2) (Opcional) Blibsend check (sem mandar msg)
    # Habilita com HEALTH_CHECK_BLIBSEND=1 no Railway
    blib_ok = None
    blib_error = None
    if os.getenv("HEALTH_CHECK_BLIBSEND", "0") == "1":
        try:
            base = (os.getenv("BLIBSEND_BASE_URL") or "").rstrip("/")
            token = (os.getenv("BLIBSEND_BEARER_TOKEN") or "").strip()
            session_token = (os.getenv("BLIBSEND_SESSION_TOKEN") or "").strip()

            if not base or not token or not session_token:
                blib_ok = False
                blib_error = "blibsend_env_missing"
            else:
                # endpoint simples só pra validar auth/rota (ajuste se sua API tiver outro)
                # Se não existir /me, troque por um endpoint que a Blibsend aceite com 200.
                url = f"{base}/me"
                r = requests.get(
                    url,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "session_token": session_token,
                        "User-Agent": "wi-api-health",
                    },
                    timeout=10,
                )
                blib_ok = r.ok
                if not r.ok:
                    blib_error = f"blibsend_http_{r.status_code}"
        except Exception as e:
            blib_ok = False
            blib_error = _safe_err(e)

    ok = db_ok and (blib_ok in (None, True))
    elapsed_ms = int((time.time() - started) * 1000)

    return {
        "ok": ok,
        "db": {"ok": db_ok, "error": db_error},
        "blibsend": (None if blib_ok is None else {"ok": blib_ok, "error": blib_error}),
        "elapsed_ms": elapsed_ms,
    }
