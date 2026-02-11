# app/api/routers/health.py
from __future__ import annotations

import os
import time
from typing import Any

import requests
from fastapi import APIRouter, Response
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


@router.head("/health", include_in_schema=False)
def health_head() -> Response:
    # UptimeRobot/health-check costuma usar HEAD. Retorna só status/headers.
    return Response(status_code=200)


@router.get("/health")
def health() -> dict[str, Any]:
    started = time.time()

    # 1) DB check
    db_ok = False
    db_error: str | None = None
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
    blib_ok: bool | None = None
    blib_error: str | None = None

    if os.getenv("HEALTH_CHECK_BLIBSEND", "0") == "1":
        try:
            base = (os.getenv("BLIBSEND_BASE_URL") or "").rstrip("/")
            token = (os.getenv("BLIBSEND_BEARER_TOKEN") or "").strip()
            session_token = (os.getenv("BLIBSEND_SESSION_TOKEN") or "").strip()

            if not base or not token or not session_token:
                blib_ok = False
                blib_error = "blibsend_env_missing"
            else:
                # ✅ Use um endpoint que você tem certeza que existe na Blibsend.
                # Se /me não existir, você vai tomar 404/405 e o monitor vai acusar "down".
                #
                # Sugestão segura: testar o próprio signin/token se você tiver rota
                # de validação; caso não tenha, deixe HEALTH_CHECK_BLIBSEND=0.
                #
                # Se você tiver um endpoint "GET /auth/me" ou parecido na Blibsend, use aqui:
                url = f"{base}/me"

                r = requests.get(
                    url,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "session_token": session_token,
                        "User-Agent": "wi-api-health",
                        "Accept": "application/json",
                    },
                    timeout=10,
                )

                blib_ok = bool(r.ok)
                if not r.ok:
                    blib_error = f"blibsend_http_{r.status_code}"

        except Exception as e:
            blib_ok = False
            blib_error = _safe_err(e)

    ok = db_ok and (blib_ok in (None, True))
    elapsed_ms = int((time.time() - started) * 1000)

    payload = {
        "ok": ok,
        "db": {"ok": db_ok, "error": db_error},
        "blibsend": (None if blib_ok is None else {"ok": blib_ok, "error": blib_error}),
        "elapsed_ms": elapsed_ms,
    }

    # ✅ Opcional (recomendado para monitor):
    # Se você quiser que o monitor marque DOWN quando ok=false,
    # retorne 503. Mantém o JSON igual.
    #
    # if not ok:
    #     from fastapi.responses import JSONResponse
    #     return JSONResponse(payload, status_code=503)

    return payload
