# app/integrations/uazapi.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import requests


class UazapiError(RuntimeError):
    pass


@dataclass(frozen=True)
class UazapiConfig:
    base_url: str
    token: str
    timeout: int = 30


def _cfg() -> UazapiConfig:
    base_url = (os.getenv("UAZAPI_BASE_URL") or "https://free.uazapi.com").rstrip("/")
    token = (os.getenv("UAZAPI_TOKEN") or "").strip()
    if not token:
        raise UazapiError("UAZAPI_TOKEN não configurado.")
    timeout = int(os.getenv("UAZAPI_TIMEOUT_SECONDS", "30"))
    return UazapiConfig(base_url=base_url, token=token, timeout=timeout)


def _headers(cfg: UazapiConfig) -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "token": cfg.token,  # Uazapi usa header token: ...
    }


def send_whatsapp_text(*, to: str, body: str) -> dict:
    """
    POST /send/text
    body: { "number": "...", "text": "..." }
    """
    cfg = _cfg()
    url = f"{cfg.base_url}/send/text"
    payload = {"number": to, "text": body}

    try:
        r = requests.post(url, json=payload, headers=_headers(cfg), timeout=cfg.timeout)
        if r.status_code >= 400:
            raise UazapiError(f"UAZAPI send/text HTTP {r.status_code}: {r.text}")
        return r.json() if r.content else {"ok": True}
    except requests.RequestException as e:
        raise UazapiError(f"UAZAPI send/text request error: {e}") from e


def send_whatsapp_media(*, to: str, type_: str, file_url: str) -> dict:
    """
    POST /send/media
    body: { "number": "...", "type": "image", "file": "https://..." }
    """
    cfg = _cfg()
    url = f"{cfg.base_url}/send/media"
    payload = {"number": to, "type": type_, "file": file_url}

    try:
        r = requests.post(url, json=payload, headers=_headers(cfg), timeout=cfg.timeout)
        if r.status_code >= 400:
            raise UazapiError(f"UAZAPI send/media HTTP {r.status_code}: {r.text}")
        return r.json() if r.content else {"ok": True}
    except requests.RequestException as e:
        raise UazapiError(f"UAZAPI send/media request error: {e}") from e
