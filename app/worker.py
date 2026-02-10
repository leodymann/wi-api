# app/worker.py
from __future__ import annotations

import os
import time
import io
import base64
import json
import requests
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Optional
from decimal import Decimal, ROUND_HALF_UP

from dotenv import load_dotenv
from PIL import Image
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, selectinload

from app.infra.db import SessionLocal
from app.infra.models import (
    FinanceORM,
    FinanceStatus,
    InstallmentORM,
    InstallmentStatus,
    PromissoryORM,
    SaleORM,
    WppSendStatus,
    ProductORM,
    ProductStatus,
)
from app.integrations.blibsend import (
    BlibsendError,
    send_whatsapp_text,
    send_whatsapp_group_file_datauri,
)

from app.infra.storage_s3 import presign_get_url

load_dotenv()

UPLOAD_ROOT = Path("uploads")

STATE_DIR = Path(".worker_state")
STATE_DIR.mkdir(exist_ok=True)

# ✅ novo estado: controla 24/dia + 1/h e não reenviar produto
OFFERS_STATE_FILE = STATE_DIR / "offers_state.json"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def today_local_date() -> date:
    # se seu servidor estiver em UTC e você quiser Fortaleza, ajuste aqui
    return datetime.now().date()


def compute_backoff_seconds(tries: int) -> int:
    if tries <= 0:
        return 60
    if tries == 1:
        return 5 * 60
    if tries == 2:
        return 15 * 60
    if tries == 3:
        return 60 * 60
    return 6 * 60 * 60


def can_try(status: Optional[WppSendStatus], next_retry_at: Optional[datetime]) -> bool:
    # se já foi enviado ou está enviando, não tenta
    if status in (WppSendStatus.SENT, WppSendStatus.SENDING):
        return False
    if next_retry_at is None:
        return True
    return next_retry_at <= now_utc()


def mark_failed_generic(
    *,
    row,
    tries_field: str,
    status_field: str,
    error_field: str,
    next_retry_field: str,
    err: str,
) -> None:
    tries = int(getattr(row, tries_field) or 0) + 1
    setattr(row, tries_field, tries)
    setattr(row, status_field, WppSendStatus.FAILED)
    setattr(row, error_field, err[:500])
    setattr(row, next_retry_field, now_utc() + timedelta(seconds=compute_backoff_seconds(tries)))


def format_brl(value) -> str:
    if value is None:
        return "R$0,00"
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    value = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    s = f"{value:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R${s}"


def format_br_phone(phone: str) -> str:
    digits = "".join(ch for ch in (phone or "") if ch.isdigit())
    if len(digits) == 11:
        ddd = digits[:2]
        p1 = digits[2:7]
        p2 = digits[7:]
        return f"({ddd}) {p1}-{p2}"
    if len(digits) == 10:
        ddd = digits[:2]
        p1 = digits[2:6]
        p2 = digits[6:]
        return f"({ddd}) {p1}-{p2}"
    return phone or "-"


def _commit_row(db: Session, row) -> None:
    """
    Commita logo após atualizar SENT/FAILED para não perder status
    se outra parte do loop der erro e fizer rollback.
    """
    db.commit()
    try:
        db.refresh(row)
    except Exception:
        pass


# ============================================================
# ✅ OFERTAS: 24/dia + 1 por hora + só produto novo (por arquivo)
# ============================================================

def _local_now() -> datetime:
    # Railway normalmente roda em UTC; pra regra "1 por hora" tanto faz.
    return datetime.now()


def _today_key() -> str:
    return _local_now().date().isoformat()


def load_offers_state() -> dict:
    """
    state:
      {
        "day": "YYYY-MM-DD",
        "sent_count": 0,
        "last_sent_at": "ISO_DATETIME" | null,
        "sent_product_ids": [1,2,3]
      }
    """
    if not OFFERS_STATE_FILE.exists():
        return {"day": _today_key(), "sent_count": 0, "last_sent_at": None, "sent_product_ids": []}

    try:
        data = json.loads(OFFERS_STATE_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("state not dict")
    except Exception:
        data = {"day": _today_key(), "sent_count": 0, "last_sent_at": None, "sent_product_ids": []}

    # reseta ao virar o dia
    if data.get("day") != _today_key():
        data = {"day": _today_key(), "sent_count": 0, "last_sent_at": None, "sent_product_ids": []}

    data.setdefault("sent_product_ids", [])
    data.setdefault("sent_count", 0)
    data.setdefault("last_sent_at", None)
    data.setdefault("day", _today_key())
    return data


def save_offers_state(state: dict) -> None:
    OFFERS_STATE_FILE.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")


def can_send_offer_now(state: dict, *, min_interval_seconds: int) -> bool:
    last = state.get("last_sent_at")
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last)
    except Exception:
        return True
    return (_local_now() - last_dt).total_seconds() >= float(min_interval_seconds)


def mark_offer_sent(state: dict, product_id: int) -> None:
    state["sent_count"] = int(state.get("sent_count", 0)) + 1
    state["last_sent_at"] = _local_now().isoformat()

    ids = state.get("sent_product_ids") or []
    if product_id not in ids:
        ids.append(product_id)
    state["sent_product_ids"] = ids

    save_offers_state(state)


def image_bytes_to_data_uri_jpeg_optimized(
    data: bytes,
    *,
    max_dim: int,
    quality: int,
    max_bytes: int,
) -> str:
    if not data:
        raise BlibsendError("Imagem vazia (bytes).")

    try:
        with Image.open(io.BytesIO(data)) as img:
            img = img.convert("RGB")

            w, h = img.size
            scale = min(1.0, max_dim / float(max(w, h)))
            if scale < 1.0:
                img = img.resize((int(w * scale), int(h * scale)))

            q = int(quality)
            while True:
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=q, optimize=True)
                raw = buf.getvalue()

                if len(raw) <= max_bytes or q <= 35:
                    b64 = base64.b64encode(raw).decode("utf-8")
                    return f"data:image/jpeg;base64,{b64}"

                q -= 10
    except Exception as e:
        raise BlibsendError(f"Falha ao processar imagem (PIL): {e}")


def fetch_image_bytes_from_storage(image_url_or_key: str, *, timeout: int = 30) -> bytes:
    u = (image_url_or_key or "").strip()
    if not u:
        raise BlibsendError("Imagem sem url/key.")

    if u.startswith("http://") or u.startswith("https://"):
        try:
            r = requests.get(u, timeout=timeout)
            r.raise_for_status()
            return r.content
        except Exception as e:
            raise BlibsendError(f"Falha ao baixar imagem por URL: {e}")

    if u.startswith("/static/"):
        rel = u.replace("/static/", "").lstrip("/")
        file_path = UPLOAD_ROOT / rel
        if not file_path.exists():
            raise BlibsendError(f"Arquivo local não encontrado: {file_path}")
        return file_path.read_bytes()

    try:
        url = presign_get_url(u, expires_seconds=3600)
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.content
    except Exception as e:
        raise BlibsendError(f"Falha ao baixar do S3 (key={u}): {e}")


def process_product_offers(db: Session, group_to: str) -> int:
    """
    ✅ Nova regra:
      - worker checa sempre
      - envia no máximo 1 por vez
      - respeita OFFERS_MAX_PER_DAY (default 24)
      - respeita OFFERS_MIN_INTERVAL_SECONDS (default 3600 = 1h)
      - só envia produto novo (não enviado hoje) e com imagem
    """
    state = load_offers_state()

    max_per_day = int(os.getenv("OFFERS_MAX_PER_DAY", "24"))
    min_interval = int(os.getenv("OFFERS_MIN_INTERVAL_SECONDS", "3600"))
    limit_query = int(os.getenv("PRODUCTS_OFFER_LIMIT", "200"))

    max_dim = int(os.getenv("OFFERS_IMAGE_MAX_DIM", "1280"))
    quality = int(os.getenv("OFFERS_IMAGE_QUALITY", "70"))
    max_bytes = int(os.getenv("OFFERS_IMAGE_MAX_BYTES", "850000"))

    # limite diário
    if int(state.get("sent_count", 0)) >= max_per_day:
        print(f"[worker] offers: daily limit reached ({state.get('sent_count')}/{max_per_day})")
        return 0

    # intervalo mínimo (1h)
    if not can_send_offer_now(state, min_interval_seconds=min_interval):
        return 0

    sent_ids = set(state.get("sent_product_ids") or [])

    stmt = (
        select(ProductORM)
        .options(selectinload(ProductORM.images))
        .where(ProductORM.status == ProductStatus.IN_STOCK)
        .order_by(ProductORM.id.desc())
        .limit(limit_query)
    )

    products = db.execute(stmt).scalars().all()

    candidate: Optional[ProductORM] = None
    cover_url: Optional[str] = None

    for p in products:
        if p.id in sent_ids:
            continue
        images = sorted(p.images or [], key=lambda x: x.position or 9999)
        if not images:
            continue
        candidate = p
        cover_url = images[0].url
        break

    if not candidate:
        # nada novo pra enviar
        return 0

    p = candidate

    title = (
        "🔥 *OFERTA DO DIA 🔥*\n"
        f"🏍️ Modelo: {p.brand} {p.model}\n"
        f"🎨 Cor: {p.color}\n"
        f"📆 Ano: {p.year}\n"
        f"🛣️ Kilometragem: {p.km}km\n"
        f"💰 *Preço: {format_brl(p.sale_price)}*\n"
    )

    try:
        print(f"[worker] offers: downloading image for product {p.id} (url/key={cover_url})")
        original_bytes = fetch_image_bytes_from_storage(cover_url or "")

        body = image_bytes_to_data_uri_jpeg_optimized(
            original_bytes,
            max_dim=max_dim,
            quality=quality,
            max_bytes=max_bytes,
        )

        print(f"[worker] offers: sending product {p.id} to group={group_to}")
        send_whatsapp_group_file_datauri(
            to_group=group_to,
            type_="image",
            title=title,
            body=body,
        )

        mark_offer_sent(state, p.id)
        print(f"[worker] offers: sent product {p.id} (sent_today={state['sent_count']}/{max_per_day})")
        return 1

    except BlibsendError as e:
        print(f"[worker] offers: FAILED product {p.id}: {e}")
        return 0


# ============================================================
# resto do worker (finance + parcelas) igual
# ============================================================

def process_finance(db: Session, to_number: str) -> int:
    today = today_local_date()

    stmt = (
        select(FinanceORM)
        .where(
            and_(
                FinanceORM.status == FinanceStatus.PENDING,
                FinanceORM.due_date <= today,
                or_(FinanceORM.wpp_status.is_(None), FinanceORM.wpp_status != WppSendStatus.SENT),
                or_(
                    FinanceORM.wpp_next_retry_at.is_(None),
                    FinanceORM.wpp_next_retry_at <= now_utc(),
                ),
            )
        )
        .order_by(FinanceORM.due_date.asc(), FinanceORM.id.asc())
        .limit(50)
    )

    rows = db.execute(stmt).scalars().all()
    sent = 0

    for f in rows:
        if not can_try(f.wpp_status, f.wpp_next_retry_at):
            continue

        f.wpp_status = WppSendStatus.SENDING
        db.flush()
        _commit_row(db, f)

        msg = (
            "*📌 Novo Conta Adicionada!!!*\n"
            f"🏦 Empresa: {f.company}\n"
            f"💰 Valor: {format_brl(f.amount)}\n"
            f"📆 Venc.: {f.due_date}\n"
        )

        try:
            send_whatsapp_text(to=to_number, body=msg)

            f.wpp_status = WppSendStatus.SENT
            f.wpp_sent_at = now_utc()
            f.wpp_last_error = None
            f.wpp_next_retry_at = None
            sent += 1

            db.flush()
            _commit_row(db, f)

        except BlibsendError as e:
            mark_failed_generic(
                row=f,
                tries_field="wpp_tries",
                status_field="wpp_status",
                error_field="wpp_last_error",
                next_retry_field="wpp_next_retry_at",
                err=str(e),
            )
            db.flush()
            _commit_row(db, f)

    return sent


def process_installments_due_soon(db: Session, to_number: str) -> int:
    days = int(os.getenv("PROMISSORY_REMINDER_DAYS", "5"))
    today = today_local_date()
    target = today + timedelta(days=days)

    stmt = (
        select(InstallmentORM)
        .options(
            selectinload(InstallmentORM.promissory).selectinload(PromissoryORM.client),
            selectinload(InstallmentORM.promissory).selectinload(PromissoryORM.product),
            selectinload(InstallmentORM.promissory)
            .selectinload(PromissoryORM.sale)
            .selectinload(SaleORM.product),
        )
        .where(
            and_(
                InstallmentORM.status == InstallmentStatus.PENDING,
                InstallmentORM.due_date == target,
                or_(InstallmentORM.wa_due_status.is_(None), InstallmentORM.wa_due_status != WppSendStatus.SENT),
                or_(
                    InstallmentORM.wa_due_next_retry_at.is_(None),
                    InstallmentORM.wa_due_next_retry_at <= now_utc(),
                ),
            )
        )
        .order_by(InstallmentORM.due_date.asc(), InstallmentORM.id.asc())
        .limit(200)
    )

    rows = db.execute(stmt).scalars().all()
    sent = 0

    for inst in rows:
        if not can_try(inst.wa_due_status, inst.wa_due_next_retry_at):
            continue

        inst.wa_due_status = WppSendStatus.SENDING
        db.flush()
        _commit_row(db, inst)

        prom = inst.promissory
        client = prom.client if prom else None

        product = None
        if prom is not None:
            product = prom.product
            if product is None and prom.sale is not None:
                product = prom.sale.product

        client_name = (client.name if client else "-") or "-"
        client_phone = format_br_phone(client.phone if client else "")

        moto_label = f"{product.brand} {product.model} ({product.year})" if product else "Produto -"
        due_str = inst.due_date.strftime("%d/%m/%Y")

        msg = (
            "📅 Lembrete!!!\n"
            f"👤 Cliente: {client_name}\n"
            f"📞 Telefone: {client_phone}\n"
            f"🏍️ Modelo: {moto_label}\n"
            f"💰 Valor: {format_brl(inst.amount)}\n"
            f"📆 Venc.: {due_str}\n"
        )

        try:
            send_whatsapp_text(to=to_number, body=msg)

            inst.wa_due_status = WppSendStatus.SENT
            inst.wa_due_sent_at = now_utc()
            inst.wa_due_last_error = None
            inst.wa_due_next_retry_at = None
            sent += 1

            db.flush()
            _commit_row(db, inst)

        except BlibsendError as e:
            tries = int(inst.wa_due_tries or 0) + 1
            inst.wa_due_tries = tries
            inst.wa_due_status = WppSendStatus.FAILED
            inst.wa_due_last_error = str(e)[:500]
            inst.wa_due_next_retry_at = now_utc() + timedelta(seconds=compute_backoff_seconds(tries))

            db.flush()
            _commit_row(db, inst)

    return sent


def process_installments_overdue(db: Session, to_number: str) -> int:
    today = today_local_date()

    stmt = (
        select(InstallmentORM)
        .options(
            selectinload(InstallmentORM.promissory).selectinload(PromissoryORM.client),
            selectinload(InstallmentORM.promissory).selectinload(PromissoryORM.product),
            selectinload(InstallmentORM.promissory)
            .selectinload(PromissoryORM.sale)
            .selectinload(SaleORM.product),
        )
        .where(
            and_(
                InstallmentORM.status == InstallmentStatus.PENDING,
                InstallmentORM.due_date < today,
                or_(InstallmentORM.wa_overdue_status.is_(None), InstallmentORM.wa_overdue_status != WppSendStatus.SENT),
                or_(
                    InstallmentORM.wa_overdue_next_retry_at.is_(None),
                    InstallmentORM.wa_overdue_next_retry_at <= now_utc(),
                ),
            )
        )
        .order_by(InstallmentORM.due_date.asc(), InstallmentORM.id.asc())
        .limit(100)
    )

    rows = db.execute(stmt).scalars().all()
    sent = 0

    for inst in rows:
        if not can_try(inst.wa_overdue_status, inst.wa_overdue_next_retry_at):
            continue

        inst.wa_overdue_status = WppSendStatus.SENDING
        db.flush()
        _commit_row(db, inst)

        prom = inst.promissory
        client = prom.client if prom else None

        product = None
        if prom is not None:
            product = prom.product
            if product is None and prom.sale is not None:
                product = prom.sale.product

        client_name = (client.name if client else "-") or "-"
        client_phone = format_br_phone(client.phone if client else "")

        moto_label = f"{product.brand} {product.model} ({product.year})" if product else "Produto -"
        due_str = inst.due_date.strftime("%d/%m/%Y")

        msg = (
            f"⚠️ *PARCELA ATRASADA - {moto_label}*\n"
            f"👤 Cliente: {client_name}\n"
            f"📞 Telefone: {client_phone}\n"
            f"💰 Parcela: {format_brl(inst.amount)} • Venc: {due_str}"
        )

        try:
            send_whatsapp_text(to=to_number, body=msg)

            inst.wa_overdue_status = WppSendStatus.SENT
            inst.wa_overdue_sent_at = now_utc()
            inst.wa_overdue_last_error = None
            inst.wa_overdue_next_retry_at = None
            sent += 1

            db.flush()
            _commit_row(db, inst)

        except BlibsendError as e:
            tries = int(inst.wa_overdue_tries or 0) + 1
            inst.wa_overdue_tries = tries
            inst.wa_overdue_status = WppSendStatus.FAILED
            inst.wa_overdue_last_error = str(e)[:500]
            inst.wa_overdue_next_retry_at = now_utc() + timedelta(seconds=compute_backoff_seconds(tries))

            db.flush()
            _commit_row(db, inst)

    return sent


def run_loop() -> None:
    to_number = os.getenv("BLIBSEND_DEFAULT_TO", "").strip()
    if not to_number:
        raise RuntimeError("Configure BLIBSEND_DEFAULT_TO no .env (numero destino do dono).")

    group_to = os.getenv("BLIBSEND_PRODUCTS_GROUP_TO", "").strip()
    if not group_to:
        raise RuntimeError("Configure BLIBSEND_PRODUCTS_GROUP_TO no .env (grupo destino).")

    interval = int(os.getenv("WORKER_INTERVAL_SECONDS", "30"))
    days = int(os.getenv("PROMISSORY_REMINDER_DAYS", "5"))

    max_per_day = int(os.getenv("OFFERS_MAX_PER_DAY", "24"))
    min_interval = int(os.getenv("OFFERS_MIN_INTERVAL_SECONDS", "3600"))
    limit_query = int(os.getenv("PRODUCTS_OFFER_LIMIT", "200"))

    print(
        f"[worker] started. interval={interval}s to={to_number} due_soon_days={days} "
        f"offers_max_per_day={max_per_day} offers_min_interval={min_interval}s "
        f"products_query_limit={limit_query} group_to={group_to}"
    )

    while True:
        started = time.time()

        with SessionLocal() as db:
            a = b = c = d = 0

            try:
                a = process_finance(db, to_number)
            except Exception as e:
                db.rollback()
                print(f"[worker] ERROR process_finance: {e}")

            try:
                c = process_installments_due_soon(db, to_number)
            except Exception as e:
                db.rollback()
                print(f"[worker] ERROR process_installments_due_soon: {e}")

            try:
                b = process_installments_overdue(db, to_number)
            except Exception as e:
                db.rollback()
                print(f"[worker] ERROR process_installments_overdue: {e}")

            try:
                # ✅ checa sempre (a função controla 1h + 24/dia + novo produto)
                d = process_product_offers(db, group_to)
            except Exception as e:
                db.rollback()
                print(f"[worker] ERROR process_product_offers: {e}")

            if a or b or c or d:
                print(f"[worker] sent finance={a} due_soon_installments={c} overdue_installments={b} offers={d}")

        elapsed = time.time() - started
        sleep_for = max(1, interval - int(elapsed))
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        run_loop()
    except KeyboardInterrupt:
        print("[worker] stopped (Ctrl+C)")
