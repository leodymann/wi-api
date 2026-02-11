# app/worker.py
from __future__ import annotations

import os
import time
import io
import base64
import requests
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Optional, Any
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

# ✅ ofertas: controla envio por janela + intervalo (1h)
OFFERS_LAST_SENT_FILE = STATE_DIR / "offers_last_sent_at.txt"


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
    db.commit()
    try:
        db.refresh(row)
    except Exception:
        pass


def _send_text_with_logs(*, to_number: str, body: str, context: str) -> Any:
    try:
        resp = send_whatsapp_text(to=to_number, body=body)
        preview = resp
        try:
            txt = str(resp)
            if len(txt) > 1500:
                preview = txt[:1500] + "…(trunc)"
        except Exception:
            preview = resp
        print(f"[worker] {context}: send_whatsapp_text OK to={to_number} resp={preview}")
        return resp
    except Exception as e:
        print(f"[worker] {context}: send_whatsapp_text FAILED to={to_number} err={repr(e)}")
        raise


# ---------------------------
# ✅ Ofertas: janela 07h-22h + intervalo mínimo 1h
# ---------------------------
def _read_last_offers_sent_at() -> Optional[datetime]:
    if not OFFERS_LAST_SENT_FILE.exists():
        return None
    raw = OFFERS_LAST_SENT_FILE.read_text(encoding="utf-8").strip()
    if not raw:
        return None
    try:
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)
    except Exception:
        return None


def _write_last_offers_sent_at(dt_utc: datetime) -> None:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    OFFERS_LAST_SENT_FILE.write_text(str(dt_utc.timestamp()), encoding="utf-8")


def offers_window_open(now_local: datetime, start_h: int, end_h: int) -> bool:
    # janela [start_h, end_h) -> envia a partir de start_h e para antes de end_h
    return start_h <= now_local.hour < end_h


def can_send_offers_now(*, now_local: datetime, start_h: int, end_h: int, min_interval_s: int) -> bool:
    if not offers_window_open(now_local, start_h, end_h):
        return False

    last = _read_last_offers_sent_at()
    if last is None:
        return True

    nowu = now_utc()
    return (nowu - last).total_seconds() >= float(min_interval_s)


def process_finance(db: Session, to_number: str) -> int:
    today = today_local_date()

    stmt = (
        select(FinanceORM)
        .where(
            and_(
                FinanceORM.status == FinanceStatus.PENDING,
                FinanceORM.due_date <= today,
                or_(FinanceORM.wpp_status.is_(None), FinanceORM.wpp_status != WppSendStatus.SENT),
                or_(FinanceORM.wpp_next_retry_at.is_(None), FinanceORM.wpp_next_retry_at <= now_utc()),
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

        msg = (
            "*📌 Novo Conta Adicionada!!!*\n"
            f"🏦 Empresa: {f.company}\n"
            f"💰 Valor: {format_brl(f.amount)}\n"
            f"📆 Venc.: {f.due_date}\n"
        )

        f.wpp_status = WppSendStatus.SENDING
        db.flush()

        try:
            _send_text_with_logs(to_number=to_number, body=msg, context=f"finance id={f.id}")

            f.wpp_status = WppSendStatus.SENT
            f.wpp_sent_at = now_utc()
            f.wpp_last_error = None
            f.wpp_next_retry_at = None
            sent += 1

            db.flush()
            _commit_row(db, f)

        except Exception as e:
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
                or_(InstallmentORM.wa_due_next_retry_at.is_(None), InstallmentORM.wa_due_next_retry_at <= now_utc()),
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
            "📅 *Lembrete de Vencimento*\n"
            f"👤 Cliente: {client_name}\n"
            f"📞 Telefone: {client_phone}\n"
            f"🏍️ Modelo: {moto_label}\n"
            f"💰 Valor: {format_brl(inst.amount)}\n"
            f"📆 Venc.: {due_str}\n"
        )

        inst.wa_due_status = WppSendStatus.SENDING
        db.flush()

        try:
            _send_text_with_logs(to_number=to_number, body=msg, context=f"due_soon inst_id={inst.id} due={due_str}")

            inst.wa_due_status = WppSendStatus.SENT
            inst.wa_due_sent_at = now_utc()
            inst.wa_due_last_error = None
            inst.wa_due_next_retry_at = None
            sent += 1

            db.flush()
            _commit_row(db, inst)

        except Exception as e:
            print(f"[worker] due_soon: FAILED inst_id={inst.id} to={to_number} err={repr(e)}")
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
                or_(InstallmentORM.wa_overdue_next_retry_at.is_(None), InstallmentORM.wa_overdue_next_retry_at <= now_utc()),
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

        inst.wa_overdue_status = WppSendStatus.SENDING
        db.flush()

        try:
            _send_text_with_logs(to_number=to_number, body=msg, context=f"overdue inst_id={inst.id} due={due_str}")

            inst.wa_overdue_status = WppSendStatus.SENT
            inst.wa_overdue_sent_at = now_utc()
            inst.wa_overdue_last_error = None
            inst.wa_overdue_next_retry_at = None
            sent += 1

            db.flush()
            _commit_row(db, inst)

        except Exception as e:
            print(f"[worker] overdue: FAILED inst_id={inst.id} to={to_number} err={repr(e)}")
            tries = int(inst.wa_overdue_tries or 0) + 1
            inst.wa_overdue_tries = tries
            inst.wa_overdue_status = WppSendStatus.FAILED
            inst.wa_overdue_last_error = str(e)[:500]
            inst.wa_overdue_next_retry_at = now_utc() + timedelta(seconds=compute_backoff_seconds(tries))

            db.flush()
            _commit_row(db, inst)

    return sent


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


def process_daily_product_offers(db: Session, group_to: str) -> int:
    limit_query = int(os.getenv("PRODUCTS_OFFER_LIMIT", "20"))
    max_per_day = int(os.getenv("OFFERS_MAX_PER_DAY", "5"))
    interval_s = int(os.getenv("OFFERS_SEND_INTERVAL_SECONDS", "8"))

    max_dim = int(os.getenv("OFFERS_IMAGE_MAX_DIM", "1280"))
    quality = int(os.getenv("OFFERS_IMAGE_QUALITY", "70"))
    max_bytes = int(os.getenv("OFFERS_IMAGE_MAX_BYTES", "850000"))

    stmt = (
        select(ProductORM)
        .options(selectinload(ProductORM.images))
        .where(ProductORM.status == ProductStatus.IN_STOCK)
        .order_by(ProductORM.id.desc())
        .limit(limit_query)
    )

    products = db.execute(stmt).scalars().all()
    print(
        f"[worker] offers: found {len(products)} products IN_STOCK "
        f"(query_limit={limit_query}, max_per_day={max_per_day}, interval={interval_s}s)"
    )

    sent = 0
    for p in products:
        if sent >= max_per_day:
            print(f"[worker] offers: reached max_per_day={max_per_day}, stopping")
            break

        images = sorted(p.images or [], key=lambda x: x.position or 9999)
        if not images:
            print(f"[worker] offers: product {p.id} has no images, skipping")
            continue

        cover = images[0]

        title = (
            "🔥 *OFERTA DO DIA 🔥*\n"
            f"🏍️ Modelo: {p.brand} {p.model}\n"
            f"🎨 Cor: {p.color}\n"
            f"📆 Ano: {p.year}\n"
            f"🛣️ Kilometragem: {p.km}\n"
            f"💰 *Preço: {format_brl(p.sale_price)}*\n"
        )

        try:
            print(f"[worker] offers: downloading image for product {p.id} (url/key={cover.url})")
            original_bytes = fetch_image_bytes_from_storage(cover.url)

            body = image_bytes_to_data_uri_jpeg_optimized(
                original_bytes,
                max_dim=max_dim,
                quality=quality,
                max_bytes=max_bytes,
            )

            print(f"[worker] offers: sending product {p.id} to group={group_to}")
            resp = send_whatsapp_group_file_datauri(
                to_group=group_to,
                type_="image",
                title=title,
                body=body,
            )
            print(f"[worker] offers: blibsend_resp product_id={p.id} resp={str(resp)[:1500]}")
            sent += 1
            print(f"[worker] offers: sent={sent}/{max_per_day}")

            if interval_s > 0:
                time.sleep(interval_s)

        except Exception as e:
            print(f"[worker] offers: FAILED product {p.id}: {repr(e)}")

    print(f"[worker] offers: sent total={sent}")
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

    offer_limit = int(os.getenv("PRODUCTS_OFFER_LIMIT", "20"))
    offer_interval = int(os.getenv("OFFERS_SEND_INTERVAL_SECONDS", "8"))
    offer_max = int(os.getenv("OFFERS_MAX_PER_DAY", "5"))

    # ✅ 07h até 22h (22h para) + intervalo mínimo 1h
    offers_start = int(os.getenv("OFFERS_START_HOUR", "7"))
    offers_end = int(os.getenv("OFFERS_END_HOUR", "22"))
    offers_min_interval = int(os.getenv("OFFERS_MIN_INTERVAL_SECONDS", "3600"))  # 1h

    print(
        f"[worker] started. interval={interval}s to={to_number} due_soon_days={days} "
        f"offers_window={offers_start:02d}h-{offers_end:02d}h min_interval={offers_min_interval}s "
        f"offers_limit={offer_limit} offers_interval={offer_interval}s offers_max={offer_max} "
        f"group_to={group_to}"
    )

    while True:
        started = time.time()

        with SessionLocal() as db:
            a = b = c = d = 0

            try:
                a = process_finance(db, to_number)
            except Exception as e:
                db.rollback()
                print(f"[worker] ERROR process_finance: {repr(e)}")

            try:
                c = process_installments_due_soon(db, to_number)
            except Exception as e:
                db.rollback()
                print(f"[worker] ERROR process_installments_due_soon: {repr(e)}")

            try:
                b = process_installments_overdue(db, to_number)
            except Exception as e:
                db.rollback()
                print(f"[worker] ERROR process_installments_overdue: {repr(e)}")

            try:
                now_local = datetime.now()
                last = _read_last_offers_sent_at()
                can_send = can_send_offers_now(
                    now_local=now_local,
                    start_h=offers_start,
                    end_h=offers_end,
                    min_interval_s=offers_min_interval,
                )
                in_window = offers_window_open(now_local, offers_start, offers_end)

                print(
                    f"[worker] offers check: now={now_local} window={offers_start}-{offers_end} "
                    f"in_window={in_window} last_sent_at={last} can_send={can_send}"
                )

                if can_send:
                    print("[worker] offers: starting...")
                    d = process_daily_product_offers(db, group_to)

                    if d > 0:
                        _write_last_offers_sent_at(now_utc())
                        print("[worker] offers: saved last_sent_at")
                    else:
                        print("[worker] offers: nothing sent, NOT updating last_sent_at")

            except Exception as e:
                db.rollback()
                print(f"[worker] ERROR process_daily_product_offers: {repr(e)}")

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
