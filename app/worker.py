# app/worker.py
from __future__ import annotations

import json
import os
import time
import io
import base64
import requests
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Optional, List
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
OFFERS_SENT_FILE = STATE_DIR / "offers_sent_date.txt"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def today_local_date() -> date:
    # se seu servidor estiver em UTC e você quiser Fortaleza, ajuste aqui
    return datetime.now().date()


def already_sent_offers_today() -> bool:
    today = today_local_date().isoformat()
    if not OFFERS_SENT_FILE.exists():
        return False
    return OFFERS_SENT_FILE.read_text(encoding="utf-8").strip() == today


def mark_offers_sent_today() -> None:
    OFFERS_SENT_FILE.write_text(today_local_date().isoformat(), encoding="utf-8")


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
    tries = int(getattr(row, tries_field, 0) or 0) + 1
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


def phone_to_blibsend_to(phone: str) -> str:
    """
    Converte telefone salvo no banco para formato esperado no envio.
    Retorna algo tipo: 55DDDNXXXXXXXX ou 55DDDNXXXXXXX
    """
    digits = "".join(ch for ch in (phone or "") if ch.isdigit())
    if not digits:
        return ""
    if digits.startswith("55") and len(digits) >= 12:
        return digits
    if len(digits) in (10, 11):
        return "55" + digits
    return digits


def parse_group_ids(env_value: str) -> List[str]:
    """
    Aceita:
      - "id1@g.us,id2@g.us"
      - '["id1@g.us","id2@g.us"]'
      - "id1@g.us" (um só)
    Retorna lista limpa e sem duplicados.
    """
    raw = (env_value or "").strip()
    if not raw:
        return []

    groups: List[str] = []

    # JSON array
    if raw.startswith("["):
        try:
            arr = json.loads(raw)
            if isinstance(arr, list):
                groups = [str(x).strip() for x in arr]
        except Exception:
            groups = []

    # vírgula ou único
    if not groups:
        if "," in raw:
            groups = [x.strip() for x in raw.split(",")]
        else:
            groups = [raw]

    # limpa, valida e remove duplicados mantendo ordem
    seen = set()
    out: List[str] = []
    for g in groups:
        if not g:
            continue
        if not g.endswith("@g.us"):
            print(f"[worker] WARN: group id inválido (ignorado): {g}")
            continue
        if g in seen:
            continue
        seen.add(g)
        out.append(g)

    return out


def _commit_row(db: Session, row) -> None:
    """
    Commita logo após atualizar SENT/FAILED/SENDING para não perder status
    e evitar duplicidade em loop rápido.
    """
    db.commit()
    try:
        db.refresh(row)
    except Exception:
        pass


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
    """
    Envia lembrete X dias antes do vencimento (para o dono).
    """
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


def process_installments_due_today_to_client(db: Session) -> int:
    """
    ✅ NO DIA DO VENCIMENTO (D0) -> manda para o CLIENTE com PIX.

    Anti-duplicidade correto:
      - usa wa_today_* (que você já criou no banco)
      - marca SENDING e commita ANTES de enviar
      - filtra no SQL para não pegar SENT/SENDING e respeitar next_retry
    """
    enabled = os.getenv("DUE_TODAY_SEND_ENABLED", "1").strip() in ("1", "true", "True")
    if not enabled:
        return 0

    pix_key = (os.getenv("PIX_KEY") or "").strip()
    pix_receiver = (os.getenv("PIX_RECEIVER_NAME") or "Wesley Motos").strip()
    pix_prefix = (os.getenv("PIX_MESSAGE_PREFIX") or "Pagamento parcela").strip()

    if not pix_key:
        print("[worker] DUE_TODAY: PIX_KEY não configurado, pulando.")
        return 0

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
                InstallmentORM.due_date == today,
                InstallmentORM.wa_today_status != WppSendStatus.SENT,
                InstallmentORM.wa_today_status != WppSendStatus.SENDING,
                or_(
                    InstallmentORM.wa_today_next_retry_at.is_(None),
                    InstallmentORM.wa_today_next_retry_at <= now_utc(),
                ),
            )
        )
        .order_by(InstallmentORM.id.asc())
        .limit(300)
    )

    rows = db.execute(stmt).scalars().all()
    sent = 0

    for inst in rows:
        prom = inst.promissory
        client = prom.client if prom else None
        if not client:
            continue

        client_to = phone_to_blibsend_to(client.phone or "")
        if not client_to:
            continue

        if not can_try(inst.wa_today_status, inst.wa_today_next_retry_at):
            continue

        inst.wa_today_status = WppSendStatus.SENDING
        db.flush()
        _commit_row(db, inst)

        product = None
        if prom is not None:
            product = prom.product
            if product is None and prom.sale is not None:
                product = prom.sale.product

        moto_label = f"{product.brand} {product.model} ({product.year})" if product else "sua compra"
        due_str = inst.due_date.strftime("%d/%m/%Y")
        client_name = (client.name or "").strip() or "Cliente"

        msg = (
            f"Olá, {client_name}! 👋\n"
            f"Hoje ({due_str}) vence sua parcela de {moto_label}.\n\n"
            f"💰 Valor: *{format_brl(inst.amount)}*\n"
            f"🔑 Chave Pix: `{pix_key}`\n"
            f"👤 Favorecido: {pix_receiver}\n"
            f"📝 Descrição: {pix_prefix} {moto_label} ({due_str})\n\n"
            "Assim que pagar, responda com o comprovante. Obrigado!"
        )

        try:
            send_whatsapp_text(to=client_to, body=msg)

            inst.wa_today_status = WppSendStatus.SENT
            inst.wa_today_sent_at = now_utc()
            inst.wa_today_last_error = None
            inst.wa_today_next_retry_at = None
            sent += 1

            db.flush()
            _commit_row(db, inst)

        except BlibsendError as e:
            tries = int(inst.wa_today_tries or 0) + 1
            inst.wa_today_tries = tries
            inst.wa_today_status = WppSendStatus.FAILED
            inst.wa_today_last_error = str(e)[:500]
            inst.wa_today_next_retry_at = now_utc() + timedelta(seconds=compute_backoff_seconds(tries))

            db.flush()
            _commit_row(db, inst)

    return sent


def process_installments_overdue(db: Session, to_number: str) -> int:
    """
    Envia alerta de parcela atrasada (para o dono).
    """
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
        r = requests.get(u, timeout=timeout)
        r.raise_for_status()
        return r.content

    if u.startswith("/static/"):
        rel = u.replace("/static/", "").lstrip("/")
        file_path = UPLOAD_ROOT / rel
        if not file_path.exists():
            raise BlibsendError(f"Arquivo local não encontrado: {file_path}")
        return file_path.read_bytes()

    url = presign_get_url(u, expires_seconds=3600)
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content


def process_daily_product_offers(db: Session, group_ids: List[str]) -> int:
    if not group_ids:
        print("[worker] offers: nenhum grupo configurado, pulando.")
        return 0

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
        f"(query_limit={limit_query}, max_per_day={max_per_day}, interval={interval_s}s, groups={len(group_ids)})"
    )

    sent = 0
    for p in products:
        if sent >= max_per_day:
            break

        images = sorted(p.images or [], key=lambda x: x.position or 9999)
        if not images:
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
            original_bytes = fetch_image_bytes_from_storage(cover.url)

            body = image_bytes_to_data_uri_jpeg_optimized(
                original_bytes,
                max_dim=max_dim,
                quality=quality,
                max_bytes=max_bytes,
            )

            # ✅ envia o MESMO produto para TODOS os grupos
            for group_to in group_ids:
                send_whatsapp_group_file_datauri(
                    to_group=group_to,
                    type_="image",
                    title=title,
                    body=body,
                )

            # conta 1 produto enviado (não 1 por grupo)
            sent += 1

            # pausa entre produtos (não entre grupos)
            if interval_s > 0:
                time.sleep(interval_s)

        except BlibsendError as e:
            print(f"[worker] offers: FAILED product {p.id}: {e}")

    return sent


def run_loop() -> None:
    to_number = os.getenv("BLIBSEND_DEFAULT_TO", "").strip()
    if not to_number:
        raise RuntimeError("Configure BLIBSEND_DEFAULT_TO no .env (numero destino do dono).")

    group_to_raw = os.getenv("BLIBSEND_PRODUCTS_GROUP_TO", "").strip()
    group_ids = parse_group_ids(group_to_raw)
    if not group_ids:
        raise RuntimeError(
            "Configure BLIBSEND_PRODUCTS_GROUP_TO no .env. "
            "Ex: 1203...@g.us,1203...@g.us ou JSON [\"...\"]"
        )

    interval = int(os.getenv("WORKER_INTERVAL_SECONDS", "30"))
    days = int(os.getenv("PROMISSORY_REMINDER_DAYS", "5"))

    offer_hour = int(os.getenv("PRODUCTS_OFFER_HOUR", "9"))
    offer_limit = int(os.getenv("PRODUCTS_OFFER_LIMIT", "20"))
    offer_interval = int(os.getenv("OFFERS_SEND_INTERVAL_SECONDS", "8"))
    offer_max = int(os.getenv("OFFERS_MAX_PER_DAY", "5"))

    print(
        f"[worker] started. interval={interval}s to={to_number} due_soon_days={days} "
        f"offers_hour={offer_hour} offers_limit={offer_limit} offers_interval={offer_interval}s "
        f"offers_max={offer_max} group_ids={group_ids}"
    )

    while True:
        started = time.time()

        with SessionLocal() as db:
            a = b = c = d = e = 0

            try:
                a = process_finance(db, to_number)
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_finance: {ex}")

            # ✅ continua avisando o dono X dias antes (PROMISSORY_REMINDER_DAYS, default 5)
            try:
                c = process_installments_due_soon(db, to_number)
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_installments_due_soon: {ex}")

            # ✅ D0: manda pro cliente com Pix
            try:
                e = process_installments_due_today_to_client(db)
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_installments_due_today_to_client: {ex}")

            try:
                b = process_installments_overdue(db, to_number)
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_installments_overdue: {ex}")

            try:
                now_local_dt = datetime.now()
                sent_today = already_sent_offers_today()

                if now_local_dt.hour >= offer_hour and not sent_today:
                    d = process_daily_product_offers(db, group_ids)

                    if d > 0:
                        mark_offers_sent_today()

            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_daily_product_offers: {ex}")

            if a or b or c or d or e:
                print(
                    f"[worker] sent finance={a} due_soon_installments={c} "
                    f"due_today_client={e} overdue_installments={b} offers={d}"
                )

        elapsed = time.time() - started
        sleep_for = max(1, interval - int(elapsed))
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        run_loop()
    except KeyboardInterrupt:
        print("[worker] stopped (Ctrl+C)")
