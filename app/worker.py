# app/worker.py
from __future__ import annotations

import json
import os
import time
import requests
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Optional, List
from decimal import Decimal, ROUND_HALF_UP

from dotenv import load_dotenv
from sqlalchemy import and_, or_, select, func
from sqlalchemy.orm import Session, selectinload

from app.infra.db import SessionLocal
from app.infra.models import (
    FinanceORM,
    FinanceStatus,
    InstallmentORM,
    InstallmentStatus,
    PromissoryORM,
    SaleORM,
    SaleStatus,
    PaymentType,
    WppSendStatus,
    ProductORM,
    ProductStatus,
)

# ✅ UAZAPI (novo)
from app.integrations.uazapi import (
    UazapiError,
    send_whatsapp_text,
    send_whatsapp_media,
)

from app.infra.storage_s3 import presign_get_url

load_dotenv()

UPLOAD_ROOT = Path("uploads")

STATE_DIR = Path(".worker_state")
STATE_DIR.mkdir(exist_ok=True)

OFFERS_HOURLY_STATE_FILE = STATE_DIR / "offers_hourly_state.json"
WEEKLY_REPORT_SENT_FILE = STATE_DIR / "weekly_report_sent.txt"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def today_local_date() -> date:
    return datetime.now().date()


# ============================================================
# ✅ CONTROLE DE OFERTAS: 1 por hora (07h->21h)
# ============================================================

def _load_offers_hourly_state() -> dict:
    if not OFFERS_HOURLY_STATE_FILE.exists():
        return {}
    try:
        return json.loads(OFFERS_HOURLY_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_offers_hourly_state(state: dict) -> None:
    OFFERS_HOURLY_STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False),
        encoding="utf-8",
    )


def offers_can_send_now(now_local: datetime, *, start_hour: int, end_hour: int) -> bool:
    if now_local.hour < start_hour or now_local.hour > end_hour:
        return False

    state = _load_offers_hourly_state()
    today = now_local.date().isoformat()

    if state.get("date") != today:
        return True

    if state.get("last_hour_sent") == now_local.hour:
        return False

    return True


def mark_offers_sent_this_hour(now_local: datetime, *, product_id: Optional[int] = None) -> None:
    state = {
        "date": now_local.date().isoformat(),
        "last_hour_sent": now_local.hour,
        "sent_at_utc": now_utc().isoformat(),
    }
    if product_id is not None:
        state["last_product_id"] = int(product_id)
    _save_offers_hourly_state(state)


# ============================================================
# HELPERS GERAIS
# ============================================================

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


def phone_to_uazapi_number(phone: str) -> str:
    """
    Uazapi aceita telefone em dígitos (ex: 5583987157461).
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
    raw = (env_value or "").strip()
    if not raw:
        return []

    groups: List[str] = []

    if raw.startswith("["):
        try:
            arr = json.loads(raw)
            if isinstance(arr, list):
                groups = [str(x).strip() for x in arr]
        except Exception:
            groups = []

    if not groups:
        if "," in raw:
            groups = [x.strip() for x in raw.split(",")]
        else:
            groups = [raw]

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
    db.commit()
    try:
        db.refresh(row)
    except Exception:
        pass


# ============================================================
# ✅ RELATÓRIO SEMANAL
# ============================================================

def week_start_end_local(today: date) -> tuple[date, date]:
    start = today - timedelta(days=today.weekday())
    end = start + timedelta(days=6)
    return start, end


def weekly_label(start: date, end: date) -> str:
    return f"{start.isoformat()}_{end.isoformat()}"


def weekly_already_sent(label: str) -> bool:
    if not WEEKLY_REPORT_SENT_FILE.exists():
        return False
    return WEEKLY_REPORT_SENT_FILE.read_text(encoding="utf-8").strip() == label


def weekly_mark_sent(label: str) -> None:
    WEEKLY_REPORT_SENT_FILE.write_text(label, encoding="utf-8")


def process_weekly_report(db: Session, to_number: str) -> int:
    enabled = (os.getenv("WEEKLY_REPORT_ENABLED", "1").strip().lower() in ("1", "true", "yes"))
    if not enabled:
        return 0

    wd = int(os.getenv("WEEKLY_REPORT_WEEKDAY", "0"))
    hour = int(os.getenv("WEEKLY_REPORT_HOUR", "8"))
    minute = int(os.getenv("WEEKLY_REPORT_MINUTE", "0"))

    now_local = datetime.now()
    if now_local.weekday() != wd:
        return 0
    if (now_local.hour, now_local.minute) < (hour, minute):
        return 0

    today = today_local_date()
    start_d, end_d = week_start_end_local(today)

    label = weekly_label(start_d, end_d)
    if weekly_already_sent(label):
        return 0

    start_dt = datetime.combine(start_d, datetime.min.time())
    end_dt = datetime.combine(end_d + timedelta(days=1), datetime.min.time())

    q_sales_confirmed = (
        select(
            func.count(SaleORM.id),
            func.coalesce(func.sum(SaleORM.total), 0),
            func.coalesce(func.sum(SaleORM.discount), 0),
            func.coalesce(func.sum(SaleORM.entry_amount), 0),
            func.coalesce(
                func.sum((SaleORM.total - SaleORM.discount) - func.coalesce(SaleORM.product_cost_price, 0)),
                0,
            ),
        )
        .where(
            and_(
                SaleORM.status == SaleStatus.CONFIRMED,
                SaleORM.created_at >= start_dt,
                SaleORM.created_at < end_dt,
            )
        )
    )
    sc, s_total, s_discount, s_entry, s_profit = db.execute(q_sales_confirmed).one()

    sales_confirmed_count = int(sc or 0)
    sales_total = Decimal(str(s_total or "0"))
    sales_discount = Decimal(str(s_discount or "0"))
    sales_entry = Decimal(str(s_entry or "0"))
    sales_net = (sales_total - sales_discount).quantize(Decimal("0.01"))
    profit_estimated = Decimal(str(s_profit or "0")).quantize(Decimal("0.01"))

    def sum_by_payment(pt: PaymentType) -> Decimal:
        q = (
            select(func.coalesce(func.sum(SaleORM.total - SaleORM.discount), 0))
            .where(
                and_(
                    SaleORM.status == SaleStatus.CONFIRMED,
                    SaleORM.payment_type == pt,
                    SaleORM.created_at >= start_dt,
                    SaleORM.created_at < end_dt,
                )
            )
        )
        v = db.execute(q).scalar()
        return Decimal(str(v or "0"))

    cash_total = sum_by_payment(PaymentType.CASH)
    pix_total = sum_by_payment(PaymentType.PIX)
    card_total = sum_by_payment(PaymentType.CARD)
    prom_total = sum_by_payment(PaymentType.PROMISSORY)
    financing_total = sum_by_payment(PaymentType.FINANCING)

    q_sales_canceled = (
        select(func.count(SaleORM.id))
        .where(
            and_(
                SaleORM.status == SaleStatus.CANCELED,
                SaleORM.created_at >= start_dt,
                SaleORM.created_at < end_dt,
            )
        )
    )
    sales_canceled_count = int(db.execute(q_sales_canceled).scalar() or 0)

    q_inst_paid = (
        select(
            func.count(InstallmentORM.id),
            func.coalesce(func.sum(InstallmentORM.paid_amount), 0),
            func.coalesce(func.sum(InstallmentORM.amount), 0),
        )
        .where(
            and_(
                InstallmentORM.status == InstallmentStatus.PAID,
                InstallmentORM.paid_at.is_not(None),
                InstallmentORM.paid_at >= start_dt,
                InstallmentORM.paid_at < end_dt,
            )
        )
    )
    ic, inst_paid_amount_sum, inst_nominal_sum = db.execute(q_inst_paid).one()
    installments_paid_count = int(ic or 0)
    installments_paid_amount = Decimal(str(inst_paid_amount_sum or "0"))
    installments_nominal = Decimal(str(inst_nominal_sum or "0"))
    received_installments = installments_paid_amount if installments_paid_amount > 0 else installments_nominal

    q_fin_created = (
        select(
            func.count(FinanceORM.id),
            func.coalesce(func.sum(FinanceORM.amount), 0),
        )
        .where(
            and_(
                FinanceORM.created_at >= start_dt,
                FinanceORM.created_at < end_dt,
            )
        )
    )
    fc, fin_created_sum = db.execute(q_fin_created).one()
    finance_created_count = int(fc or 0)
    finance_created_total = Decimal(str(fin_created_sum or "0"))

    def fin_sum_status(st: FinanceStatus) -> Decimal:
        q = select(func.coalesce(func.sum(FinanceORM.amount), 0)).where(FinanceORM.status == st)
        v = db.execute(q).scalar()
        return Decimal(str(v or "0"))

    fin_pending_total = fin_sum_status(FinanceStatus.PENDING)
    fin_paid_total = fin_sum_status(FinanceStatus.PAID)
    fin_canceled_total = fin_sum_status(FinanceStatus.CANCELED)

    has_fin_payment_type = hasattr(FinanceORM, "payment_type")

    fin_cash_week = fin_pix_week = fin_card_week = fin_prom_week = fin_financing_week = Decimal("0")

    if has_fin_payment_type:
        def fin_week_sum_by_payment(pt: PaymentType) -> Decimal:
            q = (
                select(func.coalesce(func.sum(FinanceORM.amount), 0))
                .where(
                    and_(
                        FinanceORM.created_at >= start_dt,
                        FinanceORM.created_at < end_dt,
                        FinanceORM.payment_type == pt,
                    )
                )
            )
            v = db.execute(q).scalar()
            return Decimal(str(v or "0"))

        fin_cash_week = fin_week_sum_by_payment(PaymentType.CASH)
        fin_pix_week = fin_week_sum_by_payment(PaymentType.PIX)
        fin_card_week = fin_week_sum_by_payment(PaymentType.CARD)
        fin_prom_week = fin_week_sum_by_payment(PaymentType.PROMISSORY)
        fin_financing_week = fin_week_sum_by_payment(PaymentType.FINANCING)

    period = f"{start_d.strftime('%d/%m')} a {end_d.strftime('%d/%m')}"

    finance_payment_block = ""
    if has_fin_payment_type:
        finance_payment_block = (
            "• Por pagamento (criado na semana):\n"
            f"   - Dinheiro: {format_brl(fin_cash_week)}\n"
            f"   - Pix: {format_brl(fin_pix_week)}\n"
            f"   - Cartão: {format_brl(fin_card_week)}\n"
            f"   - Promissória: {format_brl(fin_prom_week)}\n"
            f"   - Financiamento: {format_brl(fin_financing_week)}\n"
        )

    msg = (
        f"📊 *RELATÓRIO SEMANAL*\n"
        f"🗓️ Período: {period}\n\n"
        f"🏍️ *Vendas (CONFIRMADAS)*\n"
        f"• Qtd: {sales_confirmed_count}\n"
        f"• Bruto: {format_brl(sales_total)}\n"
        f"• Descontos: {format_brl(sales_discount)}\n"
        f"• Líquido (bruto-desconto): *{format_brl(sales_net)}*\n"
        f"• Entradas: {format_brl(sales_entry)}\n"
        f"• ✅ Lucro estimado: *{format_brl(profit_estimated)}*\n"
        f"• Canceladas: {sales_canceled_count}\n\n"
        f"💳 *Por pagamento (vendas - líquido)*\n"
        f"• Dinheiro: {format_brl(cash_total)}\n"
        f"• Pix: {format_brl(pix_total)}\n"
        f"• Cartão: {format_brl(card_total)}\n"
        f"• Promissória: {format_brl(prom_total)}\n"
        f"• Financiamento: {format_brl(financing_total)}\n\n"
        f"✅ *Recebimentos (parcelas pagas)*\n"
        f"• Qtd: {installments_paid_count}\n"
        f"• Total recebido: *{format_brl(received_installments)}*\n\n"
        f"🏦 *Financeiro*\n"
        f"• Contas criadas na semana: {finance_created_count}\n"
        f"• Total criado na semana: {format_brl(finance_created_total)}\n"
        f"{finance_payment_block}"
        f"• Pendente (atual): {format_brl(fin_pending_total)}\n"
        f"• Pago (atual): {format_brl(fin_paid_total)}\n"
        f"• Cancelado (atual): {format_brl(fin_canceled_total)}\n"
    )

    try:
        send_whatsapp_text(to=to_number, body=msg)
        weekly_mark_sent(label)
        return 1
    except UazapiError as e:
        print(f"[worker] weekly_report FAILED: {e}")
        return 0


# ============================================================
# PROCESSOS EXISTENTES
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

        except UazapiError as e:
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

        except UazapiError as e:
            tries = int(inst.wa_due_tries or 0) + 1
            inst.wa_due_tries = tries
            inst.wa_due_status = WppSendStatus.FAILED
            inst.wa_due_last_error = str(e)[:500]
            inst.wa_due_next_retry_at = now_utc() + timedelta(seconds=compute_backoff_seconds(tries))

            db.flush()
            _commit_row(db, inst)

    return sent


def process_installments_due_today_to_client(db: Session) -> int:
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

        client_to = phone_to_uazapi_number(client.phone or "")
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

        except UazapiError as e:
            tries = int(inst.wa_today_tries or 0) + 1
            inst.wa_today_tries = tries
            inst.wa_today_status = WppSendStatus.FAILED
            inst.wa_today_last_error = str(e)[:500]
            inst.wa_today_next_retry_at = now_utc() + timedelta(seconds=compute_backoff_seconds(tries))

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

        except UazapiError as e:
            tries = int(inst.wa_overdue_tries or 0) + 1
            inst.wa_overdue_tries = tries
            inst.wa_overdue_status = WppSendStatus.FAILED
            inst.wa_overdue_last_error = str(e)[:500]
            inst.wa_overdue_next_retry_at = now_utc() + timedelta(seconds=compute_backoff_seconds(tries))

            db.flush()
            _commit_row(db, inst)

    return sent


# ============================================================
# ✅ OFERTAS: texto + imagem via URL (Uazapi /send/media)
# ============================================================

def resolve_image_to_public_url(image_url_or_key: str) -> str:
    """
    Uazapi precisa de URL em 'file'. Então:
      - se já for http(s), usa direto
      - se for key do S3, gera presigned
      - se for /static/, não dá URL pública aqui (seu worker não é servidor).
        (você pode trocar isso depois por uma URL pública real)
    """
    u = (image_url_or_key or "").strip()
    if not u:
        raise UazapiError("Imagem sem url/key.")

    if u.startswith("http://") or u.startswith("https://"):
        return u

    if u.startswith("/static/"):
        # aqui você teria que ter um domínio público apontando pro seu /static
        raise UazapiError("Imagem local /static não tem URL pública para Uazapi. Use S3 (key) ou URL http(s).")

    # assume S3 key
    return presign_get_url(u, expires_seconds=3600)


def process_hourly_product_offer(db: Session, group_ids: List[str]) -> int:
    if not group_ids:
        print("[worker] offers: nenhum grupo configurado, pulando.")
        return 0

    limit_query = int(os.getenv("PRODUCTS_OFFER_LIMIT", "50"))

    stmt = (
        select(ProductORM)
        .options(selectinload(ProductORM.images))
        .where(ProductORM.status == ProductStatus.IN_STOCK)
        .order_by(ProductORM.id.desc())
        .limit(limit_query)
    )

    products = db.execute(stmt).scalars().all()
    print(f"[worker] offers(hourly): scanning {len(products)} IN_STOCK (query_limit={limit_query}, groups={len(group_ids)})")

    chosen: Optional[ProductORM] = None
    chosen_cover_key: Optional[str] = None

    for p in products:
        images = sorted(p.images or [], key=lambda x: x.position or 9999)
        if not images:
            continue

        cover = images[0]
        cover_key = (getattr(cover, "url", None) or "").strip()
        if not cover_key:
            continue

        chosen = p
        chosen_cover_key = cover_key
        break

    if not chosen or not chosen_cover_key:
        print("[worker] offers(hourly): nenhum produto com imagem encontrado, não enviou.")
        return 0

    p = chosen
    title = (
        "🔥 *OFERTA DO DIA 🔥*\n"
        f"🏍️ Modelo: {p.brand} {p.model}\n"
        f"🎨 Cor: {p.color}\n"
        f"📆 Ano: {p.year}\n"
        f"🛣️ Kilometragem: {p.km}\n"
        f"💰 *Preço: {format_brl(p.sale_price)}*\n"
    )

    try:
        image_url = resolve_image_to_public_url(chosen_cover_key)

        for group_to in group_ids:
            # 1) manda o texto com detalhes
            send_whatsapp_text(to=group_to, body=title)

            # 2) manda a imagem via URL
            send_whatsapp_media(to=group_to, type_="image", file_url=image_url)

        print(f"[worker] offers(hourly): SENT product_id={p.id} to_groups={len(group_ids)}")
        return 1

    except (UazapiError, requests.RequestException, Exception) as e:
        print(f"[worker] offers(hourly): FAILED product_id={p.id}: {e}")
        return 0


# ============================================================
# LOOP
# ============================================================

def run_loop() -> None:
    print("[worker] UAZAPI_DEFAULT_TO=", repr(os.getenv("UAZAPI_DEFAULT_TO")))
    print("[worker] UAZAPI_TOKEN exists? ", bool((os.getenv("UAZAPI_TOKEN") or "").strip()))
    print("[worker] BLIBSEND_DEFAULT_TO=", repr(os.getenv("BLIBSEND_DEFAULT_TO")))
    to_number = os.getenv("UAZAPI_DEFAULT_TO", "").strip()
    if not to_number:
        raise RuntimeError("Configure UAZAPI_DEFAULT_TO no .env (numero destino do dono).")

    group_to_raw = os.getenv("UAZAPI_PRODUCTS_GROUP_TO", "").strip()
    group_ids = parse_group_ids(group_to_raw)
    if not group_ids:
        raise RuntimeError(
            "Configure UAZAPI_PRODUCTS_GROUP_TO no .env. "
            "Ex: 1203...@g.us,1203...@g.us ou JSON [\"...\"]"
        )

    interval = int(os.getenv("WORKER_INTERVAL_SECONDS", "30"))
    days = int(os.getenv("PROMISSORY_REMINDER_DAYS", "5"))

    offers_start_hour = int(os.getenv("OFFERS_START_HOUR", "7"))
    offers_end_hour = int(os.getenv("OFFERS_END_HOUR", "21"))
    offer_limit = int(os.getenv("PRODUCTS_OFFER_LIMIT", "50"))

    print(
        f"[worker] started. interval={interval}s to={to_number} due_soon_days={days} "
        f"offers_window={offers_start_hour}-{offers_end_hour} products_limit={offer_limit} "
        f"group_ids={group_ids}"
    )

    while True:
        started = time.time()

        with SessionLocal() as db:
            a = b = c = d = e = wr = 0

            try:
                a = process_finance(db, to_number)
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_finance: {ex}")

            try:
                c = process_installments_due_soon(db, to_number)
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_installments_due_soon: {ex}")

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
                wr = process_weekly_report(db, to_number)
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_weekly_report: {ex}")

            try:
                now_local_dt = datetime.now()
                if offers_can_send_now(now_local_dt, start_hour=offers_start_hour, end_hour=offers_end_hour):
                    d = process_hourly_product_offer(db, group_ids)
                    if d > 0:
                        mark_offers_sent_this_hour(now_local_dt)
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_hourly_product_offer: {ex}")

            if a or b or c or d or e or wr:
                print(
                    f"[worker] sent finance={a} due_soon_installments={c} "
                    f"due_today_client={e} overdue_installments={b} offers={d} weekly_report={wr}"
                )

        elapsed = time.time() - started
        sleep_for = max(1, interval - int(elapsed))
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        run_loop()
    except KeyboardInterrupt:
        print("[worker] stopped (Ctrl+C)")

