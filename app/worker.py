# app/worker.py
from __future__ import annotations

import base64
import io
import json
import os
import time
import requests
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Optional, List, Any
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
MONTHLY_REPORT_SENT_FILE = STATE_DIR / "monthly_report_sent.txt"
SALES_ALERT_STATE_FILE = STATE_DIR / "sales_confirmed_alert_state.json"


# ============================================================
# TIME
# ============================================================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def today_local_date() -> date:
    return datetime.now().date()


def is_last_day_of_month(d: date) -> bool:
    return (d + timedelta(days=1)).month != d.month


def month_start_end(d: date) -> tuple[date, date]:
    start = d.replace(day=1)
    if start.month == 12:
        nxt = date(start.year + 1, 1, 1)
    else:
        nxt = date(start.year, start.month + 1, 1)
    end = nxt - timedelta(days=1)
    return start, end


# ============================================================
# ✅ CONTROLE DE OFERTAS: 1 por hora + sem repetir produto no dia
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
    state = _load_offers_hourly_state()
    today = now_local.date().isoformat()

    if state.get("date") != today:
        state = {"date": today, "sent_product_ids": []}

    sent_ids = state.get("sent_product_ids")
    if not isinstance(sent_ids, list):
        sent_ids = []

    sent_set: set[int] = set()
    for x in sent_ids:
        try:
            sent_set.add(int(x))
        except Exception:
            pass

    if product_id is not None:
        sent_set.add(int(product_id))

    state.update(
        {
            "date": today,
            "last_hour_sent": now_local.hour,
            "sent_at_utc": now_utc().isoformat(),
            "sent_product_ids": sorted(sent_set),
            "last_product_id": int(product_id) if product_id is not None else state.get("last_product_id"),
        }
    )
    _save_offers_hourly_state(state)


# ============================================================
# HELPERS
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
# ✅ PDF (design futurista claro) - sem libs externas
# ============================================================

def _pdf_escape(s: str) -> str:
    return (s or "").replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def build_futuristic_light_pdf_bytes(
    *,
    store_name: str,
    period_label: str,
    generated_at: str,
    kpis: list[tuple[str, str]],
    payments: list[tuple[str, str]],
    finance: list[tuple[str, str]],
    footnote: str = "",
) -> bytes:
    W, H = 595, 842
    margin_x = 46
    cmds: list[str] = []

    def set_stroke_rgb(r, g, b): cmds.append(f"{r/255:.3f} {g/255:.3f} {b/255:.3f} RG")
    def set_fill_rgb(r, g, b): cmds.append(f"{r/255:.3f} {g/255:.3f} {b/255:.3f} rg")

    def rect(x: float, y_top: float, w: float, h: float, fill: bool, stroke: bool):
        y0 = y_top - h
        cmds.append(f"{x:.2f} {y0:.2f} {w:.2f} {h:.2f} re")
        if fill and stroke:
            cmds.append("B")
        elif fill:
            cmds.append("f")
        elif stroke:
            cmds.append("S")
        else:
            cmds.append("n")

    def line(x1: float, y1: float, x2: float, y2: float):
        cmds.append(f"{x1:.2f} {y1:.2f} m {x2:.2f} {y2:.2f} l S")

    def text(font: str, size: int, x: float, y: float, s: str):
        cmds.append("BT")
        cmds.append(f"/{font} {size} Tf")
        cmds.append(f"{x:.2f} {y:.2f} Td")
        cmds.append(f"({_pdf_escape(s)}) Tj")
        cmds.append("ET")

    # background
    set_fill_rgb(246, 248, 252)
    set_stroke_rgb(246, 248, 252)
    rect(0, H, W, H, fill=True, stroke=True)

    # top panel
    top_h = 88
    set_fill_rgb(255, 255, 255)
    set_stroke_rgb(230, 233, 240)
    rect(0, H, W, top_h, fill=True, stroke=True)

    # neon lines
    set_stroke_rgb(124, 77, 255)  # purple
    cmds.append("2 w")
    line(0, H - top_h + 14, W, H - top_h + 14)
    set_stroke_rgb(0, 229, 255)   # cyan
    cmds.append("1 w")
    line(0, H - top_h + 12, W, H - top_h + 12)
    cmds.append("1 w")

    # header text
    set_fill_rgb(20, 24, 33)
    text("F1", 16, margin_x, H - 36, "Relatorio Mensal")
    set_fill_rgb(90, 97, 110)
    text("F2", 10, margin_x, H - 58, store_name)

    set_fill_rgb(90, 97, 110)
    text("F2", 9, W - margin_x - 240, H - 42, f"Periodo: {period_label}")
    text("F2", 9, W - margin_x - 240, H - 60, f"Gerado em: {generated_at}")

    # KPI cards (4)
    y = H - top_h - 24
    card_h = 58
    gap = 12
    card_w = (W - margin_x * 2 - gap * 3) / 4

    def kpi_card(ix: int, title_: str, value_: str):
        x = margin_x + ix * (card_w + gap)

        set_fill_rgb(255, 255, 255)
        set_stroke_rgb(224, 228, 238)
        rect(x, y, card_w, card_h, fill=True, stroke=True)

        set_fill_rgb(240, 241, 255)
        set_stroke_rgb(240, 241, 255)
        rect(x, y, card_w, 10, fill=True, stroke=True)

        set_stroke_rgb(124, 77, 255)
        cmds.append("1.2 w")
        line(x, y - 10 + 1, x + card_w, y - 10 + 1)
        cmds.append("1 w")

        set_fill_rgb(94, 101, 115)
        text("F2", 8, x + 10, y - 22, title_)
        set_fill_rgb(20, 24, 33)
        text("F1", 12, x + 10, y - 44, value_)

    for i, (t, v) in enumerate(kpis[:4]):
        kpi_card(i, t, v)

    # sections (2 columns)
    y2_top = y - 92
    col_gap = 14
    col_w = (W - margin_x * 2 - col_gap) / 2
    left_x = margin_x
    right_x = margin_x + col_w + col_gap
    box_h = 300

    def section_box(x: float, y_top: float, title_: str, items: list[tuple[str, str]], accent_rgb: tuple[int, int, int]):
        ar, ag, ab = accent_rgb

        set_fill_rgb(255, 255, 255)
        set_stroke_rgb(224, 228, 238)
        rect(x, y_top, col_w, box_h, fill=True, stroke=True)

        set_fill_rgb(246, 248, 252)
        set_stroke_rgb(246, 248, 252)
        rect(x, y_top, col_w, 34, fill=True, stroke=True)

        set_fill_rgb(ar, ag, ab)
        set_stroke_rgb(ar, ag, ab)
        rect(x + 12, y_top - 10, 8, 8, fill=True, stroke=True)

        set_stroke_rgb(ar, ag, ab)
        cmds.append("1.2 w")
        line(x + 24, y_top - 14, x + col_w - 12, y_top - 14)
        cmds.append("1 w")

        set_fill_rgb(20, 24, 33)
        text("F1", 11, x + 12, y_top - 24, title_)

        yy = y_top - 54
        for k, v in items:
            if yy < y_top - box_h + 24:
                break
            set_fill_rgb(95, 103, 118)
            text("F2", 9, x + 12, yy, k)
            set_fill_rgb(20, 24, 33)
            text("F2", 9, x + col_w - 170, yy, v)
            yy -= 18

    section_box(left_x, y2_top, "Vendas por pagamento (liquido)", payments, (0, 229, 255))
    section_box(right_x, y2_top, "Financeiro", finance, (124, 77, 255))

    # footer
    set_fill_rgb(120, 128, 144)
    if footnote:
        text("F2", 8, margin_x, 46, footnote[:140])
    text("F2", 8, margin_x, 28, "Gerado automaticamente pelo WI Motos.")
    text("F2", 8, W - margin_x - 70, 28, "Pagina 1/1")

    content = "\n".join(cmds).encode("utf-8")

    def obj(n: int, body: bytes) -> bytes:
        return f"{n} 0 obj\n".encode() + body + b"\nendobj\n"

    objects: list[bytes] = []
    objects.append(obj(1, b"<< /Type /Catalog /Pages 2 0 R >>"))
    objects.append(obj(2, b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>"))
    page = (
        b"<< /Type /Page /Parent 2 0 R "
        b"/MediaBox [0 0 595 842] "
        b"/Resources << /Font << /F1 4 0 R /F2 5 0 R >> >> "
        b"/Contents 6 0 R >>"
    )
    objects.append(obj(3, page))
    objects.append(obj(4, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>"))
    objects.append(obj(5, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"))
    stream = b"<< /Length %d >>\nstream\n" % len(content) + content + b"\nendstream"
    objects.append(obj(6, stream))

    pdf = io.BytesIO()
    pdf.write(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for o in objects:
        offsets.append(pdf.tell())
        pdf.write(o)

    xref_start = pdf.tell()
    pdf.write(f"xref\n0 {len(offsets)}\n".encode())
    pdf.write(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        pdf.write(f"{off:010d} 00000 n \n".encode())

    pdf.write(
        (
            "trailer\n"
            f"<< /Size {len(offsets)} /Root 1 0 R >>\n"
            "startxref\n"
            f"{xref_start}\n"
            "%%EOF\n"
        ).encode("utf-8")
    )
    return pdf.getvalue()


# ============================================================
# STATE HELPERS
# ============================================================

def _read_text_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _write_text_file(path: Path, value: str) -> None:
    path.write_text((value or "").strip(), encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _iso(dt: datetime) -> str:
    return dt.isoformat()


# ============================================================
# ✅ RELATORIO SEMANAL: texto (mensagem)
# ============================================================

def week_start_end_local(today: date) -> tuple[date, date]:
    start = today - timedelta(days=today.weekday())
    end = start + timedelta(days=6)
    return start, end


def weekly_label(start: date, end: date) -> str:
    return f"{start.isoformat()}_{end.isoformat()}"


def weekly_already_sent(label: str) -> bool:
    return _read_text_file(WEEKLY_REPORT_SENT_FILE) == label


def weekly_mark_sent(label: str) -> None:
    _write_text_file(WEEKLY_REPORT_SENT_FILE, label)


def process_weekly_report_text(db: Session, to_number: str) -> int:
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

    pt_fin = getattr(PaymentType, "FINANCING", None)
    financing_total = sum_by_payment(pt_fin) if pt_fin else Decimal("0")

    def fin_sum_status(st: FinanceStatus) -> Decimal:
        q = select(func.coalesce(func.sum(FinanceORM.amount), 0)).where(FinanceORM.status == st)
        v = db.execute(q).scalar()
        return Decimal(str(v or "0"))

    fin_pending_total = fin_sum_status(FinanceStatus.PENDING)
    fin_paid_total = fin_sum_status(FinanceStatus.PAID)

    period = f"{start_d.strftime('%d/%m')} a {end_d.strftime('%d/%m')}"

    msg = (
        f"📊 *RELATORIO SEMANAL*\n"
        f"Periodo: {period}\n\n"
        f"Vendas confirmadas: *{sales_confirmed_count}*\n"
        f"Liquido vendido: *{format_brl(sales_net)}*\n"
        f"Entradas: {format_brl(sales_entry)}\n"
        f"Lucro estimado: {format_brl(profit_estimated)}\n\n"
        f"*Pagamentos (liquido)*\n"
        f"Dinheiro: {format_brl(cash_total)}\n"
        f"Pix: {format_brl(pix_total)}\n"
        f"Cartao: {format_brl(card_total)}\n"
        f"Promissoria: {format_brl(prom_total)}\n"
        + (f"Financiamento: {format_brl(financing_total)}\n" if pt_fin else "")
        + "\n"
        f"*Financeiro (atual)*\n"
        f"Pendente: {format_brl(fin_pending_total)}\n"
        f"Pago: {format_brl(fin_paid_total)}\n"
    )

    try:
        send_whatsapp_text(to=to_number, body=msg)
        weekly_mark_sent(label)
        return 1
    except UazapiError as e:
        print(f"[worker] weekly_report_text FAILED: {e}")
        return 0


# ============================================================
# ✅ RELATORIO MENSAL: PDF no ultimo dia do mes
# ============================================================

def monthly_label(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def monthly_already_sent(label: str) -> bool:
    return _read_text_file(MONTHLY_REPORT_SENT_FILE) == label


def monthly_mark_sent(label: str) -> None:
    _write_text_file(MONTHLY_REPORT_SENT_FILE, label)


def process_monthly_report_pdf(db: Session, to_number: str) -> int:
    enabled = (os.getenv("MONTHLY_REPORT_ENABLED", "1").strip().lower() in ("1", "true", "yes"))
    if not enabled:
        return 0

    hour = int(os.getenv("MONTHLY_REPORT_HOUR", "20"))
    minute = int(os.getenv("MONTHLY_REPORT_MINUTE", "0"))

    today = today_local_date()
    if not is_last_day_of_month(today):
        return 0

    now_local = datetime.now()
    if (now_local.hour, now_local.minute) < (hour, minute):
        return 0

    label = monthly_label(today.year, today.month)
    if monthly_already_sent(label):
        return 0

    start_d, end_d = month_start_end(today)
    start_dt = datetime.combine(start_d, datetime.min.time())
    end_dt = datetime.combine(end_d + timedelta(days=1), datetime.min.time())

    q_sales = (
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
    sc, s_total, s_discount, s_entry, s_profit = db.execute(q_sales).one()

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

    pt_fin = getattr(PaymentType, "FINANCING", None)
    financing_total = sum_by_payment(pt_fin) if pt_fin else Decimal("0")

    def fin_sum_status(st: FinanceStatus) -> Decimal:
        q = select(func.coalesce(func.sum(FinanceORM.amount), 0)).where(FinanceORM.status == st)
        v = db.execute(q).scalar()
        return Decimal(str(v or "0"))

    fin_pending = fin_sum_status(FinanceStatus.PENDING)
    fin_paid = fin_sum_status(FinanceStatus.PAID)
    fin_canceled = fin_sum_status(FinanceStatus.CANCELED)

    q_fin_created = (
        select(func.coalesce(func.sum(FinanceORM.amount), 0))
        .where(and_(FinanceORM.created_at >= start_dt, FinanceORM.created_at < end_dt))
    )
    fin_created_month = Decimal(str(db.execute(q_fin_created).scalar() or "0"))

    store_name = (os.getenv("PDF_STORE_NAME") or "Wesley Motos").strip()
    period = f"{start_d.strftime('%d/%m/%Y')} a {end_d.strftime('%d/%m/%Y')}"
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    kpis = [
        ("Vendas confirmadas", str(sales_confirmed_count)),
        ("Liquido vendido", format_brl(sales_net)),
        ("Lucro estimado", format_brl(profit_estimated)),
        ("Entradas", format_brl(sales_entry)),
    ]

    payments = [
        ("Dinheiro", format_brl(cash_total)),
        ("Pix", format_brl(pix_total)),
        ("Cartao", format_brl(card_total)),
        ("Promissoria", format_brl(prom_total)),
    ] + ([("Financiamento", format_brl(financing_total))] if pt_fin else [])

    finance = [
        ("Criado no mes", format_brl(fin_created_month)),
        ("Pendente", format_brl(fin_pending)),
        ("Pago", format_brl(fin_paid)),
        ("Cancelado", format_brl(fin_canceled)),
    ]

    try:
        pdf_bytes = build_futuristic_light_pdf_bytes(
            store_name=store_name,
            period_label=period,
            generated_at=generated_at,
            kpis=kpis,
            payments=payments,
            finance=finance,
            footnote="Fechamento do mes. Valores em BRL.",
        )

        b64 = base64.b64encode(pdf_bytes).decode("utf-8")
        pdf_data_uri = f"data:application/pdf;base64,{b64}"
        filename = f"Relatorio_Mensal_{label}.pdf"

        send_whatsapp_media(
            to=to_number,
            type_="document",
            file_url=pdf_data_uri,
            text=f"📄 Relatorio mensal ({label})",
            doc_name=filename,
            mime_type="application/pdf",
        )

        monthly_mark_sent(label)
        return 1

    except UazapiError as e:
        print(f"[worker] monthly_report_pdf FAILED: {e}")
        return 0
    except Exception as e:
        print(f"[worker] monthly_report_pdf FAILED (pdf): {e}")
        return 0


# ============================================================
# ✅ ALERTA: cada venda CONFIRMADA -> WhatsApp em ALERT_TO
# (carrega client + product via relationship)
# ============================================================

def _get_alert_to() -> str:
    return (os.getenv("ALERT_TO") or "").strip()


def _load_sales_alert_state() -> dict[str, Any]:
    st = _load_json(SALES_ALERT_STATE_FILE)
    if not isinstance(st, dict):
        st = {}
    if "max_id" not in st:
        st["max_id"] = 0
    if "last_run_utc" not in st:
        st["last_run_utc"] = None
    return st


def _save_sales_alert_state(st: dict[str, Any]) -> None:
    _save_json(SALES_ALERT_STATE_FILE, st)


def process_confirmed_sales_alerts(db: Session) -> int:
    alert_to = _get_alert_to()
    if not alert_to:
        return 0

    enabled = (os.getenv("SALE_CONFIRMED_ALERTS_ENABLED", "1").strip().lower() in ("1", "true", "yes"))
    if not enabled:
        return 0

    st = _load_sales_alert_state()
    max_id = int(st.get("max_id", 0) or 0)

    stmt = (
        select(SaleORM)
        .options(
            selectinload(SaleORM.client),
            selectinload(SaleORM.product),
            selectinload(SaleORM.user),
        )
        .where(and_(SaleORM.status == SaleStatus.CONFIRMED, SaleORM.id > max_id))
        .order_by(SaleORM.id.asc())
        .limit(200)
    )

    rows = db.execute(stmt).scalars().all()
    if not rows:
        st["last_run_utc"] = _iso(now_utc())
        _save_sales_alert_state(st)
        return 0

    sent = 0
    new_max = max_id

    for s in rows:
        sale_id = int(s.id or 0)
        new_max = max(new_max, sale_id)

        # client
        c = getattr(s, "client", None)
        client_name = (getattr(c, "name", None) or f"Cliente #{getattr(s, 'client_id', '-')}")
        client_phone = format_br_phone(getattr(c, "phone", None) or "")

        # product with fallback snapshot on Sale
        p = getattr(s, "product", None)
        if p is not None:
            brand = getattr(p, "brand", "") or ""
            model = getattr(p, "model", "") or ""
            year = getattr(p, "year", "") or ""
            plate = getattr(p, "plate", None) or ""
            product_label = f"{brand} {model} ({year})" + (f" • {plate}" if plate else "")
        else:
            brand = getattr(s, "product_brand", "") or ""
            model = getattr(s, "product_model", "") or ""
            year = getattr(s, "product_year", "") or ""
            plate = getattr(s, "product_plate", None) or ""
            if brand or model or year:
                product_label = f"{brand} {model} ({year})" + (f" • {plate}" if plate else "")
            else:
                product_label = f"Produto #{getattr(s, 'product_id', '-')}"
        
        # seller/user (optional)
        u = getattr(s, "user", None)
        seller_name = (getattr(u, "name", None) or "-")

        total = getattr(s, "total", None)
        discount = getattr(s, "discount", None)
        entry = getattr(s, "entry_amount", None)
        payment_type = getattr(s, "payment_type", None)

        # líquido
        try:
            liquid = (Decimal(str(total or "0")) - Decimal(str(discount or "0"))).quantize(Decimal("0.01"))
        except Exception:
            liquid = None

        created_at = getattr(s, "created_at", None)
        created_str = created_at.strftime("%d/%m/%Y %H:%M") if isinstance(created_at, datetime) else "-"

        pt_str = str(payment_type.value if hasattr(payment_type, "value") else payment_type or "-")

        msg = (
            "✅ *VENDA CONFIRMADA*\n"
            f"ID: {sale_id}\n"
            f"Vendedor: {seller_name}\n"
            f"Cliente: {client_name}\n"
            f"Telefone: {client_phone}\n"
            f"Produto: {product_label}\n"
            f"Pagamento: {pt_str}\n"
            f"Total: {format_brl(total)}\n"
            f"Desconto: {format_brl(discount)}\n"
            f"Liquido: {format_brl(liquid)}\n"
            f"Entrada: {format_brl(entry)}\n"
            f"Data: {created_str}"
        )

        try:
            send_whatsapp_text(to=alert_to, body=msg)
            sent += 1
        except UazapiError as e:
            print(f"[worker] sale_confirmed_alert FAILED sale_id={sale_id}: {e}")
        except Exception as e:
            print(f"[worker] sale_confirmed_alert ERROR sale_id={sale_id}: {e}")

    st["max_id"] = int(new_max)
    st["last_run_utc"] = _iso(now_utc())
    _save_sales_alert_state(st)

    return sent


# ============================================================
# PROCESSOS EXISTENTES (iguais ao seu)
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


def process_installments_due_today_to_client(db: Session, to_number: str) -> int:
    """
    ✅ ATENÇÃO: no seu código anterior esta função não recebia to_number,
    mas aqui mantemos assinatura com to_number para consistência do loop.
    (Ela envia pro CLIENTE, não pro dono.)
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
                or_(InstallmentORM.wa_today_next_retry_at.is_(None), InstallmentORM.wa_today_next_retry_at <= now_utc()),
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
# ✅ OFERTAS: imagem + legenda + sem repetir no mesmo dia
# ============================================================

def resolve_image_to_public_url(image_url_or_key: str) -> str:
    u = (image_url_or_key or "").strip()
    if not u:
        raise UazapiError("Imagem sem url/key.")

    if u.startswith("http://") or u.startswith("https://"):
        return u

    if u.startswith("/static/"):
        raise UazapiError("Imagem local /static não tem URL pública para Uazapi. Use S3 (key) ou URL http(s).")

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

    state = _load_offers_hourly_state()
    today = datetime.now().date().isoformat()
    sent_today = state.get("sent_product_ids") if state.get("date") == today else []
    if not isinstance(sent_today, list):
        sent_today = []

    sent_today_set: set[int] = set()
    for x in sent_today:
        try:
            sent_today_set.add(int(x))
        except Exception:
            pass

    chosen: Optional[ProductORM] = None
    chosen_cover_key: Optional[str] = None

    for p in products:
        if p.id in sent_today_set:
            continue

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
        if sent_today_set:
            print(f"[worker] offers(hourly): nenhum produto novo para hoje (já enviados: {len(sent_today_set)}).")
        else:
            print("[worker] offers(hourly): nenhum produto com imagem encontrado, não enviou.")
        return 0

    p = chosen
    caption = (
        "🔥 *OFERTA DO DIA 🔥*\n"
        f"Modelo: {p.brand} {p.model}\n"
        f"Cor: {p.color}\n"
        f"Ano: {p.year}\n"
        f"KM: {p.km}\n"
        f"Preco: *{format_brl(p.sale_price)}*\n"
    )

    try:
        image_url = resolve_image_to_public_url(chosen_cover_key)

        for group_to in group_ids:
            send_whatsapp_media(
                to=group_to,
                type_="image",
                file_url=image_url,
                text=caption,
            )

        print(f"[worker] offers(hourly): SENT product_id={p.id} to_groups={len(group_ids)}")
        return int(p.id)

    except (UazapiError, requests.RequestException, Exception) as e:
        print(f"[worker] offers(hourly): FAILED product_id={p.id}: {e}")
        return 0


# ============================================================
# LOOP
# ============================================================

def run_loop() -> None:
    print("[worker] UAZAPI_DEFAULT_TO=", repr(os.getenv("UAZAPI_DEFAULT_TO")))
    print("[worker] UAZAPI_TOKEN exists?  ", bool((os.getenv("UAZAPI_TOKEN") or "").strip()))
    print("[worker] ALERT_TO=", repr(os.getenv("ALERT_TO")))

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
    offers_end_hour = int(os.getenv("OFFERS_END_HOUR", "22"))
    offer_limit = int(os.getenv("PRODUCTS_OFFER_LIMIT", "20"))

    print(
        f"[worker] started. interval={interval}s to={to_number} due_soon_days={days} "
        f"offers_window={offers_start_hour}-{offers_end_hour} products_limit={offer_limit} "
        f"group_ids={group_ids}"
    )

    while True:
        started = time.time()

        with SessionLocal() as db:
            a = b = c = d = e = wr = mr = sa = 0

            # ✅ alertas de venda confirmada -> ALERT_TO
            try:
                sa = process_confirmed_sales_alerts(db)
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_confirmed_sales_alerts: {ex}")

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
                e = process_installments_due_today_to_client(db, to_number)
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_installments_due_today_to_client: {ex}")

            try:
                b = process_installments_overdue(db, to_number)
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_installments_overdue: {ex}")

            # ✅ semanal -> texto
            try:
                wr = process_weekly_report_text(db, to_number)
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_weekly_report_text: {ex}")

            # ✅ mensal -> pdf (ultimo dia do mes)
            try:
                mr = process_monthly_report_pdf(db, to_number)
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_monthly_report_pdf: {ex}")

            # ✅ ofertas (1/h, sem repetir produto no dia)
            try:
                now_local_dt = datetime.now()
                if offers_can_send_now(now_local_dt, start_hour=offers_start_hour, end_hour=offers_end_hour):
                    sent_product_id = process_hourly_product_offer(db, group_ids)
                    if sent_product_id > 0:
                        mark_offers_sent_this_hour(now_local_dt, product_id=sent_product_id)
                        d = 1
                    else:
                        d = 0
            except Exception as ex:
                db.rollback()
                print(f"[worker] ERROR process_hourly_product_offer: {ex}")

            if a or b or c or d or e or wr or mr or sa:
                print(
                    f"[worker] sent sales_alerts={sa} finance={a} due_soon_installments={c} "
                    f"due_today_client={e} overdue_installments={b} offers={d} "
                    f"weekly_text={wr} monthly_pdf={mr}"
                )

        elapsed = time.time() - started
        sleep_for = max(1, interval - int(elapsed))
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        run_loop()
    except KeyboardInterrupt:
        print("[worker] stopped (Ctrl+C)")
