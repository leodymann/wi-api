from __future__ import annotations

from datetime import datetime, date
from decimal import Decimal
from typing import Optional, Tuple

from dateutil.relativedelta import relativedelta
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.infra.models import (
    SaleORM,
    ProductORM,
    ClientORM,
    UserORM,
    PromissoryORM,
    InstallmentORM,
    ProductStatus,
    SaleStatus,
    PaymentType,
    PromissoryStatus,
    InstallmentStatus,
)
from app.services.id_gen import generate_public_id


# helpers
def _today_utc() -> date:
    return datetime.utcnow().date()


def _add_months(d: date, months: int) -> date:
    return d + relativedelta(months=months)


def _unique_public_id(db: Session, model, prefix: str) -> str:
    for _ in range(30):
        pid = generate_public_id(prefix)
        exists = db.scalar(select(model.id).where(model.public_id == pid))
        if not exists:
            return pid
    raise RuntimeError(f"Falha ao gerar public_id único para prefix={prefix}.")


def _quantize_money(v: Decimal) -> Decimal:
    return v.quantize(Decimal("0.01"))


ALLOWED_TRANSITIONS: dict[SaleStatus, set[SaleStatus]] = {
    SaleStatus.DRAFT: {SaleStatus.CONFIRMED, SaleStatus.CANCELED},
    SaleStatus.CONFIRMED: set(),
    SaleStatus.CANCELED: set(),
}


def update_sale_status(db: Session, *, sale_id: int, new_status: SaleStatus) -> SaleORM:
    sale = db.query(SaleORM).filter(SaleORM.id == sale_id).first()
    if not sale:
        raise ValueError("Venda não encontrada")

    current = sale.status
    if current == new_status:
        return sale

    allowed = ALLOWED_TRANSITIONS.get(current, set())
    if new_status not in allowed:
        raise ValueError(f"Transição inválida: {current} -> {new_status}")

    sale.status = new_status
    db.add(sale)
    db.commit()
    db.refresh(sale)
    return sale


def create_sale(
    db: Session,
    *,
    client_id: int,
    user_id: int,
    product_id: int,
    total: Decimal,
    discount: Decimal = Decimal("0.00"),
    entry_amount: Optional[Decimal] = None,
    payment_type: PaymentType,
    installments_count: Optional[int] = None,
    first_due_date: Optional[date] = None,

    # ✅ NOVOS: ajuste da promissória + regras atraso
    promissory_total: Optional[Decimal] = None,
    daily_late_fee: Optional[Decimal] = None,
) -> Tuple[SaleORM, Optional[PromissoryORM]]:

    if not db.get(ClientORM, client_id):
        raise ValueError("client_id inválido.")
    if not db.get(UserORM, user_id):
        raise ValueError("user_id inválido.")

    product = db.get(ProductORM, product_id)
    if not product:
        raise ValueError("product_id inválido.")
    if product.status not in (ProductStatus.IN_STOCK, ProductStatus.RESERVED):
        raise ValueError("Produto não está disponível (precisa estar IN_STOCK ou RESERVED).")

    total = _quantize_money(Decimal(total))
    discount = _quantize_money(Decimal(discount))
    entry_amount = _quantize_money(Decimal(entry_amount)) if entry_amount is not None else None

    if total <= 0:
        raise ValueError("total deve ser maior que zero.")
    if discount < 0:
        raise ValueError("discount não pode ser negativo.")
    if entry_amount is not None and entry_amount < 0:
        raise ValueError("entry_amount não pode ser negativo.")

    entry = entry_amount or Decimal("0.00")
    if entry > total:
        raise ValueError("Entrada maior que o total.")

    # normaliza novos campos
    if promissory_total is not None:
        promissory_total = _quantize_money(Decimal(promissory_total))
        if promissory_total < 0:
            raise ValueError("promissory_total não pode ser negativo.")

    if daily_late_fee is not None:
        daily_late_fee = _quantize_money(Decimal(daily_late_fee))
        if daily_late_fee < 0:
            raise ValueError("daily_late_fee não pode ser negativo.")

    sale_public_id = _unique_public_id(db, SaleORM, "VEN")

    # snapshot produto -> venda
    sale = SaleORM(
        public_id=sale_public_id,
        client_id=client_id,
        user_id=user_id,
        product_id=product_id,
        total=total,
        discount=discount,
        entry_amount=entry_amount,
        payment_type=payment_type,
        status=SaleStatus.DRAFT,

        product_brand=(product.brand or None),
        product_model=(product.model or None),
        product_year=(product.year or None),
        product_plate=(product.plate or None),
        product_chassi=(product.chassi or None),
        product_color=(product.color or None),
        product_km=(product.km if product.km is not None else None),
        product_cost_price=(product.cost_price if product.cost_price is not None else None),
        product_sale_price=(product.sale_price if product.sale_price is not None else None),

        product_purchase_seller_name=(product.purchase_seller_name or None),
        product_purchase_seller_phone=(product.purchase_seller_phone or None),
        product_purchase_seller_cpf=(product.purchase_seller_cpf or None),
        product_purchase_seller_address=(product.purchase_seller_address or None),
    )
    db.add(sale)

    # marca produto como vendido
    product.status = ProductStatus.SOLD

    promissory: Optional[PromissoryORM] = None

    if payment_type == PaymentType.PROMISSORY:
        if not installments_count or installments_count < 1:
            raise ValueError("Para PROMISSORY informe installments_count (>= 1).")

        remaining_base = _quantize_money(total - entry)  # restante real

        # ✅ principal da promissória: se vier ajustado, usa; senão usa restante real
        prom_principal = promissory_total if promissory_total is not None else remaining_base
        prom_principal = _quantize_money(Decimal(prom_principal))

        if prom_principal < 0:
            raise ValueError("Valor da promissória inválido (negativo).")

        prom_public_id = _unique_public_id(db, PromissoryORM, "PROM")

        promissory = PromissoryORM(
            public_id=prom_public_id,
            sale=sale,
            client_id=client_id,
            product_id=product_id,

            total=prom_principal,              # ✅ valor ajustado vira a promissória
            entry_amount=_quantize_money(entry),

            daily_late_fee=daily_late_fee,

            status=PromissoryStatus.DRAFT,
        )
        db.add(promissory)

        sale_day = _today_utc()
        first_due = first_due_date or _add_months(sale_day, 1)

        # parcelas em cima do prom_principal (ajustado)
        per = _quantize_money(prom_principal / Decimal(installments_count))
        total_calc = per * Decimal(installments_count)
        diff = _quantize_money(prom_principal - total_calc)

        for n in range(1, installments_count + 1):
            due = _add_months(first_due, n - 1)
            amount = per
            if n == installments_count and diff != 0:
                amount = _quantize_money(amount + diff)

            inst = InstallmentORM(
                promissory=promissory,
                number=n,
                due_date=due,
                amount=amount,
                status=InstallmentStatus.PENDING,
            )
            db.add(inst)

    db.flush()
    return sale, promissory


def list_sales(
    db: Session,
    *,
    page: int = 1,
    page_size: int = 20,
    client_id: Optional[int] = None,
    user_id: Optional[int] = None,
    product_id: Optional[int] = None,
    payment_type: Optional[PaymentType] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
):
    if page < 1:
        raise ValueError("page deve ser >= 1")
    if page_size < 1 or page_size > 200:
        raise ValueError("page_size deve estar entre 1 e 200")

    q = db.query(SaleORM)

    if client_id is not None:
        q = q.filter(SaleORM.client_id == client_id)
    if user_id is not None:
        q = q.filter(SaleORM.user_id == user_id)
    if product_id is not None:
        q = q.filter(SaleORM.product_id == product_id)
    if payment_type is not None:
        q = q.filter(SaleORM.payment_type == payment_type)

    if date_from is not None:
        q = q.filter(SaleORM.created_at >= date_from)
    if date_to is not None:
        q = q.filter(SaleORM.created_at <= date_to)

    total = q.with_entities(func.count(SaleORM.id)).scalar() or 0

    items = (
        q.order_by(SaleORM.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return items, total


def issue_promissory(db: Session, prom_id: int) -> PromissoryORM:
    prom = db.get(PromissoryORM, prom_id)
    if not prom:
        raise ValueError("Promissória não encontrada.")

    if prom.status == PromissoryStatus.CANCELED:
        raise ValueError("Promissória cancelada não pode ser emitida.")

    if prom.status in (PromissoryStatus.ISSUED, PromissoryStatus.PAID):
        return prom

    prom.status = PromissoryStatus.ISSUED
    prom.issued_at = datetime.utcnow()
    db.flush()
    return prom


PROM_ALLOWED_TRANSITIONS: dict[PromissoryStatus, set[PromissoryStatus]] = {
    PromissoryStatus.DRAFT: {PromissoryStatus.CANCELED, PromissoryStatus.ISSUED},
    PromissoryStatus.ISSUED: {PromissoryStatus.CANCELED, PromissoryStatus.PAID},
    PromissoryStatus.PAID: set(),
    PromissoryStatus.CANCELED: set(),
}


def cancel_promissory(db: Session, prom_id: int) -> PromissoryORM:
    prom = db.get(PromissoryORM, prom_id)
    if not prom:
        raise ValueError("Promissória não encontrada.")

    if prom.status == PromissoryStatus.CANCELED:
        return prom

    if prom.status == PromissoryStatus.PAID:
        raise ValueError("Não é possível cancelar uma promissória já paga.")

    has_paid = any(i.status == InstallmentStatus.PAID for i in prom.installments)
    if has_paid:
        raise ValueError("Não é possível cancelar: existe(m) parcela(s) já paga(s).")

    allowed = PROM_ALLOWED_TRANSITIONS.get(prom.status, set())
    if PromissoryStatus.CANCELED not in allowed:
        raise ValueError(f"Transição inválida: {prom.status} -> {PromissoryStatus.CANCELED}")

    prom.status = PromissoryStatus.CANCELED

    for inst in prom.installments:
        if inst.status == InstallmentStatus.PENDING:
            inst.status = InstallmentStatus.CANCELED

    db.flush()
    return prom


def pay_installment(
    db: Session,
    inst_id: int,
    *,
    paid_amount: Optional[Decimal] = None,
) -> InstallmentORM:
    inst = db.get(InstallmentORM, inst_id)
    if not inst:
        raise ValueError("Parcela não encontrada.")

    prom = inst.promissory
    if prom.status == PromissoryStatus.CANCELED:
        raise ValueError("Promissória cancelada: não é possível pagar parcelas.")

    if inst.status == InstallmentStatus.CANCELED:
        raise ValueError("Parcela cancelada não pode ser paga.")
    if inst.status == InstallmentStatus.PAID:
        return inst

    amount_to_pay = Decimal(paid_amount) if paid_amount is not None else inst.amount
    amount_to_pay = _quantize_money(amount_to_pay)
    if amount_to_pay < 0:
        raise ValueError("paid_amount não pode ser negativo.")

    inst.status = InstallmentStatus.PAID
    inst.paid_at = datetime.utcnow()
    inst.paid_amount = amount_to_pay
    db.flush()

    # ✅ se todas as parcelas da promissória estiverem pagas:
    if all(i.status == InstallmentStatus.PAID for i in prom.installments):
        prom.status = PromissoryStatus.PAID
        db.flush()

        # ✅ marca a venda como CONFIRMED (quitada, segundo sua regra)
        sale = prom.sale  # relationship 0..1 já existe
        if sale and sale.status != SaleStatus.CANCELED:
            sale.status = SaleStatus.CONFIRMED
            db.flush()

    return inst



