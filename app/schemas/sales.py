from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from app.infra.models import SaleStatus


class SaleCreate(BaseModel):
    client_id: int
    product_id: int

    total: Decimal = Field(gt=0)
    discount: Decimal = Field(default=Decimal("0.00"), ge=0)

    entry_amount: Optional[Decimal] = Field(default=None, ge=0)

    # ✅ NOVO: tipo da entrada (CASH|PIX|CARD)
    entry_amount_type: Optional[str] = None

    payment_type: str  # CASH, PIX, CARD, PROMISSORY
    installments_count: Optional[int] = Field(default=None, ge=1, le=60)
    first_due_date: Optional[date] = None

    promissory_total: Optional[Decimal] = Field(default=None, ge=0)
    daily_late_fee: Optional[Decimal] = Field(default=None, ge=0)


class SaleOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    public_id: str
    status: str
    payment_type: str

    total: Decimal
    discount: Decimal
    entry_amount: Optional[Decimal]

    # ✅ NOVO
    entry_amount_type: Optional[str] = None

    client_id: int
    product_id: int
    user_id: int

    created_at: datetime

    # snapshot/auditoria do produto
    product_brand: Optional[str] = None
    product_model: Optional[str] = None
    product_year: Optional[int] = None

    product_plate: Optional[str] = None
    product_chassi: Optional[str] = None

    product_color: Optional[str] = None
    product_km: Optional[int] = None

    product_cost_price: Optional[Decimal] = None
    product_sale_price: Optional[Decimal] = None

    product_purchase_seller_name: Optional[str] = None
    product_purchase_seller_phone: Optional[str] = None
    product_purchase_seller_cpf: Optional[str] = None
    product_purchase_seller_address: Optional[str] = None


class SaleStatusUpdate(BaseModel):
    status: SaleStatus
