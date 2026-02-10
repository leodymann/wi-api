from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator

from app.infra.models import PromissoryStatus


class PromissoryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    public_id: str
    status: str

    total: Decimal
    entry_amount: Decimal

    # ✅ NOVO: regras de atraso
    daily_late_fee: Optional[Decimal] = None
    late_penalty: Optional[Decimal] = None
    late_penalty_percent: Optional[Decimal] = None

    issued_at: Optional[datetime]
    client_id: int
    product_id: Optional[int]
    sale_id: Optional[int]


class PromissoryStatusUpdate(BaseModel):
    status: PromissoryStatus

    @field_validator("status", mode="before")
    @classmethod
    def normalize(cls, v):
        if isinstance(v, str):
            return v.upper()
        return v
