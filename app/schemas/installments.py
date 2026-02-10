from __future__ import annotations
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from decimal import Decimal
from datetime import date, datetime

class InstallmentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    promissory_id: int
    number: int
    due_date: date
    amount: Decimal
    status: str
    paid_at: Optional[datetime]
    paid_amount: Optional[Decimal]

class InstallmentPay(BaseModel):
    paid_amount: Optional[Decimal] = Field(default=None, ge=0)
