from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict


class FinanceCreate(BaseModel):
    company: str = Field(min_length=2, max_length=120)
    amount: Decimal = Field(gt=0)
    due_date: date
    status: str = "PENDING"  # pending - paid - canceled
    description: Optional[str] = Field(default=None, max_length=200)
    notes: Optional[str] = None


class FinanceUpdate(BaseModel):
    company: Optional[str] = Field(default=None, min_length=2, max_length=120)
    amount: Optional[Decimal] = Field(default=None, gt=0)
    due_date: Optional[date] = None
    status: Optional[str] = None  # pending - paid - canceled
    description: Optional[str] = Field(default=None, max_length=200)
    notes: Optional[str] = None


class FinancePay(BaseModel):
    paid_at: Optional[datetime] = None  # se None, usa agora


class FinanceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    company: str
    amount: Decimal
    due_date: date
    status: str

    description: Optional[str]
    notes: Optional[str]

    wpp_status: str
    wpp_tries: int
    wpp_last_error: Optional[str]
    wpp_sent_at: Optional[datetime]
    wpp_next_retry_at: Optional[datetime]

    created_at: datetime
