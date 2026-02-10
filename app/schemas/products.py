from __future__ import annotations

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from decimal import Decimal


class ProductCreate(BaseModel):
    brand: str = Field(min_length=2, max_length=60)
    model: str = Field(min_length=1, max_length=80)
    year: int = Field(ge=1900, le=2100)

    plate: Optional[str] = Field(default=None, max_length=10)
    chassi: str = Field(min_length=5, max_length=30)
    km: Optional[int] = Field(default=None, ge=0)
    color: str = Field(min_length=1, max_length=30)

    cost_price: Decimal = Field(default=Decimal("0.00"), ge=0)
    sale_price: Decimal = Field(default=Decimal("0.00"), ge=0)

    status: Optional[str] = None

    # ✅ NOVO: auditoria de origem da compra (vendedor)
    purchase_seller_name: Optional[str] = Field(default=None, min_length=2, max_length=120)
    purchase_seller_phone: Optional[str] = Field(default=None, max_length=30)
    purchase_seller_cpf: Optional[str] = Field(default=None, max_length=14)
    purchase_seller_address: Optional[str] = Field(default=None, max_length=255)


class ProductUpdate(BaseModel):
    brand: Optional[str] = Field(default=None, min_length=2, max_length=60)
    model: Optional[str] = Field(default=None, min_length=1, max_length=80)
    year: Optional[int] = Field(default=None, ge=1900, le=2100)

    plate: Optional[str] = Field(default=None, max_length=10)
    chassi: Optional[str] = Field(default=None, min_length=5, max_length=30)
    km: Optional[int] = Field(default=None, ge=0)
    color: Optional[str] = Field(default=None, min_length=1, max_length=30)

    cost_price: Optional[Decimal] = Field(default=None, ge=0)
    sale_price: Optional[Decimal] = Field(default=None, ge=0)

    status: Optional[str] = None

    # ✅ NOVO: auditoria de origem da compra (vendedor)
    purchase_seller_name: Optional[str] = Field(default=None, min_length=2, max_length=120)
    purchase_seller_phone: Optional[str] = Field(default=None, max_length=30)
    purchase_seller_cpf: Optional[str] = Field(default=None, max_length=14)
    purchase_seller_address: Optional[str] = Field(default=None, max_length=255)


class ProductImageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    url: str
    position: int


class ProductOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    brand: str
    model: str
    year: int
    plate: Optional[str]
    chassi: str
    km: Optional[int]
    color: str
    cost_price: Decimal
    sale_price: Decimal
    status: str

    # ✅ NOVO: auditoria de origem da compra (vendedor)
    purchase_seller_name: Optional[str]
    purchase_seller_phone: Optional[str]
    purchase_seller_cpf: Optional[str]
    purchase_seller_address: Optional[str]

    images: List[ProductImageOut] = []
