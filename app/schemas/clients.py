from __future__ import annotations

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional


class ClientCreate(BaseModel):
    name: str = Field(min_length=2, max_length=140)
    phone: str = Field(min_length=8, max_length=20)
    cpf: Optional[str] = Field(default=None, min_length=11, max_length=14)
    address: Optional[str] = Field(default=None, max_length=255)
    notes: Optional[str] = None


class ClientUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=2, max_length=140)
    phone: Optional[str] = Field(default=None, min_length=8, max_length=20)
    cpf: Optional[str] = Field(default=None, min_length=11, max_length=14)
    address: Optional[str] = Field(default=None, max_length=255)
    notes: Optional[str] = None


class ClientOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    phone: str
    cpf: Optional[str]
    address: Optional[str]
    notes: Optional[str]
