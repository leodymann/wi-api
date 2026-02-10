from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select
from typing import Optional

from app.api.deps import DBSession
from app.infra.models import ClientORM
from app.schemas.clients import ClientCreate, ClientUpdate, ClientOut

from fastapi import Depends
from app.api.auth_deps import get_current_user

router = APIRouter(dependencies=[Depends(get_current_user)])



def normalize_phone(phone: str) -> str:
    return (
        phone.strip()
        .replace(" ", "")
        .replace("-", "")
        .replace("(", "")
        .replace(")", "")
    )


def normalize_cpf(cpf: str) -> str:
    return cpf.strip().replace(".", "").replace("-", "")


@router.post("", response_model=ClientOut, status_code=201)
def create_client(payload: ClientCreate, db: Session = DBSession):
    phone = normalize_phone(payload.phone)
    cpf = normalize_cpf(payload.cpf) if payload.cpf else None

    client = ClientORM(
        name=payload.name.strip(),
        phone=phone,
        cpf=cpf,
        address=payload.address.strip() if payload.address else None,
        notes=payload.notes,
    )
    db.add(client)
    db.flush()  # garante client.id sem depender do commit automático

    return client


@router.get("", response_model=list[ClientOut])
def list_clients(
    db: Session = DBSession,
    q: Optional[str] = Query(default=None, description="Busca por nome/telefone/cpf"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    stmt = select(ClientORM).order_by(ClientORM.id.desc())

    if q:
        qn = q.strip()
        q_phone = normalize_phone(qn)
        q_cpf = normalize_cpf(qn)

        stmt = stmt.where(
            (ClientORM.name.ilike(f"%{qn}%")) |
            (ClientORM.phone.ilike(f"%{q_phone}%")) |
            (ClientORM.cpf.ilike(f"%{q_cpf}%"))
        )

    stmt = stmt.limit(limit).offset(offset)
    clients = db.execute(stmt).scalars().all()
    return clients


@router.get("/{client_id}", response_model=ClientOut)
def get_client(client_id: int, db: Session = DBSession):
    client = db.get(ClientORM, client_id)
    if not client:
        raise HTTPException(status_code=404, detail="Cliente não encontrado.")
    return client


@router.put("/{client_id}", response_model=ClientOut)
def update_client(client_id: int, payload: ClientUpdate, db: Session = DBSession):
    client = db.get(ClientORM, client_id)
    if not client:
        raise HTTPException(status_code=404, detail="Cliente não encontrado.")

    if payload.name is not None:
        client.name = payload.name.strip()

    if payload.phone is not None:
        client.phone = normalize_phone(payload.phone)

    if payload.cpf is not None:
        client.cpf = normalize_cpf(payload.cpf) if payload.cpf else None

    if payload.address is not None:
        client.address = payload.address.strip() if payload.address else None

    if payload.notes is not None:
        client.notes = payload.notes

    db.flush()
    return client
