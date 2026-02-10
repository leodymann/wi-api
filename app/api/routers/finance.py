from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Depends, Body
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import DBSession
from app.infra.models import FinanceORM, FinanceStatus, WppSendStatus, UserRole
from app.schemas.finance import FinanceCreate, FinanceUpdate, FinancePay, FinanceOut
from app.api.auth_deps import require_roles, get_current_user

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.post("", response_model=FinanceOut, status_code=201)
def create_finance(
    payload: FinanceCreate,
    db: Session = DBSession,
    _user=Depends(require_roles(UserRole.ADMIN)),
):
    try:
        status = FinanceStatus(payload.status)
    except Exception:
        raise HTTPException(status_code=400, detail="status inválido (PENDING|PAID|CANCELED).")

    row = FinanceORM(
        company=payload.company.strip(),
        amount=payload.amount,
        due_date=payload.due_date,
        status=status,
        description=payload.description,
        notes=payload.notes,
        wpp_status=WppSendStatus.PENDING,
        wpp_tries=0,
        wpp_last_error=None,
        wpp_sent_at=None,
        wpp_next_retry_at=None,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.get("", response_model=list[FinanceOut])
def list_finance(
    db: Session = DBSession,
    _user=Depends(require_roles(UserRole.ADMIN)),
    status: Optional[str] = Query(default=None, description="PENDING|PAID|CANCELED"),
    company: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    stmt = select(FinanceORM).order_by(FinanceORM.due_date.asc(), FinanceORM.id.asc())

    if status:
        try:
            st = FinanceStatus(status)
        except Exception:
            raise HTTPException(status_code=400, detail="status inválido (PENDING|PAID|CANCELED).")
        stmt = stmt.where(FinanceORM.status == st)

    if company:
        stmt = stmt.where(FinanceORM.company.ilike(f"%{company}%"))

    stmt = stmt.limit(limit).offset(offset)
    return db.execute(stmt).scalars().all()


@router.get("/{finance_id}", response_model=FinanceOut)
def get_finance(
    finance_id: int,
    db: Session = DBSession,
    _user=Depends(require_roles(UserRole.ADMIN)),
):
    row = db.get(FinanceORM, finance_id)
    if not row:
        raise HTTPException(status_code=404, detail="Finance não encontrado.")
    return row


@router.put("/{finance_id}", response_model=FinanceOut)
def update_finance(
    finance_id: int,
    payload: FinanceUpdate,
    db: Session = DBSession,
    _user=Depends(require_roles(UserRole.ADMIN)),
):
    row = db.get(FinanceORM, finance_id)
    if not row:
        raise HTTPException(status_code=404, detail="Finance não encontrado.")

    if payload.company is not None:
        row.company = payload.company.strip()
    if payload.amount is not None:
        row.amount = payload.amount
    if payload.due_date is not None:
        row.due_date = payload.due_date
    if payload.description is not None:
        row.description = payload.description
    if payload.notes is not None:
        row.notes = payload.notes

    if payload.status is not None:
        try:
            row.status = FinanceStatus(payload.status)
        except Exception:
            raise HTTPException(status_code=400, detail="status inválido (PENDING|PAID|CANCELED).")

    db.commit()
    db.refresh(row)
    return row



@router.post("/{finance_id}/pay", response_model=FinanceOut)
def pay_finance(
    finance_id: int,
    db: Session = DBSession,
    _user=Depends(require_roles(UserRole.ADMIN)),
):
    row = db.get(FinanceORM, finance_id)
    if not row:
        raise HTTPException(status_code=404, detail="Finance não encontrado.")

    # idempotente
    if row.status == FinanceStatus.PAID:
        return row

    if row.status == FinanceStatus.CANCELED:
        raise HTTPException(status_code=400, detail="Não é possível pagar uma conta cancelada.")

    row.status = FinanceStatus.PAID

    # ✅ NÃO altera wpp_* aqui (quem altera é o worker quando enviar msg)
    db.commit()
    db.refresh(row)
    return row