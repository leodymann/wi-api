from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select
from typing import Optional

from app.api.deps import DBSession
from app.infra.models import InstallmentORM
from app.schemas.installments import InstallmentOut, InstallmentPay
from app.services.sales_service import pay_installment

from fastapi import Depends
from app.api.auth_deps import get_current_user

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.get("", response_model=list[InstallmentOut])
def list_installments(
    db: Session = DBSession,
    promissory_id: Optional[int] = Query(default=None),
):
    stmt = select(InstallmentORM).order_by(InstallmentORM.promissory_id.desc(), InstallmentORM.number.asc())
    if promissory_id is not None:
        stmt = stmt.where(InstallmentORM.promissory_id == promissory_id)
    return db.execute(stmt).scalars().all()

@router.post("/{inst_id}/pay", response_model=InstallmentOut)
def pay(inst_id: int, payload: InstallmentPay, db: Session = DBSession):
    try:
        inst = pay_installment(db, inst_id, paid_amount=payload.paid_amount)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return inst
