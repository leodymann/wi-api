from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.deps import DBSession
from app.api.auth_deps import get_current_user
from app.infra.models import PaymentType
from app.schemas.promissories import PromissoryOut
from app.schemas.sales import SaleCreate, SaleOut, SaleStatusUpdate
from app.services.sales_service import create_sale, list_sales, update_sale_status

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.patch("/{sale_id}/status", response_model=SaleOut)
def update_sale_status_endpoint(
    sale_id: int,
    payload: SaleStatusUpdate,
    db: Session = DBSession,
):
    try:
        sale = update_sale_status(db, sale_id=sale_id, new_status=payload.status)
        return SaleOut.model_validate(sale)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("", response_model=dict, status_code=201)
def create_sale_endpoint(
    payload: SaleCreate,
    db: Session = DBSession,
    current_user=Depends(get_current_user),
):
    """
    ✅ user_id vem da sessão (current_user), não do front.
    """
    try:
        sale, prom = create_sale(
            db,
            client_id=payload.client_id,
            user_id=int(current_user.id),
            product_id=payload.product_id,
            total=payload.total,
            discount=payload.discount,
            entry_amount=payload.entry_amount,
            payment_type=PaymentType(payload.payment_type),
            installments_count=payload.installments_count,
            first_due_date=payload.first_due_date,

            # ✅ novos
            promissory_total=payload.promissory_total,
            daily_late_fee=payload.daily_late_fee,
        )
        db.commit()
        db.refresh(sale)
        if prom:
            db.refresh(prom)

    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="Erro ao criar venda.")

    resp = {"sale": SaleOut.model_validate(sale), "promissory": None}
    if prom:
        resp["promissory"] = PromissoryOut.model_validate(prom)
    return resp


@router.get("", response_model=dict)
def list_sales_endpoint(
    db: Session = DBSession,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    client_id: Optional[int] = Query(None),
    user_id: Optional[int] = Query(None),
    product_id: Optional[int] = Query(None),
    payment_type: Optional[str] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
):
    try:
        pt = PaymentType(payment_type) if payment_type is not None else None

        items, total = list_sales(
            db,
            page=page,
            page_size=page_size,
            client_id=client_id,
            user_id=user_id,
            product_id=product_id,
            payment_type=pt,
            date_from=date_from,
            date_to=date_to,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "items": [SaleOut.model_validate(s) for s in items],
        "total": total,
        "page": page,
        "page_size": page_size,
    }
