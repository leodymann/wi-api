from __future__ import annotations

from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import select
from typing import Optional

from app.api.deps import DBSession
from app.infra.models import UserORM, UserRole
from app.schemas.users import UserCreate, UserOut
from app.services.security import hash_password

from app.api.auth_deps import get_current_user

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.post("", response_model=UserOut, status_code=201)
def create_user(payload: UserCreate, db: Session = DBSession):
    role = UserRole(payload.role) if payload.role else UserRole.STAFF

    email = payload.email.strip().lower()
    exists = db.scalar(select(UserORM.id).where(UserORM.email == email))
    if exists:
        raise HTTPException(status_code=409, detail="Email já cadastrado.")

    user = UserORM(
        name=payload.name.strip(),
        email=email,
        password_hash=hash_password(payload.password),
        role=role,
    )
    db.add(user)
    db.flush()
    return user


@router.get("", response_model=list[UserOut])
def list_users(
    db: Session = DBSession,
    q: Optional[str] = Query(default=None, description="Busca por nome ou email"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    stmt = select(UserORM).order_by(UserORM.id.desc())

    if q:
        qn = q.strip()
        stmt = stmt.where(
            (UserORM.name.ilike(f"%{qn}%")) |
            (UserORM.email.ilike(f"%{qn}%"))
        )

    stmt = stmt.limit(limit).offset(offset)
    return db.execute(stmt).scalars().all()