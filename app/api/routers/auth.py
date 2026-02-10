from __future__ import annotations

from fastapi import APIRouter, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.api.deps import DBSession
from app.infra.models import UserORM
from app.schemas.auth import LoginIn, TokenOut
from app.services.security import verify_password
from app.services.jwt_service import create_access_token

router = APIRouter()

@router.post("/login", response_model=TokenOut)
def login(payload: LoginIn, db: Session = DBSession):
    email = payload.email.strip().lower()

    user = db.execute(select(UserORM).where(UserORM.email == email)).scalars().first()
    if not user:
        raise HTTPException(status_code=401, detail="Credenciais inválidas.")

    if not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Credenciais inválidas.")

    token = create_access_token(sub=str(user.id), role=user.role.value)
    return TokenOut(access_token=token)
