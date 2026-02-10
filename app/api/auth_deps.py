from __future__ import annotations

from typing import Callable, Iterable
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from sqlalchemy.orm import Session

from app.api.deps import DBSession  # se já existir, senão ajuste o import
from app.config import JWT_SECRET_KEY, JWT_ALGORITHM
from app.infra.models import UserORM, UserRole

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = DBSession,
) -> UserORM:
    cred_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token inválido ou expirado.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        sub = payload.get("sub")
        if not sub:
            raise cred_exc
        user_id = int(sub)
    except (JWTError, ValueError):
        raise cred_exc

    user = db.get(UserORM, user_id)
    if not user:
        raise cred_exc
    return user

def require_roles(*allowed: UserRole) -> Callable:
    allowed_set = set(allowed)

    def dep(user: UserORM = Depends(get_current_user)) -> UserORM:
        if user.role not in allowed_set:
            raise HTTPException(status_code=403, detail="Sem permissão.")
        return user

    return dep
