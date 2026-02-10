from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
import os

from dotenv import load_dotenv
load_dotenv()

from app.infra.db import engine, SessionLocal
from app.infra.models import Base, UserORM
from app.services.security import hash_password

from app.api.routers.clients import router as clients_router
from app.api.routers.products import router as products_router
from app.api.routers.sales import router as sales_router
from app.api.routers.promissories import router as promissories_router
from app.api.routers.installments import router as installments_router
from app.api.routers.users import router as users_router
from app.api.routers.finance import router as finance_router
from app.api.routers.auth import router as auth_router
from app.api.routers.test import router as test


# allowed origins can be provided as a comma-separated env var
_env_origins = os.environ.get("FRONTEND_URLS") or os.environ.get("ALLOWED_ORIGINS")
if _env_origins:
    ALLOW_ORIGINS_LIST = [o.strip() for o in _env_origins.split(",") if o.strip()]
else:
    ALLOW_ORIGINS_LIST = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]


app = FastAPI(title="Moto Store API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS_LIST,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

print("[CORS] allow_origins =", ALLOW_ORIGINS_LIST)


def ensure_admin(db: Session) -> None:
    """
    Cria um usuário admin caso não exista.
    Configure via variáveis de ambiente no Railway:
      ADMIN_EMAIL, ADMIN_PASSWORD, ADMIN_NAME
    """
    email = os.getenv("ADMIN_EMAIL", "admin@admin.com").strip().lower()
    password = os.getenv("ADMIN_PASSWORD", "admin123").strip()
    name = os.getenv("ADMIN_NAME", "Admin").strip()

    if not email or not password:
        print("[startup] admin vars inválidas; pulando criação do admin")
        return

    existing = db.query(UserORM).filter(UserORM.email == email).first()
    if existing:
        return

    user = UserORM(
        name=name,
        email=email,
        password_hash=hash_password(password),
        role="ADMIN",
    )
    db.add(user)
    try:
        db.commit()
        print("Admin criado")
    except IntegrityError:
        db.rollback()
        # Em caso de corrida (2 instâncias subindo), ignora
        print("Admin já existe")


@app.on_event("startup")
def _startup() -> None:
    print("[startup] creating tables...")
    Base.metadata.create_all(bind=engine)
    print("[startup] tables created/checked")

    db = SessionLocal()
    try:
        ensure_admin(db)
    finally:
        db.close()


@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(clients_router, prefix="/clients", tags=["clients"])
app.include_router(products_router, prefix="/products", tags=["products"])
app.include_router(sales_router, prefix="/sales", tags=["sales"])
app.include_router(promissories_router, prefix="/promissories", tags=["promissories"])
app.include_router(installments_router, prefix="/installments", tags=["installments"])
app.include_router(users_router, prefix="/users", tags=["users"])
app.include_router(finance_router, prefix="/finance", tags=["finance"])
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(test, prefix="/test", tags=["test"])
