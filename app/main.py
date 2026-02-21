from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request
from pathlib import Path
import os

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
from app.api.routers.health import router as health_router

from dotenv import load_dotenv

load_dotenv()

_env_origins = os.environ.get("FRONTEND_URLS") or os.environ.get("ALLOWED_ORIGINS")
if _env_origins:
    ALLOW_ORIGINS_LIST = [o.strip() for o in _env_origins.split(",") if o.strip()]
else:
    ALLOW_ORIGINS_LIST = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://tauri.localhost",
    ]

app = FastAPI(title="Moto Store API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS_LIST + ["https://tauri.localhost"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_preflight(request: Request, call_next):
    if request.method == "OPTIONS":
        print("[PRE-FLIGHT] path =", request.url.path)
        print("[PRE-FLIGHT] origin =", request.headers.get("origin"))
        print("[PRE-FLIGHT] acrm =", request.headers.get("access-control-request-method"))
        print("[PRE-FLIGHT] acrh =", request.headers.get("access-control-request-headers"))
    return await call_next(request)

print("[CORS] allow_origins =", ALLOW_ORIGINS_LIST)


def ensure_admin() -> None:
    """
    Garante que exista um usuário ADMIN no banco.
    Controlado por env vars (recomendado no Railway).
    """
    admin_email = os.getenv("ADMIN_EMAIL", "admin@admin.com").strip().lower()
    admin_password = os.getenv("ADMIN_PASSWORD", "leo1idealdeveloper2admin3acess").strip()
    admin_name = os.getenv("ADMIN_NAME", "Admin").strip()

    db = SessionLocal()
    try:
        existing = db.query(UserORM).filter(UserORM.email == admin_email).first()
        if existing:
            # opcional: garantir role ADMIN caso exista com role errada
            if getattr(existing, "role", None) != "ADMIN":
                existing.role = "ADMIN"
                db.commit()
            print(f"[startup] admin exists: {admin_email}")
            return

        user = UserORM(
            name=admin_name,
            email=admin_email,
            password_hash=hash_password(admin_password),
            role="ADMIN",
        )
        db.add(user)
        db.commit()
        print(f"[startup] admin created: {admin_email}")
    finally:
        db.close()


@app.on_event("startup")
def _startup() -> None:
    print("[startup] creating tables...")
    Base.metadata.create_all(bind=engine)
    print("[startup] tables created/checked")

    ensure_admin()


app.include_router(clients_router, prefix="/clients", tags=["clients"])
app.include_router(products_router, prefix="/products", tags=["products"])
app.include_router(sales_router, prefix="/sales", tags=["sales"])
app.include_router(promissories_router, prefix="/promissories", tags=["promissories"])
app.include_router(installments_router, prefix="/installments", tags=["installments"])
app.include_router(users_router, prefix="/users", tags=["users"])
app.include_router(finance_router, prefix="/finance", tags=["finance"])
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(health_router, tags=["health"])


