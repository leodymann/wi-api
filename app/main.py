from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
import os

from app.infra.db import engine
from app.infra.models import Base

from app.api.routers.clients import router as clients_router
from app.api.routers.products import router as products_router
from app.api.routers.sales import router as sales_router
from app.api.routers.promissories import router as promissories_router
from app.api.routers.installments import router as installments_router
from app.api.routers.users import router as users_router
from app.api.routers.finance import router as finance_router
from app.api.routers.auth import router as auth_router
from app.api.routers.test import router as test

from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Moto Store API")

# ✅ CORS "sem dor de cabeça" para desktop/webview (Tauri) e web
# Como você usa Authorization Bearer (não cookie), NÃO precisa credentials.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# ✅ logger externo (roda mesmo quando a request é OPTIONS)
class LogPreflightMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method == "OPTIONS" or request.url.path.startswith("/auth/login"):
            print(
                "[REQ]",
                request.method,
                request.url.path,
                "origin=", request.headers.get("origin"),
                "acr-method=", request.headers.get("access-control-request-method"),
                "acr-headers=", request.headers.get("access-control-request-headers"),
                flush=True,
            )
        return await call_next(request)

# IMPORTANT: adicionar depois do CORS, para tentar logar tudo que entrar
app.add_middleware(LogPreflightMiddleware)

@app.on_event("startup")
def _startup() -> None:
    print("[startup] creating tables...", flush=True)
    Base.metadata.create_all(bind=engine)
    print("[startup] tables created/checked", flush=True)

app.include_router(clients_router, prefix="/clients", tags=["clients"])
app.include_router(products_router, prefix="/products", tags=["products"])
app.include_router(sales_router, prefix="/sales", tags=["sales"])
app.include_router(promissories_router, prefix="/promissories", tags=["promissories"])
app.include_router(installments_router, prefix="/installments", tags=["installments"])
app.include_router(users_router, prefix="/users", tags=["users"])
app.include_router(finance_router, prefix="/finance", tags=["finance"])
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(test, prefix="/test", tags=["test"])
