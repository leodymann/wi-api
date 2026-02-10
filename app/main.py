from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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

# ✅ cobre:
# - tauri://localhost (muito comum no build)
# - http://localhost[:porta]
# - http://127.0.0.1[:porta]
# - http://[ipv6]:porta (seu caso)
ALLOW_ORIGIN_REGEX = r"^(tauri://localhost|http://(\[.*\]|localhost|127\.0\.0\.1)(:\d+)?)$"

app = FastAPI(title="Moto Store API")

# ✅ Log pra você enxergar o preflight no Railway (remova depois se quiser)
@app.middleware("http")
async def _log_preflight(request: Request, call_next):
    if request.method == "OPTIONS" or request.url.path.startswith("/auth/login"):
        print(
            "[REQ]",
            request.method,
            request.url.path,
            "origin=", request.headers.get("origin"),
            "acr-method=", request.headers.get("access-control-request-method"),
            "acr-headers=", request.headers.get("access-control-request-headers"),
        )
    return await call_next(request)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS_LIST,       # lista tradicional (web)
    allow_origin_regex=ALLOW_ORIGIN_REGEX,  # ✅ tauri + localhost + ipv6
    allow_credentials=False,                # ✅ você usa Bearer token, não cookie
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

print("[CORS] allow_origins =", ALLOW_ORIGINS_LIST)
print("[CORS] allow_origin_regex =", ALLOW_ORIGIN_REGEX)

@app.on_event("startup")
def _startup() -> None:
    print("[startup] creating tables...")
    Base.metadata.create_all(bind=engine)
    print("[startup] tables created/checked")

app.include_router(clients_router, prefix="/clients", tags=["clients"])
app.include_router(products_router, prefix="/products", tags=["products"])
app.include_router(sales_router, prefix="/sales", tags=["sales"])
app.include_router(promissories_router, prefix="/promissories", tags=["promissories"])
app.include_router(installments_router, prefix="/installments", tags=["installments"])
app.include_router(users_router, prefix="/users", tags=["users"])
app.include_router(finance_router, prefix="/finance", tags=["finance"])
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(test, prefix="/test", tags=["test"])
